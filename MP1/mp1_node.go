package main

import (
	"bufio"
	"container/heap"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var hostName string
var n int
var nodeGroup map[string]Node
var seqNum uint64
var totalBytes uint64

var mutexSeqNum sync.Mutex
var mutexQue sync.Mutex
var mutexReceive sync.Mutex
var mutexPropose sync.Mutex
var mutexAccount sync.Mutex
var mutexFailure sync.RWMutex
var wg sync.WaitGroup
var mutexBytes sync.Mutex
var mutexWrite sync.Mutex

var bwLog *os.File
var timeLog *os.File

type Node struct {
	Ip   string
	Port string
	Ch   chan Message
}

type Priority struct {
	Seq uint64
	Pid string
}

type Transaction struct {
	Op        string
	InitPrior Priority
}

type Message struct {
	T      Transaction
	Prior  Priority
	Status string
}

type PriorQue []Transaction

var que PriorQue
var queMap map[Transaction]int
var receiveMap map[Message]bool
var priorMap map[Transaction]Priority
var proposedCnt map[Transaction]int
var canDeliver map[Transaction]bool
var balance map[string]int
var failNode map[string]bool

func (que PriorQue) Len() int {
	return len(que)
}

func (que PriorQue) Less(i, j int) bool {
	priorI, priorJ := priorMap[que[i]], priorMap[que[j]]
	if priorI.Seq == priorJ.Seq {
		return strings.Compare(priorI.Pid, priorJ.Pid) < 0
	}
	return priorI.Seq < priorJ.Seq
}

func (que PriorQue) Swap(i, j int) {
	que[i], que[j] = que[j], que[i]
	queMap[que[i]], queMap[que[j]] = i, j
}

func (que *PriorQue) Push(x any) {
	*que = append(*que, x.(Transaction))
	queMap[x.(Transaction)] = len(*que) - 1
	priorMap[x.(Transaction)] = x.(Transaction).InitPrior
}

func (que *PriorQue) Pop() any {
	old := *que
	lenOfOld := len(old)
	item := old[lenOfOld-1]
	*que = old[:lenOfOld-1]
	delete(queMap, item)
	delete(priorMap, item)
	delete(canDeliver, item)
	mutexPropose.Lock()
	delete(proposedCnt, item)
	mutexPropose.Unlock()
	return item
}

func printBalance() {
	allAccounts := make([]string, 0)
	for account := range balance {
		allAccounts = append(allAccounts, account)
	}
	sort.Strings(allAccounts)
	fmt.Print("BALANCES")
	for _, account := range allAccounts {
		if balance[account] < 0 {
			log.Fatal("Negative account!")
		}
		if balance[account] > 0 {
			fmt.Print(" ", account, ":", balance[account])
		}
	}
	fmt.Println("")
}

func updateBalance() {
	mutexQue.Lock()
	for len(que) > 0 {
		// fmt.Println(que[0], canDeliver[que[0]])
		if canDeliver[que[0]] {
			//fmt.Println(que[0], priorMap[que[0]])
			t := heap.Pop(&que).(Transaction)
			mutexAccount.Lock()
			op := strings.Split(t.Op, " ")
			if op[0] == "DEPOSIT" {
				account := op[1]
				amount, err := strconv.Atoi(op[2])
				if err != nil {
					log.Fatal("Wrong transaction format!")
				}
				balance[account] += amount
			} else {
				account1, account2 := op[1], op[3]
				amount, err := strconv.Atoi(op[4])
				if err != nil {
					log.Fatal("Wrong transaction format!")
				}
				// if source account does not exist or have enough money, simply ignore it
				if _, ok := balance[account1]; ok && balance[account1] >= amount {
					balance[account1] -= amount
					balance[account2] += amount
				}
			}
			mutexWrite.Lock()
			fmt.Fprintln(timeLog, t, float64(time.Now().UnixNano())/1e6)
			printBalance()
			mutexWrite.Unlock()
			mutexAccount.Unlock()
			continue
		}
		// handle failure nodes here
		mutexFailure.RLock()
		if _, ok := failNode[que[0].InitPrior.Pid]; ok { // discard fail node's transaction
			mutexFailure.RUnlock()
			_ = heap.Pop(&que).(Transaction)
			continue
		}
		mutexPropose.Lock()
		if que[0].InitPrior.Pid == hostName && proposedCnt[que[0]] >= n {
			defer multicast(Message{que[0], priorMap[que[0]], "agree"})
			defer deliver(Message{que[0], priorMap[que[0]], "agree"})
		}
		mutexPropose.Unlock()
		mutexFailure.RUnlock()
		break
	}
	mutexQue.Unlock()
}

func readConfigFile(filename string) {
	fd, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	scanner.Split(bufio.ScanWords)

	scanner.Scan()
	n, err = strconv.Atoi(scanner.Text())
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < n; i++ { // parse the config file and retrieve node information
		scanner.Scan()
		name := scanner.Text()
		scanner.Scan()
		ip := scanner.Text()
		scanner.Scan()
		port := scanner.Text()
		nodeGroup[name] = Node{ip, port, make(chan Message, 100)}
	}
	n++
}

func handleFailure(node string) {
	mutexFailure.Lock()
	if _, ok := failNode[node]; !ok { // handle a new failed node
		n--
		failNode[node] = true
		// fmt.Println(n, node)
	}
	mutexFailure.Unlock()
}

func multicast(m Message) {
	for name, node := range nodeGroup {
		mutexFailure.RLock()
		if _, ok := failNode[name]; !ok {
			node.Ch <- m
		}
		mutexFailure.RUnlock()
	}
}

func deliver(m Message) {

	mutexReceive.Lock()
	if _, ok := receiveMap[m]; ok {
		mutexReceive.Unlock()
		return
	}
	// fmt.Println("deliver ", m.T.Op, m.T.InitPrior, m.Prior, m.Status)
	receiveMap[m] = true
	mutexReceive.Unlock()
	mutexQue.Lock()

	if m.Status == "send" {
		if m.T.InitPrior.Pid == hostName {
			mutexQue.Unlock()
			deliver(Message{m.T, m.T.InitPrior, "propose"})
			return
		}
		mutexSeqNum.Lock()
		seqNum++
		prior := Priority{seqNum, hostName}
		mutexSeqNum.Unlock()
		priorMap[m.T] = prior
		canDeliver[m.T] = false
		heap.Push(&que, m.T)
		mutexQue.Unlock()
		// channel is already safe with inner lock
		mutexFailure.RLock()
		if _, ok := failNode[m.T.InitPrior.Pid]; !ok {
			nodeGroup[m.T.InitPrior.Pid].Ch <- Message{m.T, prior, "propose"}
		}
		mutexFailure.RUnlock()
		multicast(m)
	} else if m.Status == "propose" {
		if m.T.InitPrior.Pid != hostName {
			log.Fatal("Proposed Priority sent to wrong node!")
		}
		if m.Prior.Seq > priorMap[m.T].Seq || m.Prior.Seq > priorMap[m.T].Seq &&
			strings.Compare(m.Prior.Pid, priorMap[m.T].Pid) > 0 {
			priorMap[m.T] = m.Prior
			heap.Fix(&que, queMap[m.T])
		}
		prior := priorMap[m.T]
		mutexQue.Unlock()

		mutexPropose.Lock()
		proposedCnt[m.T]++
		agreeMade := false
		mutexFailure.RLock()
		if proposedCnt[m.T] >= n { // in case failure happens that reduces n
			agreeMade = true
		}
		mutexFailure.RUnlock()
		mutexPropose.Unlock()
		if agreeMade {
			deliver(Message{m.T, prior, "agree"})
			multicast(Message{m.T, prior, "agree"})
		}
	} else {
		priorMap[m.T] = m.Prior
		heap.Fix(&que, queMap[m.T])
		mutexSeqNum.Lock()
		if priorMap[m.T].Seq > seqNum {
			seqNum = priorMap[m.T].Seq
		}
		mutexSeqNum.Unlock()
		canDeliver[m.T] = true
		mutexQue.Unlock()
		// fmt.Println(m)
		updateBalance()
		multicast(m)
	}
}

func sendMessage(node string, conn net.Conn, ch chan Message) {
	defer wg.Done()
	enc := gob.NewEncoder(conn)
	for {
		m := <-ch
		if err := enc.Encode(m); err != nil {
			// handle failure here
			handleFailure(node)
			return
		}
		mutexBytes.Lock()
		totalBytes += uint64(unsafe.Sizeof(m))
		mutexBytes.Unlock()
		// fmt.Println(m)
	}
}

func receiveMessage(conn net.Conn) {
	defer wg.Done()
	dec := gob.NewDecoder(conn)
	for {
		m := new(Message)
		if err := dec.Decode(m); err != nil {
			// let the sender side to handle failure
			return
		}
		mutexBytes.Lock()
		totalBytes += uint64(unsafe.Sizeof(*m))
		mutexBytes.Unlock()
		deliver(*m)
	}
}

func handleTransaction() {
	defer wg.Done()

	reader := bufio.NewReader(os.Stdin)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Println("Error reading input:", err)
			continue
		}
		line = line[:len(line)-1]
		// fmt.Println(line)
		mutexSeqNum.Lock()
		seqNum++
		t := Transaction{line, Priority{seqNum, hostName}}
		mutexSeqNum.Unlock()

		mutexQue.Lock()
		priorMap[t] = t.InitPrior
		heap.Push(&que, t)
		canDeliver[t] = false
		mutexQue.Unlock()

		m := Message{t, t.InitPrior, "send"}
		deliver(m)
		multicast(m)
	}
}

func bandwidthLogger() {
	interval := time.Second // record the bandwidth per each second
	last := uint64(time.Now().Unix())
	for {
		time.Sleep(interval)
		now := uint64(time.Now().Unix())
		mutexBytes.Lock()
		fmt.Fprintln(bwLog, totalBytes/(now-last))
		totalBytes = 0
		mutexBytes.Unlock()
		last = now
	}
}

func main() {

	if len(os.Args) != 4 {
		log.Fatal("Command args are mp1_node {node id} {port} {config file}!")
	}

	nodeGroup = make(map[string]Node)
	queMap = make(map[Transaction]int)
	receiveMap = make(map[Message]bool)
	priorMap = make(map[Transaction]Priority)
	proposedCnt = make(map[Transaction]int)
	canDeliver = make(map[Transaction]bool)
	balance = make(map[string]int)
	failNode = make(map[string]bool)

	hostName = os.Args[1]
	readConfigFile(os.Args[3])

	// listen to the port
	listen, err := net.Listen("tcp", ":"+os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	defer listen.Close()

	seqNum = 0
	totalBytes = 0

	pathName := os.Args[3][:strings.Index(os.Args[3], "/")]

	bwLog, err = os.Create(pathName + "/bw_" + hostName + ".txt")
	if err != nil {
		log.Fatal(err)
	}

	timeLog, err = os.Create(pathName + "/time_" + hostName + ".txt")
	if err != nil {
		log.Fatal(err)
	}

	defer bwLog.Close()
	defer timeLog.Close()

	heap.Init(&que)

	// connect to all the other nodes
	for name, node := range nodeGroup {
		conn, err := net.Dial("tcp", node.Ip+":"+node.Port)
		for err != nil {
			time.Sleep(time.Second) // sleep for 1 second and reconnect
			conn, err = net.Dial("tcp", node.Ip+":"+node.Port)
		}
		wg.Add(1)
		go sendMessage(name, conn, node.Ch)
	}

	for i := 0; i < n-1; i++ {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go receiveMessage(conn)
	}

	go bandwidthLogger()

	wg.Add(1)
	go handleTransaction()
	wg.Wait()
}
