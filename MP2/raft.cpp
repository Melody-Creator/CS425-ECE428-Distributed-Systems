#include <iostream>
#include <cstdlib>
#include <sstream>
#include <vector>
#include <chrono>
#include <thread>
#include <mutex>

#define log_size static_cast<int>(log.size())
// define a null value

constexpr int null = -1;

class RaftNode {

public:
    RaftNode(int _node_id, int _number_of_nodes) : node_id(_node_id), number_of_nodes(_number_of_nodes) {
        current_term = 0;
        commit_index = 0;
        last_applied = 0;
        vote_count = 0;
        voted_for = null;
        leader_id = null;
        state = "FOLLOWER";

        // add an empty log at index 0
        log.emplace_back(0, std::string());
        next_index.assign(number_of_nodes, 1);
        match_index.assign(number_of_nodes, 0);

        // set the timeout to be 500ms-1000ms
        timeout = std::chrono::milliseconds(std::rand() % 501 + 500);
    }

    void sendMsg() {
        for (;;) {
            if (state == "LEADER") {
                std::unique_lock<std::mutex> lock1(muStates);
                std::unique_lock<std::mutex> lock2(muPrint);

                for (int N = log_size - 1; N > commit_index; N--) {
                    int match_count = 0;
                    for (int i = 0; i < number_of_nodes; i++) {
                        if (i == node_id || match_index[i] >= N)
                            match_count++;
                    }
                    if (match_count > number_of_nodes / 2 && log[N].first == current_term) {
                        commit_index = N;
                        break;
                    }
                }
                if (commit_index > last_applied) {
                    std::cout << "STATE commitIndex=" << commit_index << std::endl;
                    for (int i = last_applied + 1; i <= commit_index; i++) 
                        std::cout << "COMMITTED " << log[i].second << ' ' << i << std::endl;
                    last_applied = commit_index;
                }

                for (int i = 0; i < number_of_nodes; i++) {
                    if (i == node_id)  continue;
                    int prev_log_index = next_index[i] - 1;
                    int prev_log_term = log[prev_log_index].first;
                    std::cout << "SEND " << i << " AppendEntries " << current_term << ' ' 
                              << prev_log_index << ' ' << prev_log_term << ' ' << commit_index;

                    // send entries[], empty for heartbeat
                    for (int j = next_index[i]; j < log_size; j++) 
                        std::cout << ' ' << log[j].first << ' ' << log[j].second;
                    std::cout << std::endl;
                }

                lock2.unlock();
                lock1.unlock();

                // set heartbeat time to 50ms
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            } else {
                std::lock_guard<std::mutex> lock1(muStates);
                std::lock_guard<std::mutex> lock2(muPrint);

                if (state == "CANDIDATE" && vote_count > number_of_nodes / 2) {
                    state = "LEADER";
                    leader_id = node_id;
                    vote_count = 0;
                    next_index.assign(number_of_nodes, log_size);    
                    match_index.assign(number_of_nodes, 0);      

                    std::cout << "STATE state=\"LEADER\"" << std::endl;
                    std::cout << "STATE leader=" << leader_id << std::endl;
                }

                auto end_time = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

                // election timeout occurs
                if (duration >= timeout) {
                    current_term++;
                    voted_for = node_id;
                    vote_count = 1;
                    leader_id = null;
                    state = "CANDIDATE";

                    timeout = std::chrono::milliseconds(std::rand() % 501 + 500);
                    start_time = std::chrono::steady_clock::now();
                    
                    std::cout << "STATE state=\"CANDIDATE\"" << std::endl;
                    std::cout << "STATE term=" << current_term << std::endl;

                    for (int i = 0; i < number_of_nodes; i++) {
                        if (i == node_id)  continue;
                        int last_log_index = log_size - 1;
                        int last_log_term = log.back().first;
                        std::cout << "SEND " << i << " RequestVotes " << current_term << ' ' 
                                  << last_log_index << ' ' << last_log_term << std::endl;
                    }
                }
            }
        }
    }

    void receiveMsg() {

        std::string line;
        while (std::getline(std::cin, line)) {
            std::istringstream iss(line);
            std::string token;
            std::vector<std::string> tokens;
            while (std::getline(iss, token, ' '))  
                tokens.push_back(token);

            std::lock_guard<std::mutex> lock1(muStates);
            std::lock_guard<std::mutex> lock2(muPrint);

            if (tokens[0] == "LOG") {
                if (state != "LEADER")  continue;
    
                log.emplace_back(current_term, tokens[1]);
                std::cout << "STATE log[" << log_size - 1 << "]=[" << log.back().first 
                          << ",\"" << log.back().second << "\"]" << std::endl;
                continue;
            }
    
            if (tokens[2] == "AppendEntries") {
                int src_id = std::stoi(tokens[1]);
                int term = std::stoi(tokens[3]);
                int prev_log_index = std::stoi(tokens[4]);
                int prev_log_term = std::stoi(tokens[5]);
                int leader_commit = std::stoi(tokens[6]);

                timeout = std::chrono::milliseconds(std::rand() % 501 + 500);
                start_time = std::chrono::steady_clock::now();

                if (term < current_term || prev_log_index >= log_size || log[prev_log_index].first != prev_log_term) {
                    std::cout << "SEND " << leader_id << " AppendEntriesResponse " << current_term << ' ' 
                              << log_size - 1 << " false" << std::endl;
                    continue;
                } 

                // convert to follower
                if (term > current_term || leader_id != src_id) {
                    leader_id = src_id;
                    state = "FOLLOWER";
                    vote_count = 0;
                    current_term = term;
                    std::cout << "STATE term=" << current_term << std::endl;
                    std::cout << "STATE state=\"FOLLOWER\"" << std::endl;
                    std::cout << "STATE leader=" << leader_id << std::endl;
                }

                std::vector<std::pair<int, std::string>> entries;
                for (int i = 7; i < static_cast<int>(tokens.size()); i += 2) 
                    entries.emplace_back(std::stoi(tokens[i]), tokens[i+1]);
                
                for (int i = 0; i < static_cast<int>(entries.size()); i++) {
                    if (log_size > prev_log_index + i + 1 && log[prev_log_index+i+1].first != entries[i].first) {
                        while (log_size > prev_log_index + i + 1)  log.pop_back();
                    }
                    if (log_size <= prev_log_index + i + 1) {    // log_size may change, so check again here
                        log.push_back(entries[i]);
                        std::cout << "STATE log[" << log_size - 1 << "]=[" << log.back().first 
                                  << ",\"" << log.back().second << "\"]" << std::endl;
                    }
                }

                if (leader_commit > commit_index) {
                    commit_index = std::min(leader_commit, log_size - 1);
                    std::cout << "STATE commitIndex=" << commit_index << std::endl;

                    if (commit_index > last_applied) {
                        for (int i = last_applied + 1; i <= commit_index; i++) 
                            std::cout << "COMMITTED " << log[i].second << ' ' << i << std::endl;
                        last_applied = commit_index;
                    }
                }

                std::cout << "SEND " << leader_id << " AppendEntriesResponse " << current_term << ' '
                          << log_size - 1 << " true" << std::endl;

            } else if (tokens[2] == "RequestVotes") {
                int candidate_id = std::stoi(tokens[1]);
                int term = std::stoi(tokens[3]);
                int last_log_index = std::stoi(tokens[4]);
                int last_log_term = std::stoi(tokens[5]);

                if (term > current_term) {
                    state = "FOLLOWER";
                    vote_count = 0;
                    voted_for = null;
                    leader_id = null;
                    current_term = term;
                    // cannot continue, still need to vote
                }

                if ((term < current_term) || (voted_for != null && voted_for != candidate_id)) {
                    std::cout << "SEND " << candidate_id << " RequestVotesResponse " << current_term 
                              << " false" << std::endl;
                } else if ((last_log_term < log.back().first) || (last_log_term == log.back().first &&
                            last_log_index < log_size - 1)) {
                    std::cout << "SEND " << candidate_id << " RequestVotesRes ponse " << current_term 
                              << " false" << std::endl;
                } else {
                    // here only reset timeout by granting vote to candidate (potential leader)
                    voted_for = candidate_id;

                    timeout = std::chrono::milliseconds(std::rand() % 501 + 500);
                    start_time = std::chrono::steady_clock::now();
                    // std::cout << "reset " << start_time.time_since_epoch().count() << ' ' << timeout.count() << std::endl;
                    std::cout << "SEND " << candidate_id << " RequestVotesResponse " << current_term 
                              << " true" << std::endl;
                }

            } else if (tokens[2] == "AppendEntriesResponse") {
                int term = std::stoi(tokens[3]);

                // ignore stale message
                if (term < current_term)  continue;

                // convert to follower
                if (term > current_term) {
                    state = "FOLLOWER";
                    vote_count = 0;
                    current_term = term;
                    std::cout << "STATE term=" << current_term << std::endl;
                    std::cout << "STATE state=\"FOLLOWER\"" << std::endl;
                    continue;
                }

                int follower_id = std::stoi(tokens[1]);
                int replicate_index = std::stoi(tokens[4]);     // the end of the log of the follower
                std::string success = tokens[5];

                if (success == "true") {
                    next_index[follower_id] = replicate_index + 1;
                    match_index[follower_id] = replicate_index;
                    
                } else  next_index[follower_id]--;

            } else {    // RequestVotesResponse 
                int term = std::stoi(tokens[3]);

                // ignore stale message
                if (term < current_term)  continue;

                // convert to follower
                if (term > current_term) {
                    state = "FOLLOWER";
                    vote_count = 0;
                    current_term = term;
                    std::cout << "STATE term=" << current_term << std::endl;
                    std::cout << "STATE state=\"FOLLOWER\"" << std::endl;
                    continue;
                }

                std::string vote_granted = tokens[4];
                if (vote_granted == "true")  vote_count++;
            }
        }
    }

    void Start() {
        
        // node starts with it clock on
        start_time = std::chrono::steady_clock::now();

        std::thread senderThread(&RaftNode::sendMsg, this);          // in charge of sending
        std::thread receiverThread(&RaftNode::receiveMsg, this);     // in charge of receiving and responding

        senderThread.join();
        receiverThread.join();
    }

private:
    int node_id;
    int number_of_nodes;
    std::chrono::milliseconds timeout;
    std::chrono::steady_clock::time_point start_time;

    // counters
    int vote_count;

    // persistent states
    int current_term;
    int voted_for;
    std::vector<std::pair<int, std::string>> log;

    // volatile states
    int commit_index;
    int last_applied;

    // for leader only
    std::vector<int> next_index;
    std::vector<int> match_index;

    // other states for output
    std::string state;
    int leader_id;

    // locks
    std::mutex muStates;
    std::mutex muPrint;
};

int main(int argc, char** argv) {

    if (argc < 3) {
        std::cout << "Usage: ./raft <node_id> <number_of_nodes>" << std::endl;
        return 1;
    }

    int node_id = std::stoi(argv[1]);
    int number_of_nodes = std::stoi(argv[2]);

    // timeout random seeds
    std::srand((unsigned int)(std::time(nullptr) + node_id));

    RaftNode node(node_id, number_of_nodes);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    node.Start();

    return 0;
}
