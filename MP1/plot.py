import numpy as np
import matplotlib.pyplot as plt
import sys

path = sys.argv[1]
fail = False
if len(sys.argv) > 2:
    fail = True

if path == "three_nodes":
    num_of_nodes = 3
elif path == "eight_nodes":
    num_of_nodes = 8
else:
    print("Wrong path!")
    exit()

msg = {}
for index in range(1, num_of_nodes+1):
    bandwidth = []
    with open(path + "/bw_node" + str(index) + ".txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            bandwidth.append(int(line))
        
    lim = 100 if not fail else 200
    if len(bandwidth) < lim:
        lim = len(bandwidth)

    plt.figure()
    plt.title("Bandwidth of Node" + str(index) + (" (with failure)" if fail else ""))
    plt.xlabel("time (s)")
    plt.ylabel("bandwidth (bytes/s)")
    if num_of_nodes == 3:
        plt.ylim(-500, np.max(bandwidth[1:lim])+500)
    else:
        plt.ylim(np.min(bandwidth[:lim])-5000, np.max(bandwidth[np.min([4, lim-1]):lim])+5000)
    plt.plot(np.linspace(1, lim, lim), bandwidth[:lim])

    if not fail:
        plt.savefig(path + "/bw_node" + str(index) + ".png")
    else:
        plt.savefig(path + "/bw_node" + str(index) + "_fail.png")


    with open(path + "/time_node" + str(index) + ".txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            data = line.split("}} ")
            if not data[0] in msg:
                msg[data[0]] = [float(data[1])]
            else:
                msg[data[0]].append(float(data[1]))
        
delay = []
for key in msg:
    v = sorted(msg[key])
    delay.append([v[-1]-v[0], v[0]])

delay.sort(key=lambda x:x[1])
lim = len(delay)

plt.figure()
plt.title("Delay in Message Propagation" + (" (with failure)" if fail else ""))
plt.xlabel("message index")
plt.ylabel("delay (ms)")
plt.plot(np.linspace(1, lim, lim), np.array(delay)[:, 0])

if not fail:
    plt.savefig(path + "/msg_delay" + ".png")
else:
    plt.savefig(path + "/msg_delay" + "_fail.png")