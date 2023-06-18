### Simple Raft Implementation
---

This simple [Raft](https://raft.github.io/) implementation supports **leader election** and **log consensus** with **node failures** and **partitions**. 
However, it does not support *crash recovery*, *log compaction*, and *cluster membership changes*. 

We use [Raft Testing Framework](https://github.com/nikitaborisov/raft_mp) to test the functionality of the implementation. This framework is also
forked in the directory <kbd>raft_mp</kbd>. In order to log messages, please install the <kbd>aioconsole</kbd> module.

For more information, you can refer to https://courses.engr.illinois.edu/cs425/fa2021/mps/mp2.html.
