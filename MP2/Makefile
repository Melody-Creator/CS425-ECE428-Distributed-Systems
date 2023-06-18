CC = g++
CFLAGS = -std=c++11 -Wall -Wextra -Wshadow

all: raft

raft: raft.cpp
	$(CC) $(CFLAGS) -o raft raft.cpp -lpthread

clean:
	rm -f raft
