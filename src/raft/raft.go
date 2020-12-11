package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DEBUG = 0

// import "bytes"
// import "encoding/gob"

const (
	RaftElectionTimeoutMin = 150 * time.Millisecond
	RaftHeartBeatLoop      = 50 * time.Millisecond
	RaftBroadcastTimeout   = 10 * time.Millisecond
)

var colorTheme = [...]string{
	"\033[1;32m", // green
	"\033[1;33m", // yellow
	"\033[1;34m", // blue
	"\033[1;31m", // red
	"\033[1;36m"} // cyan

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term  int
	Value interface{}
}

func (le LogEntry) equal(le1 LogEntry) bool {
	return le.Term == le1.Term && le.Value == le1.Value
}

func getRandElectionTimeout() time.Duration {
	ret := RaftElectionTimeoutMin + (time.Duration(rand.Int63()) % RaftElectionTimeoutMin)
	// fmt.Printf("Get new electionTimeout: %v\n", ret)
	return ret
}

type RaftMutex struct {
	rf *Raft
	mu sync.Mutex
}

func (mu *RaftMutex) Lock() {
	// mu.rf.logger.Println("{lock}")
	mu.mu.Lock()
}

func (mu *RaftMutex) Unlock() {
	// mu.rf.logger.Println("{Unlock}")
	mu.mu.Unlock()
}

type RaftLogger struct {
	rf     *Raft
	logger *log.Logger
}

func (rl *RaftLogger) Println(v ...interface{}) {
	if DEBUG == 1 {
		rl.logger.Printf("%v\033[0m", v...)
	}
}

func (rl *RaftLogger) Printf(format string, v ...interface{}) {
	if DEBUG == 1 {
		rl.logger.Printf(format+"\033[0m", v...)
	}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        *RaftMutex
	wg        sync.WaitGroup
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg
	logs    []LogEntry

	state       int // 0 -- follower; 1 -- candidate; 2 -- leader
	currentTerm int
	votedFor    int
	voteNum     int

	electionTimeout *time.Timer
	heartBeatTimer  *time.Timer
	killCh          chan int

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	nextIndex  []int // [FOR LEADER] for each peer, index of the next log entry to send to that server
	matchIndex []int // [FOR LEADER] for each peer, index of highest log entry known to be replicated

	// helper
	logger *RaftLogger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == 2
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.voteNum)
	// e.Encode(rf.nextIndex)
	// e.Encode(rf.matchIndex)
	// e.Encode(rf.state)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.logger.Printf("reading! before read...")
	// rf.logger.Printf("currentTerm: %v, votedFor: %v, logs: %v, voteNum: %v, me: %v, commitindex: %v, lastappled: %v, state: %v",
		// rf.currentTerm, rf.votedFor, rf.logs, rf.voteNum, rf.me, rf.commitIndex, rf.lastApplied, rf.state)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
	d.Decode(&rf.voteNum)
	// d.Decode(&rf.nextIndex)
	// d.Decode(&rf.matchIndex)
	// d.Decode(&rf.state)
	// rf.logger.Printf("After read.")
	// rf.logger.Printf("currentTerm: %v, votedFor: %v, logs: %v, voteNum: %v, me: %v, commitindex: %v, lastappled: %v, state: %v",
		// rf.currentTerm, rf.votedFor, rf.logs, rf.voteNum, rf.me, rf.commitIndex, rf.lastApplied, rf.state)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term              int
	Success           bool
	ExpectedNextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// rf.logger.Printf("Receive vote request from peer %v in term %v, my term is %v. Now my votedFor: %v.",
	// 	args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
	replyTerm := args.Term
	if rf.currentTerm > args.Term {
		replyTerm = rf.currentTerm
		reply.Term = replyTerm
		reply.VoteGranted = false
		return
	}
	// rf.electionTimeout.Reset(getRandElectionTimeout())
	// if rf.state == 1 || rf.state == 2 {
	// 	rf.becomeFollower()
	// }
	// if len(rf.logs) > 0 {
	// 	rf.logger.Printf("The vote info: args.LastLogTerm: %v, my last log term: %v. args.LastLogIndex: %v, my last index: %v",
	// 		args.LastLogTerm, rf.logs[len(rf.logs)-1].Term, args.LastLogIndex, len(rf.logs)-1)
	// }
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(len(rf.logs) == 0 || args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
			(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term &&
				args.LastLogIndex >= len(rf.logs)-1)) {
		reply.Term = replyTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.Term = replyTerm
		reply.VoteGranted = false
	}
	rf.currentTerm = args.Term
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	replyTerm := args.Term
	if len(args.Entries) != 0 {
		rf.logger.Printf("Receive append entries %v from %v, leader's Term: %v, my Term: %v",
			args.Entries, args.LeaderId, args.Term, rf.currentTerm)
	} else {
		// rf.logger.Printf("Receive heartbeat %v from %v, leader's Term: %v, my Term: %v",
		// 	args.Entries, args.LeaderId, args.Term, rf.currentTerm)
	}
	rf.logger.Printf("args.PrevLogIndex: %v, args.PrevLogTerm: %v",
		args.PrevLogIndex, args.PrevLogTerm)
	if rf.currentTerm > args.Term {
		replyTerm = rf.currentTerm
	} else {
		rf.becomeFollower()
		rf.currentTerm = args.Term
	}
	reply.Term = replyTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.ExpectedNextIndex = -1
	} else {
		reply.Success = true
		rf.electionTimeout.Reset(getRandElectionTimeout())
		rf.logger.Printf("Checking whether to apply. commitIndex: %v, lastApplied: %v", rf.commitIndex, rf.lastApplied)
		// safe to apply
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsg := ApplyMsg{}
				applyMsg.Index = i + 1
				applyMsg.Command = rf.logs[i].Value
				rf.applyCh <- applyMsg
			}
			rf.lastApplied = rf.commitIndex
		}
		if args.PrevLogIndex >= len(rf.logs) ||
			(args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term) {
			reply.Success = false
			if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.logs) {
				badTerm := rf.logs[args.PrevLogIndex].Term
				t := args.PrevLogIndex
				for ; t >= 0; t-- {
					if rf.logs[t].Term != badTerm {
						break
					}
				}
				reply.ExpectedNextIndex = t + 1
			} else {
				reply.ExpectedNextIndex = -1
			}
			rf.logger.Printf("Not match!!! my len(logs): %v, my logs: %v", len(rf.logs), rf.logs)
		} else if len(args.Entries) > 0 {
			rf.logger.Printf("Receive append entries %v from peer %v in term %v, receiver's term is %v.",
				args.Entries, args.LeaderId, args.Term, rf.currentTerm)
			rf.logger.Printf("before updating, args.PrevLogIndex: %v\tlen(rf.logs): %v, rf.logs: %v",
				args.PrevLogIndex, len(rf.logs), rf.logs)
			reply.Success = true
			k := args.PrevLogIndex + 1
			// rf.logger.Printf("k: %v, upperbound: %v", k, args.PrevLogIndex+1+len(args.Entries))
			for ; k < args.PrevLogIndex+1+len(args.Entries); k++ {
				if len(rf.logs)-1 >= k {
					if rf.logs[k].Term != args.Entries[k-args.PrevLogIndex-1].Term {
						rf.logs = rf.logs[:k]
						break
					}
				} else {
					break
				}
			}
			// rf.logger.Printf("k: %v, upperbound: %v", k, args.PrevLogIndex+1+len(args.Entries))
			for i := k; i < args.PrevLogIndex+1+len(args.Entries); i++ {
				entry := args.Entries[i-args.PrevLogIndex-1]
				rf.logs = append(rf.logs, LogEntry{entry.Term, entry.Value})
			}
			rf.logger.Printf("after updating, logs: %v", len(rf.logs))
		}
		if reply.Success && args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if rf.commitIndex > len(rf.logs)-1 {
				rf.commitIndex = len(rf.logs) - 1
			}
			rf.logger.Printf("rf.commitIndex updated to %v", rf.commitIndex)
		}
		rf.logger.Printf("rf.commitIndex: %v", rf.commitIndex)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	// rf.logger.Printf("send request vote to peer %v.", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if ok {
		if rf.currentTerm < reply.Term {
			rf.becomeFollower()
			rf.currentTerm = reply.Term
		}
		if reply.VoteGranted {
			rf.voteNum++
			// rf.logger.Printf("get voted from %v! voteNum: %v", server, rf.voteNum)
		} else {
			// rf.logger.Printf("get unvoted from %v. voteNum: %v", server, rf.voteNum)
		}
		if rf.voteNum > len(rf.peers)/2 {
			if rf.state != 2 {
				rf.becomeLeader()
			}
		}
	}
	defer rf.mu.Unlock()
	defer rf.persist()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	// safe to apply
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{}
			applyMsg.Index = i + 1
			applyMsg.Command = rf.logs[i].Value
			rf.applyCh <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
	}
	if ok {
		if rf.currentTerm < reply.Term {
			rf.logger.Printf("Fuck, My term is %v and I received an AppendEntryRPC from %v in term %v.", rf.currentTerm,
				server, reply.Term)
			rf.becomeFollower()
			rf.currentTerm = reply.Term
		}
		if len(args.Entries) > 0 && rf.state == 2 {
			rf.logger.Printf("Before updating nextindex for peer %v, nextindex: %v",
				server, rf.nextIndex[server])
			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.logger.Printf("Suceess. Update nextindex for peer %v: %v, because newly add %v",
					server, rf.nextIndex[server], args.Entries)
			} else {
				rf.logger.Printf("Unmatch of %v. nextindex: %v. args: %v", server, rf.nextIndex[server], args)
				if args.PrevLogIndex >= 0 {
					if reply.ExpectedNextIndex != -1 {
						rf.nextIndex[server] = reply.ExpectedNextIndex
					} else {
						rf.nextIndex[server] = args.PrevLogIndex
					}
					newArgs := AppendEntriesArgs{}
					newArgs.Term = rf.currentTerm
					newArgs.LeaderId = rf.me
					newArgs.PrevLogIndex = rf.nextIndex[server] - 1
					if newArgs.PrevLogIndex >= 0 {
						newArgs.PrevLogTerm = rf.logs[newArgs.PrevLogIndex].Term
					}
					lastLogIndex := len(rf.logs) - 1
					for j := rf.nextIndex[server]; j <= lastLogIndex; j++ {
						newArgs.Entries = append(newArgs.Entries, LogEntry{rf.logs[j].Term, rf.logs[j].Value})
					}
					newArgs.LeaderCommit = rf.commitIndex
					rf.logger.Printf("Sending new entries to peer %v, entries: %v, leaderCommit: %v",
						server, newArgs.Entries, newArgs.LeaderCommit)
					rf.logger.Printf("nextIndex[%v]: %v, lastLogIndex: %v", server, rf.nextIndex[server], lastLogIndex)
					var newReply AppendEntriesReply
					rf.persist()
					rf.mu.Unlock()
					rf.sendAppendEntries(server, newArgs, &newReply)
					rf.mu.Lock()
				}
			}
		}
		// set commitIndex
		for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
			if rf.logs[N].Term != rf.currentTerm {
				continue
			}
			cnt := 0

			for i := range rf.matchIndex {
				if rf.matchIndex[i] >= N {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = N
				rf.logger.Printf("set rf.commitIndex: %v, cnt: %v, len/2: %v",
					rf.commitIndex, cnt, len(rf.peers)/2)
				break
			}
		}
	} else {
		if rf.state == 2 && len(args.Entries) > 0 {
			rf.persist()
			var newReply AppendEntriesReply
			rf.mu.Unlock()
			time.Sleep(RaftHeartBeatLoop)
			rf.sendAppendEntries(server, args, &newReply)
			rf.mu.Lock()
		}
	}
	defer rf.mu.Unlock()
	defer rf.persist()
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	rf.logger.Printf("Called Start() on command: %v", command)
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == 2
	if !isLeader {
		// rf.logger.Printf("I am not a leader, return.")
		rf.mu.Unlock()
		return index, term, isLeader
	}
	rf.logger.Printf("Start agreement on command: %v [%T]", command, command)
	index = len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
	rf.logger.Printf("I am the leader. I append the new command, now logs: %v", rf.logs)
	rf.nextIndex[rf.me] = len(rf.logs)
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if len(rf.logs)-1 >= rf.nextIndex[i] {
			rf.persist()
			rf.mu.Unlock()
			go func(i int, rf *Raft) {
				rf.mu.Lock()
				if rf.state != 2 || rf.nextIndex[i] > len(rf.logs) - 1 {
					rf.persist()
					rf.mu.Unlock()
					return
				}
				lastLogIndex := len(rf.logs) - 1
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				if args.PrevLogIndex >= 0 {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				}
				for j := rf.nextIndex[i]; j <= lastLogIndex; j++ {
					args.Entries = append(args.Entries, LogEntry{rf.logs[j].Term, rf.logs[j].Value})
				}
				args.LeaderCommit = rf.commitIndex
				rf.logger.Printf("Sending new entries to peer %v, entries: %v, leaderCommit: %v",
					i, args.Entries, args.LeaderCommit)
				rf.logger.Printf("nextIndex[%v]: %v, lastLogIndex: %v", i, rf.nextIndex[i], lastLogIndex)
				var reply AppendEntriesReply
				rf.persist()
				rf.mu.Unlock()
				rf.sendAppendEntries(i, args, &reply)
			}(i, rf)
			rf.mu.Lock()
		}
	}
	defer rf.mu.Unlock()
	defer rf.persist()
	return index + 1, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.killCh <- 0
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == 2 {
		rf.mu.Unlock()
		return
	}
	rf.logger.Println("election timeout!")
	rf.voteNum = 0
	rf.votedFor = -1
	if rf.state == 1 {
		rf.logger.Printf("To avoid split, I begin waiting...")
		rf.persist()
		rf.mu.Unlock()
		time.Sleep(getRandElectionTimeout())
		rf.mu.Lock()
		rf.logger.Printf("Finish waiting! My state: %v", rf.state)
		if rf.state != 1 {
			rf.persist()
			rf.mu.Unlock()
			return
		}
	}
	rf.logger.Println("start election.")
	rf.state = 1 // change to candidate
	rf.logger.Println("become a candidate.")
	rf.currentTerm++ // increment currentTerm
	rf.logger.Printf("My term increases to %v", rf.currentTerm)
	rf.voteNum = 1
	rf.votedFor = rf.me                                // Vote for self
	rf.electionTimeout.Reset(getRandElectionTimeout()) // Reset election Timer
	rf.wg.Add(len(rf.peers) - 1)
	// Make args of RequestVoteRPC
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.persist()
		rf.mu.Unlock()
		go func(i int, rf *Raft) {
			rf.mu.Lock()
			if rf.state != 1 {
				rf.mu.Unlock()
				return
			}
			defer rf.wg.Done()
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.logs) - 1 //FIXME:
			if len(rf.logs) > 0 {
				args.LastLogTerm = rf.logs[len(rf.logs)-1].Term // FIXME:
			}
			var reply RequestVoteReply
			rf.mu.Unlock()
			rf.sendRequestVote(i, args, &reply)
		}(i, rf)
		rf.mu.Lock()
	}
	waitTimeout(rf.wg, RaftBroadcastTimeout)
	rf.persist()
	rf.mu.Unlock()
}

func waitTimeout(wg sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false
	case <-time.After(timeout):
		return true
	}
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	if rf.state != 2 {
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.wg.Add(len(rf.peers) - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.persist()
		rf.mu.Unlock()
		go func(i int, rf *Raft) {
			rf.mu.Lock()
			defer rf.wg.Done()
			if rf.state != 2 {
				rf.mu.Unlock()
				return
			}
			// rf.logger.Printf("Prepare to sending heart beat, my state: %v", rf.state)
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = len(rf.logs) - 1
			if len(rf.logs) > 0 {
				args.PrevLogTerm = rf.logs[len(rf.logs)-1].Term
			}
			args.LeaderCommit = rf.commitIndex
			var reply AppendEntriesReply
			// rf.logger.Printf("send heart beat to peer %v", i)
			rf.mu.Unlock()
			rf.sendAppendEntries(i, args, &reply)
		}(i, rf)
		rf.mu.Lock()
	}
	waitTimeout(rf.wg, RaftBroadcastTimeout)

	// rf.logger.Printf("finish heartbeat sending. timeout? %v", timeout)
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) becomeFollower() {
	if rf.state != 0 {
		rf.logger.Println("become a follower.")
	}
	rf.voteNum = 0
	rf.votedFor = -1
	rf.electionTimeout.Reset(getRandElectionTimeout())
	rf.state = 0
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.logger.Println("become a leader.")
	rf.state = 2
	rf.voteNum = 0
	rf.votedFor = -1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = -1
	}
	rf.persist()
	rf.mu.Unlock()
	rf.sendHeartBeat()
	rf.mu.Lock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())
	rf := &Raft{}
	rf.mu = &RaftMutex{}
	rf.mu.rf = rf
	rf.me = me
	prefix := fmt.Sprintf("%v[peer %v] ", colorTheme[rf.me%len(colorTheme)], rf.me)
	logger := log.New(os.Stdout, prefix, log.Ldate|log.Lmicroseconds|log.Lshortfile)
	rf.logger = &RaftLogger{}
	rf.logger.logger = logger
	rf.logger.rf = rf
	rf.peers = peers
	rf.persister = persister

	// Your initialization code here.
	rf.applyCh = applyCh
	rf.killCh = make(chan int)
	rf.state = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.electionTimeout = time.NewTimer(getRandElectionTimeout())
	rf.heartBeatTimer = time.NewTimer(RaftHeartBeatLoop)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// rf.logger.Printf("Finish initialization, state: %v", rf.state)

	go func(rf *Raft) {
		for {
			select {
			case <-rf.electionTimeout.C:
				go func() {
					rf.startElection()
				}()
			case <-rf.killCh:
				return
			}
		}
	}(rf)

	go func(rf *Raft) {
		for {
			select {
			case <-rf.heartBeatTimer.C:
				rf.heartBeatTimer.Reset(RaftHeartBeatLoop)
				go func() {
					rf.sendHeartBeat()
				}()
			case <-rf.killCh:
				return
			}
		}
	}(rf)

	// go func(rf *Raft) {
	// 	rf.logger.Println("Start goroutine for listening apply message.")
	// 	for {
	// 		select {
	// 		case msg := <-rf.applyCh:
	// 			rf.sendMessageEntry(msg)
	// 		case <-rf.killCh:
	// 			return
	// 		}
	// 	}
	// }(rf)

	return rf
}
