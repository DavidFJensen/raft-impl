package raft

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

/*=========================================
  ================= DEBUG =================
  =========================================*/

/*DEBUG for debug/run mode switch.*/
const DEBUG = 0

/*The five different colors for convenience of logging for different peers*/
var colorTheme = [...]string{
	"\033[1;32m", // green
	"\033[1;33m", // yellow
	"\033[1;34m", // blue
	"\033[1;31m", // red
	"\033[1;36m"} // cyan

/*=========================================
  ============ DATA STRUCTURE =============
  =========================================*/

const (
	/*RaftElectionTimeoutMin is the minimum timeout of leader election*/
	RaftElectionTimeoutMin = 400 * time.Millisecond
	/*RaftHeartBeatLoop is the time period of sending heart beat*/
	RaftHeartBeatLoop = 100 * time.Millisecond
)

/*ApplyMsg is the structure of apply message to state machine*/
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

/*LogEntry is the structure of each log entry, composed by its term and value*/
type LogEntry struct {
	Term  int
	Value interface{}
}

/*Get random election time from the range of [ETM, 2ETM]*/
func getRandElectionTimeout() time.Duration {
	return RaftElectionTimeoutMin + (time.Duration(rand.Int63()) % RaftElectionTimeoutMin)
}

/*RMutex is the structure of mutex lock used by Raft, for convenience of debugging*/
type RMutex struct {
	rf *Raft
	mu sync.Mutex
}

/*Lock for locking (using sync.Mutex)*/
func (mu *RMutex) Lock() {
	mu.mu.Lock()
}

/*Unlock for unlocking (using sync.Mutex)*/
func (mu *RMutex) Unlock() {
	mu.mu.Unlock()
}

/*RLogger is the structure of logger used by Raft, for convenience of debugging*/
type RLogger struct {
	rf     *Raft
	logger *log.Logger
}

/*Println for printing with \n ended (using log.Logger)*/
func (rl *RLogger) Println(v ...interface{}) {
	if DEBUG == 1 {
		rl.logger.Printf("%v\033[0m", v...)
	}
}

/*Printf for printing with format string (using log.Logger)*/
func (rl *RLogger) Printf(format string, v ...interface{}) {
	if DEBUG == 1 {
		rl.logger.Printf(format+"\033[0m", v...)
	}
}

/*Raft is the core structure of each server making consensus with RAFT*/
type Raft struct {
	/*Basic info*/
	peers []*labrpc.ClientEnd
	me    int
	state int // 0--follower; 1--candidate; 2--leader

	/*Channels for applying message and graceful shutdown*/
	applyCh chan ApplyMsg
	killCh  chan int

	/*Mutual Exclusion*/
	mu *RMutex

	/*Persister and persistent states*/
	persister   *Persister
	logs        []LogEntry
	currentTerm int
	votedFor    int
	voteSet     []bool

	/*Timers for election and heart beat period*/
	electionTimeout *time.Timer
	heartBeatTimer  *time.Timer

	/*Volatile states for committing and applying*/
	commitIndex int
	lastApplied int

	/*Volatile states on leaders*/
	nextIndex  []int
	matchIndex []int

	/*Logging for debugging*/
	logger *RLogger
}

/*RequestVoteArgs is the structure of RequestVote RPC arguments*/
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

/*RequestVoteReply is the structure of RequestVote RPC reply*/
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

/*AppendEntriesArgs is the structure of AppendEntries RPC arguments*/
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

/*AppendEntriesReply is the structure of AppendEntries RPC reply*/
type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConflictTerm int
	FirstIDTerm  int
}

/*=========================================
  =========== CLIENT/TEST API  ============
  =========================================*/

/*GetState returns currentTerm and whether this server believes it is the leader*/
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == 2
}

/*Start is used by clients to start agreement on the next command to be appended to Raft's logs*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == 2
	if !isLeader {
		rf.persist()
		rf.mu.Unlock()
		return index, term, isLeader
	}
	index = len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
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
				if rf.checkKill() {
					return
				}
				rf.mu.Lock()
				if rf.state != 2 || rf.nextIndex[i] > len(rf.logs)-1 {
					rf.persist()
					rf.mu.Unlock()
					return
				}
				args, reply := rf.makeNewAppendEntries(i, false)
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

/*Make is used by clients or tester to create a Raft instance*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())

	/*Initilization*/
	rf := &Raft{}
	rf.mu = &RMutex{}
	rf.mu.rf = rf
	rf.me = me
	prefix := fmt.Sprintf("%v[peer %v] ", colorTheme[rf.me%len(colorTheme)], rf.me)
	logger := log.New(os.Stdout, prefix, log.Ldate|log.Lmicroseconds|log.Lshortfile)
	rf.logger = &RLogger{}
	rf.logger.logger = logger
	rf.logger.rf = rf
	rf.peers = peers
	rf.persister = persister
	rf.applyCh = applyCh
	rf.killCh = make(chan int)
	rf.state = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteSet = make([]bool, len(peers))
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.electionTimeout = time.NewTimer(getRandElectionTimeout())
	rf.heartBeatTimer = time.NewTimer(RaftHeartBeatLoop)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	/*Initialize from state persisted before a crash*/
	rf.readPersist(persister.ReadRaftState())

	/*Start a goroutine for listening the timeout of election*/
	go func(rf *Raft) {
		for {
			select {
			case <-rf.electionTimeout.C:
				go func() {
					if rf.checkKill() {
						return
					}
					rf.startElection()
				}()
			case <-rf.killCh:
				return
			}
		}
	}(rf)

	/*Start a goroutine for controlling periodical heart beat sending*/
	go func(rf *Raft) {
		for {
			select {
			case <-rf.heartBeatTimer.C:
				go func() {
					if rf.checkKill() {
						return
					}
					rf.sendHeartBeat()
				}()
			case <-rf.killCh:
				return
			}
		}
	}(rf)

	return rf
}

/*Kill is used by tester to shutdown a Raft instance. Graceful shutdown is implemented.*/
func (rf *Raft) Kill() {
	close(rf.killCh)
}

/*=========================================
  ============= PERSISTENCE  ==============
  =========================================*/

/*Save Raft's persistent states to stable storage*/
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.voteSet)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

/*Restore previously persisted states*/
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
	d.Decode(&rf.voteSet)
}

/*=========================================
  ======== RPC HANDLER (RECEIVER)  ========
  =========================================*/

/*RequestVote is the handler of RequestVote RPC*/
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	if rf.checkKill() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	replyTerm := args.Term
	rf.electionTimeout.Reset(getRandElectionTimeout())
	if rf.currentTerm > args.Term {
		replyTerm = rf.currentTerm
		reply.Term = replyTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.becomeFollower()
	}
	if ((args.Term > rf.currentTerm) ||
		(args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateID))) &&
		(len(rf.logs) == 0 || args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
			(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1)) {
		reply.Term = replyTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	} else {
		reply.Term = replyTerm
		reply.VoteGranted = false
	}
	rf.currentTerm = args.Term
}

/*AppendEntries is the handler of AppendEntries RPC*/
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.checkKill() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	replyTerm := args.Term
	if rf.currentTerm > args.Term {
		replyTerm = rf.currentTerm
	} else {
		rf.becomeFollower()
		rf.currentTerm = args.Term
	}
	reply.Term = replyTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.FirstIDTerm = -1
		reply.ConflictTerm = -1
	} else {
		reply.Success = true
		rf.electionTimeout.Reset(getRandElectionTimeout())
		rf.tryApply()
		if args.PrevLogIndex >= len(rf.logs) ||
			(args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term) {
			reply.Success = false
			if args.PrevLogIndex < len(rf.logs) {
				reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
				t := args.PrevLogIndex
				for ; t >= 0; t-- {
					if t < len(rf.logs) && rf.logs[t].Term != reply.ConflictTerm {
						break
					}
				}
				reply.FirstIDTerm = t + 1
			} else {
				reply.ConflictTerm = 1
				reply.FirstIDTerm = len(rf.logs)
			}
		} else if len(args.Entries) > 0 {
			reply.Success = true
			k := args.PrevLogIndex + 1
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
			for i := k; i < args.PrevLogIndex+1+len(args.Entries); i++ {
				entry := args.Entries[i-args.PrevLogIndex-1]
				rf.logs = append(rf.logs, LogEntry{entry.Term, entry.Value})
			}
		}
		if reply.Success && args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if rf.commitIndex > args.PrevLogIndex+len(args.Entries) {
				rf.commitIndex = args.PrevLogIndex + len(args.Entries)
			}
		}
	}
}

/*=========================================
  ======= RPC HANDLER ((RE)SENDER)  =======
  =========================================*/

/*Send RequestVote RPC to server, and updating/resending when reply returned*/
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.checkKill() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if ok {
		if rf.currentTerm < reply.Term {
			rf.becomeFollower()
			rf.currentTerm = reply.Term
		}
		if reply.VoteGranted {
			rf.voteSet[server] = true
		} else {
		}
		voteNum := 0
		for i := range rf.voteSet {
			if rf.voteSet[i] {
				voteNum++
			}
		}
		if voteNum > len(rf.peers)/2 {
			if rf.state != 2 {
				rf.becomeLeader()
			}
		}
	} else {
		newArgs, newReply := rf.copyRequestVote(args)
		rf.persist()
		rf.mu.Unlock()
		rf.sendRequestVote(server, newArgs, &newReply)
		rf.mu.Lock()
	}
	defer rf.mu.Unlock()
	defer rf.persist()
	return ok
}

/*Send AppendEntries RPC to server, and updating/resending when reply returned*/
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.checkKill() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	rf.tryApply()
	if ok {
		if rf.currentTerm < reply.Term {
			rf.becomeFollower()
			rf.currentTerm = reply.Term
		}
		if len(args.Entries) > 0 && rf.state == 2 {
			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else {
				if args.PrevLogIndex >= 0 {
					if reply.ConflictTerm != -1 {
						rf.nextIndex[server] = reply.FirstIDTerm
					} else {
						rf.nextIndex[server] = args.PrevLogIndex
					}
					newArgs, newReply := rf.makeNewAppendEntries(server, false)
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
				break
			}
		}
	} else {
		if rf.state == 2 && len(args.Entries) > 0 {
			newArgs, newReply := rf.copyAppendEntries(args)
			rf.persist()
			rf.mu.Unlock()
			rf.sendAppendEntries(server, newArgs, &newReply)
			rf.mu.Lock()
		}
	}
	defer rf.mu.Unlock()
	defer rf.persist()
	return ok
}

/*=========================================
  ========= EVENT & STATE SWITCH  =========
	=========================================*/

/*Sending heart beat periodically*/
func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	if rf.state != 2 {
		rf.persist()
		rf.mu.Unlock()
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.persist()
		rf.mu.Unlock()
		go func(i int, rf *Raft) {
			if rf.checkKill() {
				return
			}
			rf.mu.Lock()
			if rf.state != 2 {
				rf.persist()
				rf.mu.Unlock()
				return
			}
			args, reply := rf.makeNewAppendEntries(i, true)
			rf.persist()
			rf.mu.Unlock()
			rf.sendAppendEntries(i, args, &reply)
		}(i, rf)
		rf.mu.Lock()
	}
	rf.heartBeatTimer.Reset(RaftHeartBeatLoop)

	rf.persist()
	rf.mu.Unlock()
}

/*Start a new election and becomes a candidate*/
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == 2 {
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.tryApply()
	rf.becomeCandidate()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.persist()
		rf.mu.Unlock()
		go func(i int, rf *Raft) {
			if rf.checkKill() {
				return
			}
			rf.mu.Lock()
			if rf.state != 1 {
				rf.persist()
				rf.mu.Unlock()
				return
			}
			args, reply := rf.makeNewRequestVote()
			rf.persist()
			rf.mu.Unlock()
			rf.sendRequestVote(i, args, &reply)
		}(i, rf)
		rf.mu.Lock()
	}
	rf.persist()
	rf.mu.Unlock()
}

/*Become a follower*/
func (rf *Raft) becomeFollower() {
	for i := range rf.voteSet {
		rf.voteSet[i] = false
	}
	rf.electionTimeout.Reset(getRandElectionTimeout())
	rf.state = 0
	rf.persist()
	rf.logger.Printf("become a follower. My term is %v.", rf.currentTerm)
}

/*Become a candidate*/
func (rf *Raft) becomeCandidate() {
	for i := range rf.voteSet {
		rf.voteSet[i] = false
	}
	rf.state = 1
	rf.currentTerm++
	rf.voteSet[rf.me] = true
	rf.votedFor = rf.me
	rf.electionTimeout.Reset(getRandElectionTimeout())
	rf.persist()
	rf.logger.Printf("become a candidate. My term is %v.", rf.currentTerm)
}

/*Become a leader*/
func (rf *Raft) becomeLeader() {
	rf.state = 2
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = -1
	}
	rf.persist()
	rf.logger.Printf("become a leader. My term is %v.", rf.currentTerm)
	rf.mu.Unlock()
	if rf.checkKill() {
		return
	}
	rf.sendHeartBeat()
	rf.mu.Lock()
}

/*=========================================
  ========== UTILITY FUNCTIONS  ===========
  =========================================*/

/*For sending kill signal to goroutines for graceful shutdown*/
func (rf *Raft) checkKill() bool {
	select {
	case <-rf.killCh:
		return true
	default:
		return false
	}
}

/*Make new AppendEntries RPC*/
func (rf *Raft) makeNewAppendEntries(server int, isHearbeat bool) (AppendEntriesArgs, AppendEntriesReply) {
	var reply AppendEntriesReply
	lastLogIndex := len(rf.logs) - 1
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	}
	if !isHearbeat {
		for j := rf.nextIndex[server]; j <= lastLogIndex; j++ {
			args.Entries = append(args.Entries, LogEntry{rf.logs[j].Term, rf.logs[j].Value})
		}
	}
	args.LeaderCommit = rf.commitIndex
	return args, reply
}

func (rf *Raft) makeNewRequestVote() (RequestVoteArgs, RequestVoteReply) {
	var reply RequestVoteReply
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.logs) - 1
	if len(rf.logs) > 0 {
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	return args, reply
}

/*Copy old AppendEntries RPC to new ones for resending*/
func (rf *Raft) copyAppendEntries(args AppendEntriesArgs) (AppendEntriesArgs, AppendEntriesReply) {
	var newReply AppendEntriesReply
	newArgs := AppendEntriesArgs{}
	newArgs.Term = args.Term
	newArgs.LeaderID = args.LeaderID
	newArgs.PrevLogIndex = args.PrevLogIndex
	newArgs.PrevLogTerm = args.PrevLogTerm
	newArgs.Entries = args.Entries
	newArgs.LeaderCommit = args.LeaderCommit
	return newArgs, newReply
}

/*Copy old RequestVote RPC to new ones for resending*/
func (rf *Raft) copyRequestVote(args RequestVoteArgs) (RequestVoteArgs, RequestVoteReply) {
	var newReply RequestVoteReply
	newArgs := RequestVoteArgs{}
	newArgs.Term = args.Term
	newArgs.CandidateID = args.CandidateID
	newArgs.LastLogIndex = args.LastLogIndex
	newArgs.LastLogTerm = args.LastLogTerm
	return newArgs, newReply
}

/*Try to apply*/
func (rf *Raft) tryApply() {
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{}
			applyMsg.Index = i + 1
			applyMsg.Command = rf.logs[i].Value
			rf.applyCh <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
	}
}
