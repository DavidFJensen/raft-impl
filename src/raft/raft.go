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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "log"
import "os"
import "fmt"

// import "bytes"
// import "encoding/gob"

var (
	WarningLogger *log.Logger
	InfoLogger    *log.Logger
	ErrorLogger   *log.Logger
)

const (
	RaftElectionTimeoutMin = 500 * time.Millisecond
	RaftAppendEntryTimeout = 200 * time.Millisecond
	RaftHeartBeatLoop = 150 * time.Millisecond
)


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
	term				int
	value				int
}

func (le *LogEntry) equal(le1 *LogEntry) bool {
	return le.term == le1.term && le.value == le1.value
}

func getRandElectionTimeout() time.Duration {
	return RaftElectionTimeoutMin + time.Duration(rand.Int63()) % RaftElectionTimeoutMin
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        		sync.Mutex
	peers     		[]*labrpc.ClientEnd
	persister 		*Persister
	me        		int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh 			chan ApplyMsg
	logs					[]*LogEntry

	state					int // 0 -- follower; 1 -- candidate; 2 -- leader
	currentTerm		int
	votedFor			interface{}
	voteNum				int

	appendEntryTimeout	*time.Timer
	electionTimeout			*time.Timer
	heartBeatTimer			*time.Timer
	killCh				chan int

	commitIndex		int // index of highest log entry known to be committed
	lastApplied		int // index of highest log entry applied to state machine

	nextIndex			[]int // [FOR LEADER] for each peer, index of the next log entry to send to that server
	matchIndex		[]int // [FOR LEADER] for each peer, index of highest log entry known to be replicated

	// helper
	logger				*log.Logger
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
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	term						int
	candidateId			int
	lastLogIndex		int
	lastLogTerm			int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	term						int
	voteGranted			bool
}

type AppendEntriesArgs struct {
	term						int
	leaderId				int
	prevLogIndex		int
	prevLogTerm			int
	entries					[]*LogEntry
	leaderCommit		int
}

type AppendEntriesReply struct {
	term						int
	success					bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock() 
	rf.logger.Printf("Receive vote request from peer %v in term %v, receiver's term is %v.", 
		args.candidateId, args.term, rf.currentTerm)
	reply_term := args.term
	if rf.currentTerm > args.term {
		reply_term = rf.currentTerm
	} else if rf.currentTerm < args.term{
		rf.state = 0
		rf.currentTerm = args.term
	}
	if args.term >= rf.currentTerm && (rf.votedFor == nil || rf.votedFor == args.candidateId) && 
		(len(rf.logs) == 0 || args.lastLogTerm >= rf.logs[len(rf.logs) - 1].term || 
		args.lastLogIndex >= len(rf.logs) - 1) {
		reply.term = reply_term
		reply.voteGranted = true
	} else {
		reply.term = reply_term
		reply.voteGranted = false
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply_term := args.term
	if rf.currentTerm > args.term {
		reply_term = rf.currentTerm
	} else if rf.currentTerm < args.term{
		rf.state = 0
		rf.currentTerm = args.term
	}
	reply.term = reply_term
	if rf.currentTerm > args.term {
		reply.success = false
	} else {
		rf.appendEntryTimeout.Reset(RaftAppendEntryTimeout)
		if args.prevLogIndex >= len(rf.logs) || args.prevLogTerm != rf.logs[args.prevLogIndex].term {
			reply.success = false
		} else {
			reply.success = true
			//TODO: 3, 4, 5
		}
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
	rf.logger.Printf("Before sending request vote, args's candidateId: %v, term: %v.", 
		args.candidateId, args.term)
	rf.logger.Printf("send request vote to peer %v.", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if ok {
		if rf.currentTerm < reply.term {
			rf.state = 0
			rf.currentTerm = reply.term
		}
		if reply.voteGranted {
			rf.voteNum++
		}
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if ok {
		if rf.currentTerm < reply.term {
			rf.state = 0
			rf.currentTerm = reply.term
		}
		//TODO:
	}
	rf.mu.Unlock()
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
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
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
	rf.logger.Println("start election.")
	rf.state = 1 // change to candidate
	rf.currentTerm++ // increment currentTerm
	rf.voteNum = 1 // Vote for self
	rf.electionTimeout.Reset(getRandElectionTimeout()) // Reset election Timer
	rf.mu.Unlock()
	// Make args of RequestVoteRPC
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, rf *Raft) {
			rf.logger.Println(i)
			args := RequestVoteArgs{}
			args.term = rf.currentTerm
			args.candidateId = rf.me
			args.lastLogIndex = len(rf.logs) - 1 //FIXME:
			if len(rf.logs) > 0 {
				args.lastLogTerm = rf.logs[len(rf.logs) - 1].term // FIXME:
			}
			var reply RequestVoteReply
			rf.sendRequestVote(i, args, &reply)
		}(i, rf)
	}
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	rf.heartBeatTimer.Reset(RaftHeartBeatLoop)
	//TODO:
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, rf *Raft) {
			args := AppendEntriesArgs{}
			args.term = rf.currentTerm
			args.leaderId = rf.me
			args.prevLogIndex = len(rf.logs) - 1 //FIXME:
			if len(rf.logs) > 0 {
				args.prevLogTerm = rf.logs[len(rf.logs) - 1].term //FIXME:
			}
			args.leaderCommit = rf.commitIndex
			var reply AppendEntriesReply
			rf.sendAppendEntries(i, args, &reply)
		}(i, rf)
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.state = 2
	//TODO:
	rf.mu.Unlock()
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
	rf.me = me
	prefix := fmt.Sprintf("[peer %v] ", rf.me)
	rf.logger = log.New(os.Stdout, prefix, log.Ldate|log.Lmicroseconds|log.Lshortfile)
	rf.peers = peers
	rf.persister = persister
	
	// Your initialization code here.
	rf.applyCh = applyCh
	rf.killCh = make(chan int)
	rf.state = 0
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.appendEntryTimeout = time.NewTimer(RaftAppendEntryTimeout)
	rf.electionTimeout = time.NewTimer(getRandElectionTimeout())
	rf.heartBeatTimer = time.NewTimer(RaftHeartBeatLoop)
	rf.logger.Println("Finish initialization.")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			// In follower state
			if rf.state == 0 {
				select {
				case <- rf.appendEntryTimeout.C:
					rf.startElection()
				case <- rf.killCh:
					return
				}
			}
			// In candidate state
			if rf.state == 1 {
				if rf.voteNum > len(rf.peers) / 2 {
					rf.becomeLeader()
				} else {
					select {
					case <- rf.electionTimeout.C:
						rf.startElection()
					case <- rf.killCh:
						return
					}
				}
			}
			// In leader state
			if rf.state == 2 {
				select {
				case <- rf.heartBeatTimer.C:
					rf.sendHeartBeat()
				case <- rf.killCh:
					return
				}
			}
		}
	}()

	return rf
}
