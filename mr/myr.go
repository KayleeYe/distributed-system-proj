package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"cs350/labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CANDIDATE = iota
	LEADER
	FOLLOWER
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int //the candidateId that received the vote in current term
	state       int32
	heartbeat   chan bool
	votecount   int
	//electionTimeout   *time.Timer
	voted chan bool

	winElect         chan bool
	leaderToFollower chan bool
	candToFollower   chan bool

	//Raft B
	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (4A).
	isleader = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	}
	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	/*w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	*/
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	/*r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
	} else {
		rf.mu.Lock()
		rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, log
		rf.mu.Unlock()
	}
	*/

}

// raft B
func (rf *Raft) IsUpToDate(args *RequestVoteArgs) bool {
	log.Println("get into IsUpToDate func")
	lastIndex := len(rf.logs) - 1
	cur_index, cur_term := lastIndex, rf.logs[lastIndex].Term
	if cur_term == args.LastLogTerm {
		return cur_index <= args.LastLogIndex
	}
	return cur_term < args.LastLogTerm
}

// Raft B
func (rf *Raft) applyLogs() {
	log.Printf("applyLogs can run")
	//time.Sleep(10 * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		//rf.lastApplied = i
	}
	//自改
	rf.lastApplied = rf.commitIndex
	//rf.persist()
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term        int //candidate's term
	CandidateId int //candidate requesting vote
	//below skip for part A
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("RequestVote can run")
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <= rf.currentTerm {
		//rf is the node who received the args
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//rf.persist()
		log.Println("winthin RequestVote, args.Term <= rf.currentTerm")
		return
	} //when args.Term == rf.currentTerm || args.Term>rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		log.Println("winthin RequestVote, args.Term > rf.currentTerm, ready to set follower channel")
		if rf.state == CANDIDATE {
			rf.setCandToFollower()
		} else if rf.state == LEADER {
			rf.setLeaderToFollower()
		}
	}
	log.Printf("Within RequestVote, the comparison on args.term and rf.currentterm ends")
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//so rf are followers
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.IsUpToDate(args) { //Raft B
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		//rf.persist()
		rf.setVoted()
	}
}

type AppendEntriesArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	//raft b
	Entries []LogEntry
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	//raft b
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("AppendEntries can run")
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LeaderTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		reply.Success = false
		//Raft B
		//rf.persist()
		return
	}
	if args.LeaderTerm > rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
		if rf.state == CANDIDATE {
			rf.setCandToFollower()
		} else {
			if rf.state == LEADER {
				rf.setLeaderToFollower()
			}
		}
		//rf.persist()
	}
	//time.Sleep(60 * time.Millisecond)
	rf.setHeartBeat()
	reply.Term = rf.currentTerm
	//Raft B
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	lastIndex := len(rf.logs) - 1
	if lastIndex < args.PrevLogIndex {
		reply.ConflictIndex = lastIndex + 1
		//rf.persist()
		return
	}
	if cur_term := rf.logs[args.PrevLogIndex].Term; cur_term != args.PrevLogTerm {
		reply.ConflictTerm = cur_term
		//speed up
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != cur_term {
				reply.ConflictIndex = i + 1
				//rf.persist()
				break
			}
		}
		return
	}

	i, j := args.PrevLogIndex+1, 0
	for ; i < lastIndex+1 && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.logs[i].Term != args.Entries[j].Term {
			break
		}
	}
	rf.logs = rf.logs[:i]
	args.Entries = args.Entries[j:]
	rf.logs = append(rf.logs, args.Entries...)
	reply.Success = true
	//rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		lastIndex = len(rf.logs) - 1
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		go rf.applyLogs()
	}
}

// Raft B
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("sendAppendEntries can run")
	//time.Sleep(10 * time.Millisecond)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Lock()
	if rf.state != LEADER || rf.currentTerm > reply.Term || rf.currentTerm != args.LeaderTerm {
		return
	}
	if rf.currentTerm < reply.Term {
		//rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.setLeaderToFollower()
		//rf.persist()
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm < 0 {
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		lastIndex := len(rf.logs) - 1
		for ; lastIndex >= 0; lastIndex-- {
			if rf.logs[lastIndex].Term == reply.ConflictTerm {
				break
			}
		}
		if lastIndex < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = lastIndex + 1
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	for n := len(rf.logs) - 1; n >= rf.commitIndex; n-- {
		cnt := 1
		if rf.logs[n].Term == rf.currentTerm {
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					cnt += 1
				}
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = n
			//rf.persist()
			go rf.applyLogs()
			break
		}
	}
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("sendRequestVote can run")
	//time.Sleep(10 * time.Millisecond)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		log.Printf("call RequestVote fail")
		return
	}
	log.Printf("call ReqeustVote ends")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != CANDIDATE || args.Term != rf.currentTerm || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.setCandToFollower()
		//rf.mu.Unlock()
		//rf.persist()
		return
		//rf.state = FOLLOWER
	}
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//加的ß
	/*if reply.Term < rf.currentTerm {
		return
	}
	*/
	if reply.VoteGranted == true {
		rf.votecount += 1
		if rf.votecount > len(rf.peers)/2 {
			rf.setWinElect()
		}
		//rf.persist()
	}
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//term := rf.currentTerm
	if rf.state != LEADER {
		return -1, rf.currentTerm, false
	}
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
	//len := len(rf.logs) - 1
	return len(rf.logs) - 1, rf.currentTerm, true

}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// ticker 里有两个case一个是当rf不是leader（那也要判断其他node是否有leader？）时，开始election
// election 里又分是follower和candidate的case
// 第二个case是当rf是leader，开始broadcast
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch state {
			case FOLLOWER:
				select {
				case <-rf.voted:
				case <-rf.heartbeat:
				case <-time.After(ElectionTimeout()):
					//if appendentries RPC received from new leader: convert to follower
					rf.updateState(CANDIDATE)
					go rf.startElection()
				}
			case CANDIDATE:
				select {
				case <-rf.candToFollower:
					rf.updateState(FOLLOWER)
				case <-rf.heartbeat: //if recieve appendentries from others, convert to follower
					rf.updateState(FOLLOWER)
				case <-time.After(ElectionTimeout()):
					go rf.startElection()
				case <-rf.winElect:
					rf.updateState(LEADER)
					go rf.broadCastAppendEntries()
				}
			case LEADER:
				select {
				case <-rf.leaderToFollower:
					rf.updateState(FOLLOWER)
				case <-time.After(60 * time.Millisecond):
					go rf.broadCastAppendEntries()
				}
			}
		}
	}
}

// RESET ELECTION TIMEOUT：
// 1）the follower election timeoout, become a CANDIDATE, reset the elctiontimeout
// while other followers still counting down their timeout
// 2) other followers reset ElectionTimeout when they vote after receiveing the requestVote
// 3) No need to reset ELECTION TIMEOUT when CANDIDATE become LEADER, but start broadcast appendentries
// need to count HeartBeat
// 4) reset when other followers receive and reply the AppendEntriesRequest/HeartBeat packet
func ElectionTimeout() time.Duration {
	//The election timeout is the amount of time a follower waits
	//until becoming a candidate.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration(r.Intn(300)+200)
}

// Raft B
func (rf *Raft) broadCastAppendEntries() {
	//log.Printf("BroadCastAppendEntries can run")
	//time.Sleep(10 * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs{LeaderTerm: rf.currentTerm, LeaderId: rf.me}
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		//use deep copy
		entries := rf.logs[rf.nextIndex[i]:]
		args.Entries = make([]LogEntry, len(entries))
		copy(args.Entries, entries)
		log.Printf("within BroadCastAppendEntries, sendAppendEntries will run")
		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
		//rf.persist()
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (4A, 4B).
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.heartbeat = make(chan bool)
	rf.voted = make(chan bool)
	rf.winElect = make(chan bool)
	rf.leaderToFollower = make(chan bool)
	rf.candToFollower = make(chan bool)

	//rf.votecount = 0
	//Raft B
	rf.applyCh = applyCh
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm})

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}

func (rf *Raft) startElection() {
	log.Printf("startElection can run")
	//Raft B
	//time.Sleep(10 * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != CANDIDATE {
		return
	}
	rf.votedFor = rf.me
	rf.votecount = 1
	rf.currentTerm += 1
	lastIndex := len(rf.logs) - 1
	//自改
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastIndex, LastLogTerm: rf.logs[lastIndex].Term}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		log.Printf("within startElection, sendRequestVote will run")
		go rf.sendRequestVote(i, &args, &RequestVoteReply{})
		//rf.persist()
	}
}

func (rf *Raft) setHeartBeat() {
	go func() {
		rf.heartbeat <- true
	}()
}
func (rf *Raft) setWinElect() {
	go func() {
		rf.winElect <- true
	}()
	//rf.persist()
}
func (rf *Raft) setLeaderToFollower() {
	go func() {
		rf.leaderToFollower <- true
	}()
	//rf.persist()
}
func (rf *Raft) setVoted() {
	go func() {
		rf.voted <- true
	}()
	//rf.persist()
}
func (rf *Raft) setCandToFollower() {
	go func() {
		rf.candToFollower <- true
	}()
	//rf.persist()
}
func (rf *Raft) updateState(state int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == state {
		return
	}

	old_state := rf.state
	switch state {
	case FOLLOWER:
		rf.state = FOLLOWER
		//rf.votedFor = -1
		//rf.persist()
	case CANDIDATE:
		rf.state = CANDIDATE
		//rf.persist()
	case LEADER:
		rf.state = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		lastIndex := len(rf.logs)
		for i := range rf.peers {
			rf.nextIndex[i] = lastIndex
		}
		//rf.persist()
	default:
		log.Printf("Unknown state %d", state)
		//time.Sleep(10 * time.Millisecond)
		//rf.persist()
	}
	log.Printf("In term %d, machine %d update state from %d to %d", rf.currentTerm, rf.me, old_state, rf.state)
}




package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"cs350/labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CANDIDATE = "candidate"
	LEADER    = "leader"
	FOLLOWER  = "follower"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int //the candidateId that received the vote in current term
	state       string
	heartbeat   chan bool
	votecount   int
	//electionTimeout   *time.Timer
	voted chan bool

	winElect         chan bool
	leaderToFollower chan bool
	candToFollower   chan bool

	//Raft B
	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
}

// Return currentTerm and whether this server
// believes it is the leader.
// 待改
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (4A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	}
	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) IsUpToDate(args *RequestVoteArgs) bool {
	log.Println("get into IsUpToDate func")
	lastIndex := len(rf.logs) - 1
	cur_index, cur_term := lastIndex, rf.logs[lastIndex].Term
	if cur_term == args.LastLogTerm {
		return cur_index <= args.LastLogIndex
	}
	return cur_term < args.LastLogTerm
}

// Raft B
func (rf *Raft) applyLogs() {
	log.Printf("applyLogs can run")
	//time.Sleep(10 * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		//rf.lastApplied = i
	}
	//自改
	rf.lastApplied = rf.commitIndex
	//rf.persist()
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term        int //candidate's term
	CandidateId int //candidate requesting vote
	//below skip for part A
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <= rf.currentTerm {
		//rf is the node who received the args
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else { //when args.Term == rf.currentTerm || args.Term>rf.currentTerm
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = args.Term
			if rf.state == CANDIDATE {
				rf.setCandToFollower()
			} else if rf.state == LEADER {
				rf.setLeaderToFollower()
			}
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//so rf are followers
	if rf.votedFor == -1 && rf.IsUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		go func() {
			rf.voted <- true
		}()

	}
}

type AppendEntriesArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	//raft b
	Entries []LogEntry
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	//raft b
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LeaderTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		//Raft B
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		return
	}
	if args.LeaderTerm > rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
		if rf.state == CANDIDATE {
			rf.setCandToFollower()
		} else {
			if rf.state == LEADER {
				rf.setLeaderToFollower()
			}
		}
	}
	reply.Term = rf.currentTerm
	go func() {
		rf.heartbeat <- true
	}()

	//Raft B
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	lastIndex := len(rf.logs) - 1
	if lastIndex < args.PrevLogIndex {
		reply.ConflictIndex = lastIndex + 1
		//rf.persist()
		return
	}
	if cur_term := rf.logs[args.PrevLogIndex].Term; cur_term != args.PrevLogTerm {
		reply.ConflictTerm = cur_term
		//speed up
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != cur_term {
				reply.ConflictIndex = i + 1
				//rf.persist()
				break
			}
		}
		return
	}

	i, j := args.PrevLogIndex+1, 0
	for ; i < lastIndex+1 && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.logs[i].Term != args.Entries[j].Term {
			break
		}
	}
	rf.logs = rf.logs[:i]
	args.Entries = args.Entries[j:]
	rf.logs = append(rf.logs, args.Entries...)
	reply.Success = true
	//rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		lastIndex = len(rf.logs) - 1
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		go rf.applyLogs()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.setLeaderToFollower()
	}
	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm < 0 {
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		lastIndex := len(rf.logs) - 1
		for ; lastIndex >= 0; lastIndex-- {
			if rf.logs[lastIndex].Term == reply.ConflictTerm {
				break
			}
		}
		if lastIndex < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = lastIndex + 1
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	for n := len(rf.logs) - 1; n >= rf.commitIndex; n-- {
		cnt := 1
		if rf.logs[n].Term == rf.currentTerm {
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					cnt += 1
				}
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = n
			//rf.persist()
			go rf.applyLogs()
			break
		}
	}
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != CANDIDATE || args.Term != rf.currentTerm || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.setCandToFollower()
	}
	if reply.VoteGranted == true {
		rf.votecount += 1
		if rf.votecount > len(rf.peers)/2 {
			rf.setWinElect()
		}
	}
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	if rf.state != LEADER {
		return -1, term, false
	}
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	len := len(rf.logs) - 1
	return len, term, true
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// ticker 里有两个case一个是当rf不是leader（那也要判断其他node是否有leader？）时，开始election
// election 里又分是follower和candidate的case
// 第二个case是当rf是leader，开始broadcast
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case LEADER:
			select {
			case <-rf.leaderToFollower:
				rf.updateState(FOLLOWER)
			case <-time.After(70 * time.Millisecond):
				go rf.broadCastAppendEntries()
			}
		case FOLLOWER:
			select {
			case <-rf.voted:
			case <-rf.heartbeat:
			case <-time.After(ElectionTimeout()):
				//if appendentries RPC received from new leader: convert to follower
				rf.updateState(CANDIDATE)
				go rf.LeaderElection()
			}
		case CANDIDATE:
			select {
			case <-rf.winElect:
				rf.updateState(LEADER)
				//time.Sleep(60 * time.Millisecond)
				go rf.broadCastAppendEntries()
			case <-rf.candToFollower:
				rf.updateState(FOLLOWER)
			case <-rf.heartbeat: //if recieve appendentries from others, convert to follower
				rf.updateState(FOLLOWER)
			case <-time.After(ElectionTimeout()):
				go rf.LeaderElection()
			}
		}
	}
}

// RESET ELECTION TIMEOUT：
// 1）the follower election timeoout, become a CANDIDATE, reset the elctiontimeout
// while other followers still counting down their timeout
// 2) other followers reset ElectionTimeout when they vote after receiveing the requestVote
// 3) No need to reset ELECTION TIMEOUT when CANDIDATE become LEADER, but start broadcast appendentries
// need to count HeartBeat
// 4) reset when other followers receive and reply the AppendEntriesRequest/HeartBeat packet
func ElectionTimeout() time.Duration {
	//The election timeout is the amount of time a follower waits
	//until becoming a candidate.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration(r.Intn(300)+200)
}

/*
func (rf *Raft) HeartBeatTimeout() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration(r.Intn(150)+0)
}
*/

// FIX！！
// 把leaderElection里的broadcast的go func提出来放到这里，首先判断rf是否是leader
//get broadcast from leaderElection out as a individual function, and go it, only for rf.state = LEADER
//if not, just return

/*
func (rf *Raft) broadcastVote(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.sendAppendEntries(server, args, reply)
}
*/

func (rf *Raft) LeaderElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//change on itself
	rf.votedFor = rf.me
	rf.votecount = 1
	rf.currentTerm += 1
	go rf.braodcastRequestVote()
}

func (rf *Raft) braodcastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) broadCastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//args := AppendEntriesArgs{LeaderTerm: rf.currentTerm, LeaderId: rf.me}
	for i := range rf.peers {
		if i != rf.me {
			args := AppendEntriesArgs{LeaderTerm: rf.currentTerm, LeaderId: rf.me}
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			args.LeaderCommit = rf.commitIndex
			//use deep copy
			entries := rf.logs[rf.nextIndex[i]:]
			args.Entries = make([]LogEntry, len(entries))
			copy(args.Entries, entries)
			go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
		}
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (4A, 4B).
	rf.heartbeat = make(chan bool)
	rf.voted = make(chan bool)
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.votecount = 0
	rf.winElect = make(chan bool)
	rf.leaderToFollower = make(chan bool)
	rf.candToFollower = make(chan bool)
	//Raft B
	rf.applyCh = applyCh
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm})
	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}

func (rf *Raft) setHeartBeat() {
	go func() {
		rf.heartbeat <- true
	}()
}
func (rf *Raft) setWinElect() {
	go func() {
		rf.winElect <- true
	}()
	//rf.persist()
}
func (rf *Raft) setLeaderToFollower() {
	go func() {
		rf.leaderToFollower <- true
	}()
	//rf.persist()
}

func (rf *Raft) setCandToFollower() {
	go func() {
		rf.candToFollower <- true
	}()
	//rf.persist()
}
func (rf *Raft) updateState(state string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == state {
		return
	}
	old_state := rf.state
	switch state {
	case FOLLOWER:
		rf.state = FOLLOWER
		rf.votedFor = -1
		//rf.persist()
	case CANDIDATE:
		rf.state = CANDIDATE
		//rf.persist()
	case LEADER:
		rf.state = LEADER
	default:
		log.Printf("Unknown state %s", state)
		//time.Sleep(10 * time.Millisecond)
		//rf.persist()
	}
	log.Printf("In term %d, machine %d update state from %s to %s", rf.currentTerm, rf.me, old_state, rf.state)
}
