package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"cs350/labrpc"
	rand2 "math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type status int

const (
	Follower status = iota
	Leader
	Candidate
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// ApplyMsg as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Raft peer: A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // (RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[])
	dead      int32               // set by Kill()

	// Your Data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	heartBeat    time.Duration
	status       status
	electionTime time.Time
	currentTerm  int
	votedFor     int

	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	applych chan ApplyMsg
}

// GetState return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {

	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.logs)

	// rf.persister.SaveRaftState(w.Bytes())

}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {

	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var currentTerm int
	// var votedFor int
	// var logs []LogEntry
	// if d.Decode(&currentTerm) != nil ||
	//  d.Decode(&votedFor) != nil ||
	//  d.Decode(&logs) != nil {

	// } else {
	//  rf.currentTerm = currentTerm
	//  rf.votedFor = votedFor
	//  rf.logs = logs

	// }
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your Data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your Data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Reply false if term < currentTerm
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()

	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.IsUpdate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId

		rf.setElectionTime()
	}
}
func (rf *Raft) IsUpdate(args *RequestVoteArgs) bool {
	Lastindex := len(rf.logs) - 1
	if Lastindex == args.LastLogTerm {
		return Lastindex <= args.LastLogIndex

	}
	return rf.logs[Lastindex].Term < args.LastLogTerm
}

// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, Votecnt *int) {
	time.Sleep(10 * time.Millisecond)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = reply.Term

		rf.votedFor = -1
		rf.persist()
		return
	}
	if reply.Term < rf.currentTerm {
		return
	}
	if reply.VoteGranted {
		*Votecnt++
	}
	if *Votecnt > len(rf.peers)/2 {
		rf.status = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		lastindex := len(rf.logs)
		for i := range rf.peers {
			rf.nextIndex[i] = lastindex
		}

		go rf.sendAppendEntries()
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
	term, Isleader := rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !Isleader {
		return -1, term, false
	}
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})

	return len(rf.logs) - 1, term, true
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	rf.mu.Lock()
	rf.setElectionTime()
	rf.mu.Unlock()

	for rf.killed() == false {

		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if rf.status == Leader {

			rf.setElectionTime()
			rf.mu.Unlock()

			rf.sendAppendEntries()
			continue
		}
		if time.Now().After(rf.electionTime) {

			go rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
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

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	rf.votedFor = -1

	rf.heartBeat = 60 * time.Millisecond

	rf.status = Follower
	rf.applych = applyCh
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm})

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

func (rf *Raft) setElectionTime() {

	rf.electionTime = time.Now().Add(time.Duration(210+rand2.Intn(210)) * time.Millisecond)
}

func (rf *Raft) startElection() {
	time.Sleep(10 * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setElectionTime()
	rf.status = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	var Vote int = 1
	lastindex := len(rf.logs) - 1
	for i := range rf.peers {
		if i != rf.me {
			reply := RequestVoteReply{}
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastindex,
				LastLogTerm:  rf.logs[lastindex].Term,
			}
			go rf.sendRequestVote(i, &args, &reply, &Vote)
		}
	}
}

func (rf *Raft) sendAppendEntries() {
	time.Sleep(10 * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return
	}
	for i := range rf.peers {

		if i == rf.me {
			continue
		}

		args := appendEntryArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}

		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		entries := rf.logs[rf.nextIndex[i]:]
		args.Entries = make([]LogEntry, len(entries))
		copy(args.Entries, entries)
		go func(rf *Raft, args *appendEntryArgs, i int) {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()

			reply := appendEntryReply{}

			rf.mu.Unlock()
			ok := rf.peers[i].Call("Raft.AppendEntriesRPC", args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.votedFor = -1

				rf.status = Follower

				rf.setElectionTime()
				return
			}
			if reply.Term < rf.currentTerm || rf.status != Leader || rf.currentTerm != args.Term {
				return
			}

			if reply.Success {
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				rf.matchIndex[i] = max(newMatchIndex, rf.matchIndex[i])
				rf.nextIndex[i] = max(rf.nextIndex[i], rf.matchIndex[i]+1)

			} else if reply.ConflictTerm < 0 {
				rf.nextIndex[i] = reply.ConflictIndex
				rf.matchIndex[i] = rf.nextIndex[i] - 1
			} else {
				lastindex := len(rf.logs) - 1
				for ; lastindex >= 0; lastindex-- {
					if rf.logs[lastindex].Term == reply.ConflictTerm {
						break
					}
				}
				if lastindex < 0 {
					rf.nextIndex[i] = reply.ConflictIndex
				} else {
					rf.nextIndex[i] = lastindex + 1
				}
				rf.matchIndex[i] = rf.nextIndex[i] - 1
			}
			for n := rf.commitIndex + 1; n <= len(rf.logs)-1; n++ {

				if rf.logs[n].Term != rf.currentTerm {
					continue
				}
				cnt := 1
				for x := range rf.peers {
					if x != rf.me && rf.matchIndex[x] >= n {
						cnt += 1
					}
				}

				if cnt > len(rf.peers)/2 {
					rf.commitIndex = n
					go rf.applyLogs()
					break
				}
			}

		}(rf, &args, i)

	}
}

type appendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type appendEntryReply struct {
	Term          int
	Index         int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntriesRPC(args *appendEntryArgs, reply *appendEntryReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		reply.Success = false

		return
	}
	if args.Term > rf.currentTerm {

		rf.setElectionTime()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower

	}

	// if rf.status == Candidate {
	//  //If AppendEntries RPC received from new leader: convert to follower
	//  rf.status = Follower
	// }
	reply.Term = rf.currentTerm
	rf.setElectionTime()

	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	lastindex := len(rf.logs) - 1
	if lastindex < args.PrevLogIndex {
		reply.ConflictIndex = lastindex + 1

		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%d 's term does not match with the leader", rf.me)
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != rf.logs[i-1].Term {

				reply.ConflictIndex = i

				break
			}
		}

		return

	}

	nextIndex := args.PrevLogIndex + 1
	conflictIndex := 0

	for i := 0; i < len(args.Entries); i++ {
		if ((len(rf.logs) - 1) < (nextIndex + i)) || rf.logs[nextIndex+i].Term != args.Entries[i].Term {

			conflictIndex = i
			break
		}
	}

	rf.logs = append(rf.logs[:nextIndex+conflictIndex], args.Entries[conflictIndex:]...)
	DPrintf("raft %d appended entries from leader,log length: %d\n", rf.me, len(rf.logs))

	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		lastindex = len(rf.logs) - 1
		if args.LeaderCommit < lastindex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastindex
		}
		DPrintf("%d have commitIndex %d", rf.me, rf.commitIndex)
		go rf.applyLogs()

	}

}

func (rf *Raft) applyLogs() {
	time.Sleep(10 * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applych <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}

}
