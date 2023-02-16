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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	TermIdx int
	LogIdx  int
}

type State int

const (
	Leader State = iota
	Candidate
	Follower
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// for all servers
	currentTerm     int
	state           State
	votedFor        int
	log             []LogEntry
	commitIdx       int
	lastApplied     int
	lastHeard       time.Time
	electionTimeOut time.Duration
	applyCh         chan ApplyMsg

	// for leaders
	nextIdx  []int
	matchIdx []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
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
	Term    int
	Success bool
	Command interface{}
	LogIdx  int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm { // initialize values in the new term
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = args.Term
	if rf.votedFor == -1 { // haven't voted for anyone in the target term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetTimer()
	} else {
		reply.VoteGranted = false // already vote for someone else
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Heartbeat Logic
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// Convert to follower if receive heartbeat from leader with higher term
	rf.state = Follower
	rf.votedFor = -1
	rf.resetTimer()
	rf.currentTerm = args.Term
	reply.Term = args.Term

	prevLogIdx := args.PrevLogIndex
	leaderLogs := args.Entries

	if len(leaderLogs) == 0 { // nothing to append, just heartbeat
		reply.Success = true
		return
	}

	// Reply false if log doesn't contain an entry at PrevLogIndex whose term matches prevLogTerm
	if prevLogIdx >= 0 && rf.log[prevLogIdx].TermIdx != args.PrevLogTerm {
		reply.Success = false
		return
	}
	reply.Success = true
	nextIdx := prevLogIdx + 1

	// Delete the existing entry and all that follow it
	for nextIdx = prevLogIdx + 1; nextIdx < len(rf.log); nextIdx++ {
		if rf.log[nextIdx] != leaderLogs[nextIdx] {
			break
		}
	}
	rf.log = rf.log[:nextIdx]

	// Append any new entries not already in the log
	for i := nextIdx; i < len(leaderLogs); i++ {
		rf.log = append(rf.log, leaderLogs[i])
	}

	latestLog := rf.log[len(rf.log)-1]
	reply.Command = latestLog.Command
	reply.LogIdx = latestLog.LogIdx
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Deal with command request from client
	// Your code here (2B).
	index := len(rf.log)
	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, -1, false
	}

	rf.log = append(rf.log, LogEntry{LogIdx: index, TermIdx: term, Command: command})
	// fmt.Printf("Leader %d, logs %v\n", rf.me, rf.log)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Leader:
			rf.serverAsLeader()
			time.Sleep(time.Duration(120) * time.Millisecond)
		case Candidate:
			rf.serveAsCandidate()
		case Follower:
			rf.serveAsFollower()
		}
	}
}

func (rf *Raft) serverAsLeader() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		rf.mu.Lock()
		commitCh := make(chan AppendEntriesReply, len(rf.peers))
		rf.mu.Unlock()
		rf.sendOutLogEntryRequests(idx, commitCh)
		// rf.waitForLogEntryResponse(idx, commitCh)
	}
}

func (rf *Raft) serveAsFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if time.Since(rf.lastHeard) > rf.electionTimeOut {
		rf.state = Candidate
		return
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
}

func (rf *Raft) serveAsCandidate() {
	// new term, reset timer, vote for self
	rf.mu.Lock()
	lastHeard := rf.lastHeard
	timeout := rf.electionTimeOut
	rf.mu.Unlock()
	if time.Since(lastHeard) > timeout {
		rf.mu.Lock()
		voteCh := make(chan RequestVoteReply, len(rf.peers))
		rf.mu.Unlock()
		rf.sendOutVoteRequests(voteCh)
		rf.waitForVoteResponse(voteCh, timeout)
	}
}

func (rf *Raft) resetTimer() {
	rf.lastHeard = time.Now()
	// timeout is set from 300 ms to 500 ms
	rf.electionTimeOut = time.Duration(300+rand.Intn(200)) * time.Millisecond
}

func (rf *Raft) sendOutLogEntryRequests(peerIdx int, commitCh chan<- AppendEntriesReply) {
	rf.mu.Lock()
	prevLogIdx := rf.matchIdx[peerIdx]
	var prevTerm int
	if prevLogIdx == -1 {
		prevTerm = -1
	} else {
		prevTerm = rf.log[prevLogIdx].TermIdx
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevTerm,
		Entries:      rf.log,
		LeaderCommit: rf.commitIdx,
	}
	rf.mu.Unlock()
	go func(followerIdx int) {
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(followerIdx, &args, &reply)
		if reply.Success && reply.Command != nil {
			commitCh <- reply
		}
	}(peerIdx)
}

func (rf *Raft) waitForLogEntryResponse(peerIdx int, commitCh <-chan AppendEntriesReply) {
	timeout := time.Duration(100 * time.Millisecond)
	logCommitted := 1
	for i := 0; i < len(rf.peers); i++ {
		select {
		case reply := <-commitCh:
			logCommitted++
			if logCommitted > len(rf.peers)/2 {
				fmt.Printf("Reply: %d, %d\n", reply.Command, reply.LogIdx)
				rf.mu.Lock()
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      reply.Command,
					CommandIndex: reply.LogIdx,
				}
				rf.mu.Unlock()
				return
			}
		case <-time.After(timeout):
			return
		}
	}
	// rf.mu.Lock()
	// currentLen := len(args.Entries)
	// rf.matchIdx[followerIdx] = currentLen - 1
	// rf.nextIdx[followerIdx] = currentLen
	// rf.mu.Unlock()
}

func (rf *Raft) sendOutVoteRequests(voteCh chan<- RequestVoteReply) {
	rf.mu.Lock()
	rf.resetTimer()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		CandidateId: rf.me,
		Term:        rf.currentTerm,
	}
	rf.mu.Unlock()
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(peerIdx int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peerIdx, &args, &reply)
			if ok {
				voteCh <- reply
			}
		}(idx)
	}
}

func (rf *Raft) waitForVoteResponse(
	voteCh <-chan RequestVoteReply, timeout time.Duration) {
	voteGranted := 1
	for i := 0; i < len(rf.peers); i++ {
		select {
		case voteResult := <-voteCh:
			if voteResult.VoteGranted == true {
				voteGranted++
			}
			if voteGranted > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.state = Leader // win election
				rf.initializeLeader()
				rf.mu.Unlock()
				return
			}
		case <-time.After(timeout):
			return
		}
	}
}

func (rf *Raft) initializeLeader() {
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIdx = append(rf.matchIdx, -1)
		rf.nextIdx = append(rf.nextIdx, len(rf.log)-1)
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.commitIdx = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.votedFor = -1
	rf.resetTimer()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
