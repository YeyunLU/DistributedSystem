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
	"math/rand"
	"sync"
	"sync/atomic"

	"log"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

/*------------------- Data Types --------------------*/
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

const (
	heartbeat          = 50   // 50ms
	minElectionTimeout = 500  // 500 ms
	maxElectionTimeout = 1000 // 1000 ms
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
}

/*------------------- Initialization --------------------*/
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
	rf.log = []LogEntry{{LogIdx: 0, TermIdx: 0}} // placeholder to make the index easier
	// so that log idx start at 1
	rf.resetTimer()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.raftMain()

	return rf
}

/*------------------- Main Function --------------------*/
// The raftMain go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) raftMain() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Leader:
			rf.serveAsLeader()
			// heartbeat 50 ms << election timeout 500-1000 ms
			time.Sleep(time.Duration(heartbeat) * time.Millisecond)
		case Candidate:
			rf.serveAsCandidate()
		case Follower:
			rf.serveAsFollower()
		}
	}
}

/*---------------------- Check State ----------------------------*/
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

/*------------------- Serve as Different States --------------------*/
func (rf *Raft) serveAsLeader() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.sendOutLogEntryRequests(idx)
	}
}

func (rf *Raft) serveAsFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if time.Since(rf.lastHeard) > rf.electionTimeOut {
		rf.state = Candidate
		return
	}
}

func (rf *Raft) serveAsCandidate() {
	rf.mu.Lock()
	lastHeard := rf.lastHeard
	timeout := rf.electionTimeOut
	rf.mu.Unlock()
	if time.Since(lastHeard) > timeout {
		voteCh := make(chan RequestVoteReply, len(rf.peers))
		rf.resetTimer()
		rf.sendOutVoteRequests(voteCh)
		rf.collectVoteResults(voteCh, timeout)
	}
}

/*------------------- Requset vote logic --------------------*/
// Send out all vote request
func (rf *Raft) sendOutVoteRequests(voteCh chan<- RequestVoteReply) {
	rf.mu.Lock()
	// new term, reset timer, vote for self
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	lastLogEntry := rf.log[len(rf.log)-1]
	args := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogIndex: lastLogEntry.LogIdx,
		LastLogTerm:  lastLogEntry.TermIdx,
	}
	rf.mu.Unlock()
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(peerIdx int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peerIdx, &args, &reply)
			if ok && reply.VoteGranted {
				voteCh <- reply
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.stepDownToFollower(-1, reply.Term)
				rf.resetTimer()
			}
			rf.mu.Unlock()
		}(idx)
	}
}

// RPC caller
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(-1, args.Term)
		rf.resetTimer()
	}

	reply.Term = args.Term
	candidateLog := LogEntry{LogIdx: args.LastLogIndex, TermIdx: args.LastLogTerm}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		compareTwoLogEntry(rf.log[len(rf.log)-1], candidateLog) < 1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
	}
}

/*------------------- Append entries logic --------------------*/
// Send out log entry request to a single follower
func (rf *Raft) sendOutLogEntryRequests(peerIdx int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIdx := rf.nextIdx[peerIdx] - 1
	prevTerm := rf.log[prevLogIdx].TermIdx
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevTerm,
		Entries:      rf.log[prevLogIdx+1:],
		LeaderCommit: rf.commitIdx,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.peers[peerIdx].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if reply.Term > args.Term {
		rf.stepDownToFollower(-1, reply.Term)
		rf.resetTimer()
	}
	if rf.state == Leader {
		if reply.Success {
			rf.matchIdx[peerIdx] = args.PrevLogIndex + len(args.Entries)
			rf.nextIdx[peerIdx] = rf.matchIdx[peerIdx] + 1
			rf.checkCommits()
		} else if rf.nextIdx[peerIdx] > 1 { // prevent negative idx for prevLogIdx
			rf.nextIdx[peerIdx]--
		}
	}
	rf.mu.Unlock()
}

// RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// Failed because of lower term
	if args.Term < rf.currentTerm {
		return
	}

	rf.resetTimer()

	// Failed because of mismatched log
	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].TermIdx != args.PrevLogTerm {
		return
	}

	reply.Success = true

	// Step down because of leader with higher term
	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(-1, args.Term)
	}

	// Step down if vote for another candidate
	if rf.state == Candidate && args.Term >= rf.currentTerm {
		rf.stepDownToFollower(-1, args.Term)
	}

	rf.checkAndAppendEntries(args, reply)

	// Apply commits if need
	if args.LeaderCommit > rf.commitIdx {
		rf.commitIdx = min(args.LeaderCommit, rf.log[len(rf.log)-1].LogIdx)
		rf.applyCommits()
	}
}

/*------------------- Persist logic --------------------*/
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Persistent items
	// 1. currentTerm
	// 2. votedFor
	// 3. log[]

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	// Your code here (2C).
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logs) != nil {
		log.Fatalf("Failed to decode persistent data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

/*------------------- Snapshot logic --------------------*/
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
	term, isLeader := rf.GetState()

	if !isLeader {
		return 0, 0, false
	}

	// Deal with command request from client
	// Your code here (2B).
	rf.mu.Lock()
	index := rf.nextIdx[rf.me]
	rf.nextIdx[rf.me]++
	rf.matchIdx[rf.me] = index
	rf.log = append(rf.log, LogEntry{LogIdx: index, TermIdx: term, Command: command})
	rf.persist()
	rf.mu.Unlock()
	rf.serveAsLeader()
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

/*------------------- Other Helper Functions ------------------*/
func (rf *Raft) resetTimer() {
	rf.lastHeard = time.Now()
	// timeout is set from 500 ms to 1000 ms
	timeoutRange := maxElectionTimeout - minElectionTimeout
	rf.electionTimeOut = time.Duration(minElectionTimeout+rand.Intn(timeoutRange)) * time.Millisecond
}

func (rf *Raft) checkCommits() {
	for logIdx := len(rf.log) - 1; logIdx > rf.commitIdx; logIdx-- {
		count := 0
		for serverIdx := range rf.peers {
			if rf.matchIdx[serverIdx] >= logIdx {
				count++
				if count > len(rf.peers)/2 {
					rf.commitIdx = logIdx
					rf.applyCommits()
					return
				}
			}
		}
	}
}

// Apply commits helper
func (rf *Raft) applyCommits() {
	// apply commits in target server
	currentApply := rf.lastApplied
	commitIdx := rf.commitIdx
	for currentApply < commitIdx {
		currentApply++
		logEntry := rf.log[currentApply]
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.LogIdx,
		}
	}
	rf.lastApplied = currentApply
}

// Collect vote result helper
func (rf *Raft) collectVoteResults(
	voteCh <-chan RequestVoteReply, timeout time.Duration) {
	voteGranted := 1
	for i := 0; i < len(rf.peers); i++ {
		select {
		case <-voteCh:
			voteGranted++
			if voteGranted > len(rf.peers)/2 { // win election
				rf.mu.Lock()
				rf.state = Leader
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
	rf.matchIdx = make([]int, len(rf.peers))
	rf.nextIdx = make([]int, len(rf.peers))
	lastLogIdx := rf.log[len(rf.log)-1].LogIdx
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIdx[i] = 0
		rf.nextIdx[i] = lastLogIdx + 1
	}
	rf.matchIdx[rf.me] = lastLogIdx
}

func (rf *Raft) stepDownToFollower(votedFor int, updatedTerm int) {
	rf.state = Follower
	rf.votedFor = -1
	rf.matchIdx = nil
	rf.nextIdx = nil
	if updatedTerm > rf.currentTerm {
		rf.currentTerm = updatedTerm
	}
	rf.persist()
}

func (rf *Raft) checkAndAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	leaderLogs := args.Entries
	nextIdx := args.PrevLogIndex + 1
	idx := 0
	// Delete the existing entry and all that follow it
	existingEntries := rf.log[nextIdx:]
	for idx := 0; idx < min(len(leaderLogs), len(existingEntries)); idx++ {
		if existingEntries[idx] != leaderLogs[idx] {
			break
		}
	}
	rf.log = rf.log[:nextIdx+idx]

	// Append any new entries not already in the log
	for i := 0; i < len(leaderLogs); i++ {
		rf.log = append(rf.log, leaderLogs[i])
	}

	rf.persist()
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func compareTwoLogEntry(a, b LogEntry) int {
	if a.TermIdx < b.TermIdx {
		return -1
	} else if a.TermIdx > b.TermIdx {
		return 1
	} else {
		if a.LogIdx < b.LogIdx {
			return -1
		} else if a.LogIdx > b.LogIdx {
			return 1
		} else {
			return 0
		}
	}
}
