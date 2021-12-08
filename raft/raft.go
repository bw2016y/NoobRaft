package raft

// raft service

// Make() // create a raft service

// rf.Start(command interface{}) (index,term, isleader) // start agreement on a new log entry


// rf.GetState() (term,isLeader)  // ask a Raft service server for its current term , and whether is thinks it is leader

// ApplyMsg  // each time a new entry is committed to the log , Raft peer send ApplyMsg to upper service

import (
	"MRaft/chanrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)




type RaftState int32

// consts
const (
	Follower RaftState = 0
	Leader RaftState = 1
	Candidate RaftState = 2

	HeartbeatTimeout = 125
	ElectionTimeout = 1000

)


func (state RaftState) String() string{
	switch state{
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	}
	panic(fmt.Sprintf("unexpected RaftState %d",state))
}

type localRand struct {
	rand *rand.Rand
}

func (r *localRand) Intn(n int) int{
	return r.rand.Intn(n)
}

var rrand = &localRand{
	rand : rand.New(rand.NewSource(time.Now().UnixNano())),
}



// Msg sent to the upper service
//
// we can send snapshot instead of command
//
type ApplyMsg struct {
	CommandValid bool
	Command 	 interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot	[]byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft  service will send Entry to Rafr peers
type Entry struct{
	Index 		int
	Term 		int
	Command 	interface{}
}

func StableHeartbeatTimeout() time.Duration{
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration{
	return time.Duration(ElectionTimeout +  rrand.Intn(ElectionTimeout)) * time.Millisecond
}

// todo check Raft paper
type AppendEntriesRequest struct {
	Term 			int
	LeaderId 		int
	PrevLogIndex 	int
	PrevLogTerm		int

	LeaderCommit 	int
	Entries  		[]Entry
}

type AppendEntriesResponse struct {
	Term 			int
	Success			bool
	ConflictIndex	int
	ConflictTerm	int
}


type InstallSnapshotRequest struct {
	Term 				int
	LeaderId			int
	LastIncludedIndex 	int
	LastIncludedTerm 	int
	Data 				[]byte
}

type InstallSnapshotResponse struct {
	Term				int
}

// todo check paper
type RequestVoteRequest struct {
	Term 		 	int
	CandidateId 	int
	LastLogIndex 	int
	LastLogTerm		int
}

type RequestVoteResponse struct {
	Term 			int
	VoteGranted		bool
}

// Raft peer
type Raft struct {

	// Lock to protect shared access to this peer's state
	// this can be set to RWMutex to get  further optimized
	mu 			sync.Mutex
	// RPC client of all peers
	peers 		[]*chanrpc.Client
	// hold this peer's persisted state
	persister 	*Persister
	// this peer's index in peers
	me			int
	// set ky Kill()
	dead		int32


	// todo data here
	state 			RaftState
	applyCh 		chan ApplyMsg
	applyCond 		*sync.Cond // used to wakeup applier goroutine after committing new entries
	replicatorCond 	[]*sync.Cond // used to signal replicator goroutine to batch replicating entries


	// election
	heartBeatTimer  *time.Timer
	electionTimer 	*time.Timer

	// paper Figure 2 // persistent state
	currentTerm 	int
	votedFor 		int
	logs			[]Entry // set a dummy Entry which contains LastSnapshotTerm , LastSnapshotIndex and nil Command


	// volatile state on all server
	commitIndex 	int
	lastApplied		int


	// volatile state on leaders
	nextIndex		[]int
	matchIndex		[]int



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
func Make(peers []*chanrpc.Client, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {



	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	rf.applyCh = applyCh
	rf.replicatorCond = make([]*sync.Cond , len(peers))
	rf.state  = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Entry,1)

	rf.nextIndex = make([]int,len(peers))
	rf.matchIndex = make([]int,len(peers))

	rf.heartBeatTimer = time.NewTimer(StableHeartbeatTimeout())
	rf.electionTimer = time.NewTimer(RandomizedElectionTimeout())




	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)

	lastLog := rf.getLastLog()

	for i :=0 ; i< len(peers) ; i++{
		// todo what is nextIndex
		rf.matchIndex[i] , rf.nextIndex[i] = 0 , lastLog.Index + 1

		if i != rf.me{
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
				go rf.replicator(i)
		}

	}





	// start ticker goroutine to start elections
	go rf.ticker()
	// todo make Raft server

	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()
	// applyCh -> upper service

	// upper service -> raft
	// rf.Start()

	return rf
}


// check replica
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for rf.killed() == false {
		for !rf.needReplicating(peer){
			// releader CPU
			rf.replicatorCond[peer].Wait()
		}
		// replicate one round()
		rf.replicateOneRound(peer)
	}
}

// used by replicator goroutine to judge whether a peer needs replicating
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// only leader maintains the replicas
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	// double check
	if rf.state != Leader {
		rf.mu.Unlock()
		// not leader
		return
	}

	prevLogIndex := rf.nextIndex[peer] -1

	if prevLogIndex < rf.getFirstLog().Index {
		// only sanpshot can catch up
		request := rf.genInstallSnapshotRequest()
		rf.mu.Unlock()

		response := new(InstallSnapshotResponse)

		if rf.sendInstallSnapshot(peer,request,response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer,request,response)
			rf.mu.Unlock()
		}

	} else {
		// just entries can catch up

		request := rf.genAppendEntriesRequest(prevLogIndex)

		rf.mu.Unlock()

		response := new(AppendEntriesResponse)

		if rf.sendAppendEntriesResponse(peer,request,response) {
			rf.mu.Lock()
			rf.handdleAppendEntriesResponse(peer,request,response)
			rf.mu.Unlock()
		}

	}
}



func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest{
	//todo
	return new(AppendEntriesRequest)
}




// a dedicated applier goroutine to guarantee that each log Entry will be push into applyCh exactly once
// ensuring that service's applying entries and raft's committing entries can be parallel
// commit a entry
// apply the entry to upper services
func (rf *Raft) applier(){
	// todo
}

// get the first log Entry

func (rf *Raft) getFirstLog() Entry{
	return rf.logs[0]
}


// get the last log Entry
func (rf *Raft) getLastLog() Entry{
	return rf.logs[len(rf.logs)-1]
}


// return current term and whether this server believes it is the leader
func (rf *Raft) GetState() (int,bool){
	var term int
	var isLeader bool

	// todo code

	return term , isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(){
	// todo
}

// restore previously persisted state
func (rf *Raft) readPersist(data []byte){
	// todo add code
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// todo add code

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// todo make snapshot
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// todo VoteReqArgs
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// todo VoteRepArgs

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// todo deal with Votes
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// todo upper service api

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
	// todo Kill stuff
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
//
//
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// todo election start check

		select {
				case <- rf.electionTimer.C:
					// elect
					rf.mu.Lock()

					rf.ChangeState(Candidate)
					rf.currentTerm += 1
					rf.StartElection()
					rf.electionTimer.Reset(RandomizedElectionTimeout())

					rf.mu.Unlock()



				case <- rf.heartBeatTimer.C:
					// todo
		}
	}
}

// call with Lock
func (rf *Raft) StartElection(){
	 request := rf.genRequestVoteRequest()
	 DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me , request)

	 grantedVotes := 1
	 rf.votedFor = rf.me

	 // todo why persist here?
	 rf.persist()
	 for peer := range rf.peers {
		 if peer == rf.me {
			 continue
		 }
		 go func(peer int){
			 response := new (RequestVoteResponse)
			 if rf.sendRequestVote(peer,request,response){
				 rf.mu.Lock()
				 defer rf.mu.Unlock()
				 DPrintf("{Node  %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, response , peer , request  ,rf.currentTerm )
				 if rf.currentTerm == request.Term && rf.state == Candidate{
					 if response.VoteGranted {
						 grantedVotes += 1
						 if grantedVotes > len(rf.peers)/2{
							 // seccess
							 DPrintf("{Node %v} receives majority votes in term %v", rf.me , rf.currentTerm)
							 rf.ChangeState(Leader)
							 rf.BroadcastHeartbeat(true)
						 }
					 }else if response.Term > rf.currentTerm{
						 DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me , peer , response.Term ,rf.currentTerm)
						 rf.ChangeState(Follower)
						 // didnt vote in that term yet
						 rf.currentTerm , rf.votedFor = response.Term , -1
						 // todo why persist here?
						 rf.persist()

					 }
				 }

			 }
		 }(peer)
	 }

}

func (rf *Raft) ChangeState(to RaftState){
	if rf.state == to {
		return
	}
	DPrintf("Node %d change state from %s to %s in term %d", rf.me , rf.state , to ,  rf.currentTerm)

	rf.state = to
	switch to{
	case Follower:
		rf.heartBeatTimer.Stop()
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case Candidate:
		// todo
	case Leader:
		lastLog := rf.getLastLog()
		for i:=0 ;i<len(rf.peers);i++{

			// reset matchIndex and nextIndex
			rf.matchIndex[i] , rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartBeatTimer.Reset(StableHeartbeatTimeout())
	}
}
