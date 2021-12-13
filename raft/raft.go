package raft

// raft service

// Make() // create a raft service

// rf.Start(command interface{}) (index,term, isleader) // start agreement on a new log entry


// rf.GetState() (term,isLeader)  // ask a Raft service server for its current term , and whether is thinks it is leader

// ApplyMsg  // each time a new entry is committed to the log , Raft peer send ApplyMsg to upper service

import (
	"MRaft/chanrpc"
	"MRaft/labgob"
	"bytes"
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
	CommandTerm	 int
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

		if rf.sendAppendEntries(peer,request,response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer , request , response)
			rf.mu.Unlock()
		}

	}
}



func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest{

	firstIndex := rf.getFirstLog().Index
	entries := make([]Entry,len(rf.logs[prevLogIndex +1-firstIndex :]))
	copy(entries , rf.logs[prevLogIndex+1 - firstIndex:])



	return & AppendEntriesRequest{
		Term: 			rf.currentTerm,
		LeaderId:		rf.me,
		PrevLogIndex:	prevLogIndex,
		PrevLogTerm:	rf.logs[prevLogIndex-firstIndex].Term,
		Entries:		entries,
		LeaderCommit:	rf.commitIndex,
	}

}




// a dedicated applier goroutine to guarantee that each log Entry will be push into applyCh exactly once
// ensuring that service's applying entries and raft's committing entries can be parallel
// commit a entry
// apply the entry to upper services
func (rf *Raft) applier(){
	for rf.killed() == false {
		rf.mu.Lock()

		// if there is no need to apply entries , just releader CPU and wait ohter goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex{
			rf.applyCond.Wait()
		}

		firstIndex , commitIndex , lastApplied := rf.getFirstLog().Index , rf.commitIndex , rf.lastApplied


		entries := make([]Entry , commitIndex  - lastApplied)
		copy(entries , rf.logs[lastApplied-firstIndex+1: commitIndex-firstIndex+1])
		rf.mu.Unlock()


		for _ , entry := range entries{
			rf.applyCh <- ApplyMsg{
				CommandValid:	true,
				Command:		entry.Command,
				CommandTerm: 	entry.Term,
				CommandIndex:	entry.Index,
			}
		}

		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v - %v in term %v",rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.lastApplied = Max(rf.lastApplied , commitIndex)
		rf.mu.Unlock()
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm , rf.state == Leader

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(){
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	return w.Bytes()
}

// restore previously persisted state
func (rf *Raft) readPersist(data []byte){

	if data == nil || len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm , votedFor int
	var logs []Entry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("{Node %v} restores persisted state failed", rf.me)
	}


	rf.currentTerm , rf.votedFor , rf.logs = currentTerm , votedFor , logs

	// there will always be at least one entry in rf.logs
	rf.lastApplied , rf.commitIndex = rf.logs[0].Index , rf.logs[0].Index
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludeIndex %v to check whether snapshot is still valid in term %v", rf.me , lastIncludedTerm ,lastIncludedIndex , rf.currentTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me , lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index{
		rf.logs = make([]Entry,1)
	}else {
		rf.logs = shrinkEntriesArray(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs[0].Command = nil
	}

	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.logs[0].Term , rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied , rf.commitIndex = lastIncludedIndex , lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(),snapshot)
	DPrintf("{Node %v}' state is {state %v,term %v,commitIndex %v,lastApplied %v, firstLog %v, lastLog %v} after accepting the snaphost which lastIncludedTerm is %v , lastIncludedIndex is %v",rf.me , rf.state , rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex{
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v",rf.me , index, snapshotIndex, rf.currentTerm )
		return
	}

	rf.logs = shrinkEntriesArray(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

	DPrintf("{Node %v}'s state is {state %v, term %v, commitIndex %v, lastApplied %v, firstLog %v , lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller",rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index , snapshotIndex)

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// todo delete this
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// todo VoteReqArgs
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
// todo delete this
type RequestVoteReply struct {
	// Your data here (2A).
	// todo VoteRepArgs

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(request *RequestVoteRequest, response * RequestVoteResponse) {
	// Your code here (2A, 2B).
	// todo deal with Votes

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is{state %v, term %v , commitIndex %v, lastApplied %v , firstLog %v , lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me , rf.state , rf.currentTerm,  rf.commitIndex , rf.lastApplied , rf.getFirstLog(), rf.getLastLog() , request, response)

	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId){
		response.Term , response.VoteGranted = rf.currentTerm , false
		return
	}

	if request.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm , rf.votedFor = request.Term , -1
	}

	if !rf.isLogUpToDate(request.LastLogTerm , request.LastLogIndex){
		response.Term , response.VoteGranted = rf.currentTerm , false
		return
	}

	rf.votedFor = request.CandidateId
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	response.Term , response.VoteGranted = rf.currentTerm , true
	return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteRequest, reply *RequestVoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, request * AppendEntriesRequest , response * AppendEntriesResponse) bool{
	return rf.peers[server].Call("Raft.AppendEntries",request,response)
}

func (rf *Raft) sendInstallSnapshot(server int, request *InstallSnapshotRequest , response *InstallSnapshotResponse) bool{
	return rf.peers[server].Call("Raft.InstallSnapshot",request,response)
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


	// Your code here (2B).
	// upper service api


	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader{
		return -1 , -1 , false
	}

	newLog := rf.appendNewEntry(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me , newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false)
	return newLog.Index , newLog.Term , true

}

// to append a new Entry to logs
func (rf *Raft) appendNewEntry(command interface{}) Entry{
	lastLog := rf.getLastLog()

	newLog := Entry{lastLog.Index+1 , rf.currentTerm , command}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me] , rf.nextIndex[rf.me] = newLog.Index , newLog.Index + 1

	rf.persist()
	return newLog

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
	return atomic.LoadInt32(&rf.dead) == 1
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
					rf.mu.Lock()

					if rf.state == Leader{
						rf.BroadcastHeartbeat(true)
						rf.heartBeatTimer.Reset(StableHeartbeatTimeout())
					}

					rf.mu.Unlock()
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


// HearBeat

func (rf *Raft) BroadcastHeartbeat (isHeartBeat bool){
	for peer := range rf.peers{
		if peer == rf.me{
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		}else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}

// snapshot stuff

func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest{
	firstLog := rf.getFirstLog()
	return &InstallSnapshotRequest{
		Term:				rf.currentTerm,
		LeaderId:			rf.me,
		LastIncludedIndex:	firstLog.Index,
		LastIncludedTerm:	firstLog.Term,
		Data:				rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) handleInstallSnapshotResponse(peer int,request *InstallSnapshotRequest, response *InstallSnapshotResponse){
	// todo

	if rf.state == Leader && rf.currentTerm == request.Term {
		if response.Term > rf.currentTerm {
			rf.ChangeState(Follower)
			rf.currentTerm , rf.votedFor = response.Term , -1
			rf.persist()
		}else{
			rf.matchIndex[peer] , rf.nextIndex[peer] = request.LastIncludedIndex , request.LastIncludedIndex + 1
		}
	}
	DPrintf("{Node %v}' state is {state %v , term %v , commitIndex %v , lastApplied %v , firstLog %v , lastLog %v} after handling InstallSnapshotResponse %v for InstallSnapshotRequest %v",rf.me , rf.state , rf.currentTerm, rf.commitIndex, rf.lastApplied , rf.getFirstLog(), rf.getLastLog(), response , request)
}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest ,response *InstallSnapshotResponse){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v, commitIndex %v, lastApplied %v, firstLog %v ,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.state , rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request , response)


	response.Term = rf.currentTerm

	if request.Term < rf.currentTerm{
		return
	}

	if request.Term > rf.currentTerm{
		rf.currentTerm , rf.votedFor = request.Term , -1
		rf.persist()
	}

	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	// outdated snapshot
	if request.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid:  true,
			Snapshot:		request.Data,
			SnapshotTerm:	request.LastIncludedTerm,
			SnapshotIndex:	request.LastIncludedIndex,
		}
	}()

}





// Vote Stuff


func (rf *Raft) genRequestVoteRequest() *RequestVoteRequest {
	lastLog := rf.getLastLog()
	return & RequestVoteRequest{
		Term: 				rf.currentTerm,
		CandidateId: 		rf.me,
		LastLogIndex:       lastLog.Index,
		LastLogTerm:		lastLog.Term,
	}
}
// Append

func (rf *Raft) AppendEntries(request * AppendEntriesRequest , response * AppendEntriesResponse) {
	// todo append Entries

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v , term %v , commitIndex %v , lastApplied %v , firstLog %v, lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me , rf.state  , rf.currentTerm , rf.commitIndex , rf.lastApplied , rf.getFirstLog() , rf.getLastLog() ,  request , response)


	if request.Term < rf.currentTerm {
		response.Term , response.Success = rf.currentTerm , false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm , rf.votedFor = request.Term , -1
	}

	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())



	// todo check paper
	if request.PrevLogIndex < rf.getFirstLog().Index {
		response.Term , response.Success = 0 ,false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me , request , request.LeaderId , request.PrevLogIndex , rf.getFirstLog().Index)
		return
	}


	if !rf.matchLog(request.PrevLogTerm , request.PrevLogIndex){
		response.Term , response.Success = rf.currentTerm , false
		lastIndex := rf.getLastLog().Index

		if  lastIndex < request.PrevLogIndex {
			response.ConflictTerm , response.ConflictIndex  = -1 , lastIndex + 1
		} else {
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex-1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm{
				index--
			}
			response.ConflictIndex = index
		}
		return
	}

	firstIndex := rf.getFirstLog().Index
	for index , entry := range request.Entries{
		if entry.Index - firstIndex >= len(rf.logs) || rf.logs[entry.Index - firstIndex].Term != entry.Term{
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex],request.Entries[index:]...))
			break
		}
	}


	rf.advanceCommitIndexForFollower(request.LeaderCommit)

	response.Term , response.Success = rf.currentTerm , true

}

func (rf *Raft) handleAppendEntriesResponse(peer int , request *AppendEntriesRequest , response *AppendEntriesResponse){
	if rf.state == Leader && rf.currentTerm == request.Term{
		if response.Success{
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.advanceCommitIndexForLeader()
		}else{
			if response.Term > rf.currentTerm {
				rf.ChangeState(Follower)
				rf.currentTerm , rf.votedFor = response.Term , -1
				rf.persist()

			}else if response.Term == rf.currentTerm {
				rf.nextIndex[peer] = response.ConflictIndex
				if response.ConflictTerm != -1 {
					firstIndex := rf.getFirstLog().Index
					for i:= request.PrevLogIndex ; i>= firstIndex ;i--{
						if rf.logs[i-firstIndex].Term == response.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'state is {state %v , term %v , commitIndex %v , lastApplied %v , firstLog %v , lastLog %v} after handling AppendEntriesResponse %v for AppendEntriesRequest %v", rf.me , rf.state , rf.currentTerm , rf.commitIndex, rf.lastApplied, rf.getFirstLog() , rf.getLastLog(), response , request )


}

// check Log stuff
// call with a lock
// judge whether log is matched
func (rf *Raft) matchLog(term , index int) bool{
	return index <= rf.getLastLog().Index && rf.logs[index-rf.getFirstLog().Index].Term == term
}

// used to compute and advance commitIndex by matchIndex[]
func (rf *Raft) advanceCommitIndexForLeader(){

	n := len(rf.matchIndex)
	srt := make([]int, n)
	copy(srt, rf.matchIndex)

	insertionSort(srt)

	newCommitIndex := srt[ n - (n/2+1) ]

	if newCommitIndex > rf.commitIndex {
		// only advance commitIndex for current term's log
		if rf.matchLog(rf.currentTerm, newCommitIndex){


			DPrintf("{Node %d} advance commitIndex from %d to %d with matchIndex %v in term %d" , rf.me , rf.commitIndex, newCommitIndex , rf.matchIndex,  rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()


		}else {
			DPrintf("{Node %d} can not advance commitIndex from %d because the term of newCommitIndex %d is not equal to currentTerm %d", rf.me, rf.commitIndex,  newCommitIndex, rf.currentTerm)
		}

	}

}



// used to advance commitIndex by leaderCommit
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int){
	newCommitIndex := Min(leaderCommit , rf.getLastLog().Index)

	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %d} advance commitIndex from %d to %d with leaderCommit %d in term %d", rf.me, rf.commitIndex , newCommitIndex , leaderCommit , rf.currentTerm )
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
}




func shrinkEntriesArray(entries []Entry) []Entry{
	const lenMultiple = 2
	if len(entries) * lenMultiple < cap(entries){
		newEntries := make([]Entry , len(entries))
		copy(newEntries,entries)
		return newEntries
	}
	return entries
}


// called with a lock
func (rf *Raft) isLogUpToDate(term , index int) bool{

	lastLog := rf.getLastLog()

	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)

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
