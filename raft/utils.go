package raft

import (
	"MRaft/chanrpc"
	"MRaft/labgob"
	"bytes"
	"encoding/base64"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)
import crand "crypto/rand"



func Min(x , y int) int{
	if x>y{
		return y
	}else {
		return x
	}
}

func Max(x, y int) int{
	if x<y{
		return y
	}else{
		return x
	}
}

func insertionSort(arr []int){
	a,b := 0 , len(arr)
	for i:= a+1 ; i<b ; i++{
		for j:= i; j>a && arr[j] < arr[j-1] ; j--{
			// swap
			arr[j] , arr[j-1] = arr[j-1] , arr[j]
		}
	}
}

const Debug = false

func DPrintf(format string , args ... interface{})(n int, err error){
	if Debug {
		log.Printf(format,args...)
	}
	return
}


func randstring(n int) string{
	b := make([]byte,2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64{
	max := big.NewInt(int64(1)<<62)
	bigx , _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type config struct {
	mu	 		sync.Mutex
	t			*testing.T
	net 		*chanrpc.Network

	n	 		int  // raft peers num
	rafts 		[]*Raft
	applyErr	[]string
	connected	[]bool  // raft peers connected state
	saved		[]*Persister
	clientNames [][]string
	logs 		[]map[int]interface{}

	start 		time.Time // time at which make_config() was Called

	t0 			time.Time
	rpcs0		int
	cmds0		int
	bytes0		int64
	maxIndex	int
	maxIndex0	int
}


var ncpu_once sync.Once

func make_config(t *testing.T , n int , unreliable bool ,snapshot bool) *config{
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2{
			fmt.Printf("warning : only one CPU , which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})

	runtime.GOMAXPROCS(4)

	cfg := &config{}

	cfg.t = t
	cfg.net = chanrpc.NewNetwork()
	cfg.n = n
	cfg.applyErr = make([]string,cfg.n)
	cfg.rafts	= make([]*Raft , cfg.n)
	cfg.connected = make([]bool ,cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.clientNames = make([][]string , cfg.n)
	cfg.logs = make([]map[int]interface{} , cfg.n)  // copy of each server's committed entries
	cfg.start = time.Now()


	cfg.setUnreliable(unreliable)

	cfg.net.LongDelays(true)

	applier := cfg.applier

	if snapshot {
		applier = cfg.applierSnap
	}

	// create a full set of Rafts
	for i:=0 ; i<cfg.n ;i++{
		cfg.logs[i] = map[int]interface{}{}
		cfg.start1(i,applier)
	}

	// connect raft peers
	for i:=0 ;i<cfg.n ; i++{
		cfg.connect(i)
	}

	return cfg
}


// shut down a Raft server but save its persistent state
func (cfg *config) crash1(i int){
	cfg.disconnect(i)
	cfg.net.DeleteServer(i)  // disable client connections to the server

	cfg.mu.Lock()
	defer cfg.mu.Unlock()


	//
	// why copy?
	// always pass Make() the last persisted state
	// Copy() will flush the old state
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	rf := cfg.rafts[i]

	if rf != nil {
		// add concurrency
		cfg.mu.Unlock()

		// do it asynchronous
		rf.Kill()

		cfg.mu.Lock()
		cfg.rafts[i] = nil
	}


	// todo ?
	if cfg.saved[i] != nil {
		raftlog := cfg.saved[i].ReadRaftState()
		snapshot := cfg.saved[i].ReadSnapshot()

		cfg.saved[i] = &Persister{}
		cfg.saved[i].SaveStateAndSnapshot(raftlog,snapshot)

	}
}


func (cfg *config) checkLogs(i int, m ApplyMsg)(string ,bool){
	err_msg := ""

	v := m.Command

	for j:=0; j<len(cfg.logs); j++{
		if old , oldok := cfg.logs[j][m.CommandIndex]; oldok && old != v {


			log.Printf("%v: log %v; server %v\n", i, cfg.logs[i], cfg.logs[j])

			err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
				m.CommandIndex, i, m.Command, j, old)
		}
	}
	// ?
	_ , prevok := cfg.logs[i][m.CommandIndex-1]

	cfg.logs[i][m.CommandIndex] = v
	if m.CommandIndex > cfg.maxIndex {
		cfg.maxIndex = m.CommandIndex
	}

	return err_msg,prevok
}

// applier reads message from applyCh and checks that they match the log contents
func (cfg *config) applier(i int, applyCh chan ApplyMsg){

	for m := range applyCh {
		if m.CommandValid == false{
			// not valid
		} else {
			cfg.mu.Lock()
			err_msg , prevok := cfg.checkLogs(i,m)
			cfg.mu.Unlock()

			if m.CommandIndex > 1 && prevok == false {
				err_msg = fmt.Sprintf("server %v apply out of order %v", i , m.CommandIndex)
			}

			if err_msg != "" {
				log.Fatalf("apply error: %v \n", err_msg)
				cfg.applyErr[i] = err_msg


			}
		}
	}

}

const SnapShotInterval = 10

// periodically snapshot raft state
func (cfg *config) applierSnap(i int , applyCh chan ApplyMsg){
	// todo
	lastApplied := 0
	for m:= range applyCh{
		if m.SnapshotValid {
			cfg.mu.Lock()

			if cfg.rafts[i].CondInstallSnapshot(m.SnapshotTerm , m.SnapshotIndex , m.Snapshot){
				cfg.logs[i] = make(map[int]interface{})


				r := bytes.NewBuffer(m.Snapshot)
				d := labgob.NewDecoder(r)
				var v int
				if d.Decode(&v) != nil {
					log.Fatalf("decode error\n")
				}
				cfg.logs[i][m.SnapshotIndex] = v
				lastApplied = m.SnapshotIndex

			}

			cfg.mu.Unlock()
		} else if m.CommandValid && m.CommandIndex > lastApplied {
			cfg.mu.Lock()
			err_msg , prevok := cfg.checkLogs(i,m)
			cfg.mu.Unlock()

			if m.CommandIndex > 1 && prevok == false {
				err_msg = fmt.Sprintf("server %v apply out of order %v" , i , m.CommandIndex)
			}

			if err_msg != ""{
				log.Fatalf("apply error: %v \n",err_msg)
				cfg.applyErr[i] = err_msg
			}


			lastApplied = m.CommandIndex

			if (m.CommandIndex + 1)%SnapShotInterval == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				v := m.Command
				e.Encode(v)

				cfg.rafts[i].Snapshot(m.CommandIndex , w.Bytes())

			}
		} else {
			// Ignore other types of ApplyMsg or old commands.
		}

	}
}

// start Or ReBoot a Raft peer

// if raft service already exists , "kill" it first
// allocate new outgoing port file names, and a new state persister, to isolate previous instance of this server
func (cfg *config) start1(i int, applier func(int,chan ApplyMsg)){
	//todo
	cfg.crash1(i)

	// fresh client set
	// old crashed instance client cant send
	cfg.clientNames[i] = make([]string, cfg.n)

	for j := 0 ; j < cfg.n ; j++ {
		cfg.clientNames[i][j] = randstring(20)
	}

	// a fresh set of Client
	clients := make([]*chanrpc.Client , cfg.n)
	for j := 0 ; j < cfg.n ; j++ {
		// make client
		clients[j] = cfg.net.MakeClient(cfg.clientNames[i][j])

		// connect
		cfg.net.Connect(cfg.clientNames[i][j],j)
	}

	cfg.mu.Lock()

	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	applyCh := make(chan ApplyMsg)

	rf := Make(clients , i , cfg.saved[i] , applyCh)

	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()


	go applier(i , applyCh)

	svc := chanrpc.MakeService(rf)
	server := chanrpc.MakeServer()
	server.AddService(svc)
	cfg.net.AddServer(i,server)

}


func (cfg *config) checkTimeout(){
	// 120s time limit
	if !cfg.t.Failed() && time.Since(cfg.start) > 120 * time.Second{
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup(){
	for i :=0 ; i< len(cfg.rafts);i++{
		if cfg.rafts[i] != nil{
			cfg.rafts[i].Kill()
		}
	}
	cfg.net.Cleanup()

	// check
	cfg.checkTimeout()
}



// attatch server i to the net
func (cfg *config) connect(i int){
	cfg.connected[i] = true

	for j := 0 ; j <cfg.n ; j++ {
		if cfg.connected[j] {
			clientName := cfg.clientNames[i][j]
			cfg.net.Enable(clientName , true)
		}
	}

	for j := 0 ; j <cfg.n ;j++ {
		if cfg.connected[j]{
			clientName := cfg.clientNames[j][i]
			cfg.net.Enable(clientName,true)
		}
	}
}

// detach server i fron the net
func (cfg *config) disconnect(i int){

	cfg.connected[i] = false

	for j := 0; j <cfg.n ;j++{
		if cfg.clientNames[i] != nil {
			clientName := cfg.clientNames[i][j]
			cfg.net.Enable(clientName,false)
		}
	}

	for j := 0 ; j <cfg.n ;j++ {
		if cfg.connected[j]{
			clientName := cfg.clientNames[j][i]
			cfg.net.Enable(clientName,false)
		}
	}

}


func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) rpcTotal() int{
	return cfg.net.GetTotalCount()
}


func (cfg *config) setUnreliable(flag bool){
	cfg.net.Reliable(!flag)
}

func (cfg *config) bytesTotal() int64{
	return cfg.net.GetTotalBytes()
}

func (cfg *config) setlongreordering(longrel bool){
	cfg.net.LongReordering(longrel)
}


// check that there's exactly one leader
func (cfg *config) checkOneLeader() int{
	for iters := 0 ; iters < 10 ; iters++{
		ms := 450 + (rand.Int63()%100)
		time.Sleep(time.Duration(ms)*time.Millisecond)

		leaders := make(map[int][]int)
		for i:=0 ; i<cfg.n;i++{
			if cfg.connected[i]{
				if term , leader := cfg.rafts[i].GetState(); leader{
					leaders[term] = append(leaders[term],i)
				}
			}
		}

		lastTermWithLeader := -1

		for term , leader := range leaders{
			if len(leader) > 1 {
				cfg.t.Fatalf("term %d has %d (>1) leaders", term , len(leader))
			}

			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaders) !=0 {
			return leaders[lastTermWithLeader][0]
		}

	}

	cfg.t.Fatalf("expected one leader, got none")
	return -1
}

// check that everyone agrees on the term
func (cfg *config) checkTerms() int{
	term := -1
	for i:=0 ; i<cfg.n ;i++{
		if cfg.connected[i]{
			xterm , _ := cfg.rafts[i].GetState()
			if term == -1 {
				term = xterm
			}else if term != xterm{
				cfg.t.Fatalf("servers disagree on term")
			}

		}
	}
	return term
}

// check that there is no leader
func (cfg *config) checkNoLeader(){
	for i:=0 ;i <cfg.n; i++{
		if cfg.connected[i] {
			_ , f := cfg.rafts[i].GetState()
			if f {
				cfg.t.Fatalf("expected no leader , but %v claims to be leader",i)
			}
		}
	}
}

// how many servers think a log entry is committed?
func (cfg *config) nCommitted(index int) (int ,interface{}){
	cnt := 0

	var cmd interface{} = nil
	for i:= 0 ;i<cfg.n; i++{
		if cfg.applyErr[i] != ""{
			cfg.t.Fatal(cfg.applyErr[i])
		}

		cfg.mu.Lock()
		ccmd , f := cfg.logs[i][index]
		cfg.mu.Unlock()

		if f{
			if cnt > 0 && ccmd != cmd {
				cfg.t.Fatalf("committed values do not match: index %v , %v , %v\n",index ,cmd ,ccmd)
			}
			cnt += 1
			cmd = ccmd
		}
	}

	return cnt,cmd
}

// wait for at least n servers to commit
// dont wait forever
func (cfg *config) wait(index int , n int ,startTerm int) interface{}{
	to := 10 * time.Millisecond
	for it := 0 ; it < 30 ; it++{
		cnt , _ := cfg.nCommitted(index)
		if cnt >= n {
			break
		}
		time.Sleep(to)

		if to < time.Second {
			//
			to *= 2
		}

		if startTerm > -1 {
			for _ , r := range cfg.rafts{
				if curterm ,_  := r.GetState() ; curterm > startTerm{
					// raft peers has moved on
					// we cant guarentee that raft will agree on entry (index)
					return -1
				}
			}
		}

	}

	cnt , cmd := cfg.nCommitted(index)
	if cnt < n {
		cfg.t.Fatalf("only %d decided for index %d; wanted %d \n", cnt,index,n)
	}

	return cmd
}

// do a complete agreement
// using for loop to find leader
// try to re submit before giving up
// entirely give up after about 10 seconds
//
// if retry ==  true, may submit the command multiple times (in case that a leader fails just after Start())
// if retry ==  false, call raft.Start() only once, in order to simplify
//

func (cfg *config) one(cmd interface{}, expectedServers int , retry bool) int{
	t0 := time.Now()
	starts := 0

	for time.Since(t0).Seconds() < 10{
		// try all the servers , maybe one is the leader

		index := -1
		for si :=0 ; si<cfg.n ; si++{
			starts = (starts+1) %cfg.n

			var rf *Raft
			cfg.mu.Lock()
			if cfg.connected[starts]{
				rf = cfg.rafts[starts]
			}
			cfg.mu.Unlock()
			if rf != nil{
				cindex , _ ,ok := rf.Start(cmd)
				if ok {
					index = cindex
					break
				}
			}

		}


		if index != -1{
			// somebody claimed to be the leader and have submitted our command;
			// wait a while for agreement
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2{
				cnt , cmd1 := cfg.nCommitted(index)
				if cnt > 0 && cnt >= expectedServers {
					// committed
					if cmd1 == cmd{
						// and it was the command that we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}

			if retry == false {
				cfg.t.Fatalf("one(%v) failed to reach agreement",cmd)
			}

		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}

	cfg.t.Fatalf("one(%v) failed to reash agreement", cmd)
	return -1
}


// start a test
// print the Test message
func (cfg *config) begin(description string){
	fmt.Printf("%s ... \n",description)

	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.bytes0 = cfg.bytesTotal()

	cfg.cmds0 = 0
	cfg.maxIndex0 = cfg.maxIndex
}

// end a Test
// and Test go through here with no failure

func (cfg *config) end(){
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		cfg.mu.Lock()
		t := time.Since(cfg.t0).Seconds()

		npeers := cfg.n
		nrpc := cfg.rpcTotal() - cfg.rpcs0
		nbytes := cfg.bytesTotal() - cfg.bytes0
		ncmds := cfg.maxIndex - cfg.maxIndex0

		cfg.mu.Unlock()

		fmt.Printf("----- passed -----")
		fmt.Printf("%4.1f %d %4d %7d %4d \n", t,npeers,nrpc,nbytes,ncmds)
	}
}

// Maximum log size across all raft peers
func (cfg * config) LogSize() int{
	logsize := 0
	for i:=0 ; i<cfg.n; i++{
		n := cfg.saved[i].RaftStateSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}