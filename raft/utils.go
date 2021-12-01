package raft

import (
	"MRaft/chanrpc"
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
	connected	[]bool
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
		cfg.saved[i].SaveStateAndSanpshot(raftlog,snapshot)

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
	// todo
}

const SnapShotInterval = 10

// periodically snapshot raft state
func (cfg *config) applierSnap(i int , applyCh chan ApplyMsg){
	// todo
}


func (cfg *config) start1(i int, applier func(int,chan ApplyMsg)){
	//todo
}

// attatch server i to the net
func (cfg *config) connect(i int){

}

// detach server i fron the net
func (cfg *config)disconnect(i int){

}

func (cfg *config) setUnreliable(flag bool){
	cfg.net.Reliable(flag)
}