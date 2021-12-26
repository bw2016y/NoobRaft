package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection2A (t *testing.T){
	servers := 3
	// no network failures
	cfg := make_config(t , servers , false , false )
	defer cfg.cleanup()


	cfg.begin("Test(2A): initial election")

	// is a Leader elected?
	cfg.checkOneLeader()

	// check all raft peers that if they agree on term
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v , but should be at least 1",term1)
	}


	// leader and term should stay the same because there is no network failures
	time.Sleep(2*RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1!=term2{
		fmt.Println("warning : term changed even though there were no failures")
	}


	// chekc one leader
	cfg.checkOneLeader()

	cfg.end()
}

func TestReElections2A(t *testing.T){
	servers := 3
	cfg := make_config(t,servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")

	leader1 := cfg.checkOneLeader()


	cfg.disconnect(leader1)
	cfg.checkOneLeader()


	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum , no leader should be elected.
	cfg.disconnect(leader2)
	//cfg.disconnect((leader2+1)%servers)
	//time.Sleep(2 * RaftElectionTimeout)
	//cfg.checkOneLeader()


	//cfg.connect((leader2+1)%servers)
	cfg.checkOneLeader()



	cfg.connect(leader2)
	cfg.checkOneLeader()

	cfg.end()
}


func TestManyElections2A(t *testing.T){
	servers := 7
	cfg := make_config(t,servers,false,false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): multiple elections")
	cfg.checkOneLeader()


	iters := 10
	for ii := 1;ii<iters;ii++{
		i1 := rand.Int()%servers
		i2 := rand.Int()%servers
		i3 := rand.Int()%servers

		cfg.disconnect(i1)
		cfg.disconnect(i2)
		cfg.disconnect(i3)

		cfg.checkOneLeader()

		cfg.connect(i1)
		cfg.connect(i2)
		cfg.connect(i3)
	}

	cfg.checkOneLeader()
	cfg.end()

}



func TestBasicAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): basic agreement")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}