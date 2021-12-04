package raft

import (
	"fmt"
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