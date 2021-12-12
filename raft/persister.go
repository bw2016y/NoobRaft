package raft


import "sync"

type Persister struct {
	mu		sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister{
	return &Persister{}
}

func clone(orin []byte) []byte{
	x := make([]byte,len(orin))
	copy(x,orin)
	return x
}

func (ps *Persister) Copy() *Persister{
	ps.mu.Lock()
	defer ps.mu.Unlock()

	rps := MakePersister()
	rps.raftstate = ps.raftstate
	rps.snapshot = ps.snapshot
	return rps
}

func (ps *Persister) SaveRaftState(state []byte){
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.raftstate = clone(state)
}

func (ps *Persister) ReadRaftState() []byte{
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int{
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) SaveStateAndSnapshot(state []byte , snapshot []byte){
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.raftstate = clone(state)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte{
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int{
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}