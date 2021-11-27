package chanrpc

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
)


type JunkArgs struct{
	X int
}

type JunkReply struct {
	X string
}

type JunkServer struct {
	mu sync.Mutex
	log1 [] string
	log2 [] int
}

func (js * JunkServer) Handler1(args string,reply *int){
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log1 = append(js.log1, args)
	*reply, _ = strconv.Atoi(args)
}


func TestEasic(t *testing.T){
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()


}