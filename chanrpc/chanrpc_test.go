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

type JunkService struct {
	mu sync.Mutex
	log1 [] string
	log2 [] int
}

func (js * JunkService) Handler1(args string,reply *int){
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log1 = append(js.log1, args)
	*reply, _ = strconv.Atoi(args)
}


func (js * JunkService) Handler2(args int,reply * string){
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log2 = append(js.log2, args)
	*reply = "handler2-" + strconv.Itoa(args)
}


func TestEasic(t *testing.T){
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	e := net.MakeClient("c1")

	js := &JunkService{}
	svc := MakeService(js)

	server := MakeServer()
	server.AddService(svc)
	net.AddServer("server1",server)

	net.Connect("c1","server1")
	net.Enable("c1",true)

	{
  		reply := ""
		e.Call("JunkService.Handler2",111,&reply)
		if reply != "handler2-111"{
			t.Fatalf("wrong reply from handler2")
		}
	}

	{
		reply := 0
		e.Call("JunkService.Handler1","999",&reply)
		if reply != 999{
			t.Fatalf("wrong reply from handler1")
		}
	}



}