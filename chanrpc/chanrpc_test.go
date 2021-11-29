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

func (js *JunkService) Handler4(args *JunkArgs, reply *JunkReply){
	reply.X = "pointer"
}

func (js *JunkService) Handler5(args JunkArgs, reply *JunkReply){
	reply.X = "no pointer"
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

func TestTypes(t *testing.T){
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	c := net.MakeClient("c1")

	js := &JunkService{}
	svc := MakeService(js)

	server := MakeServer()

	server.AddService(svc)

	net.AddServer("s1",server)

	net.Connect("c1","s1")

	// net.Enable("c1",true)

	{
		var args JunkArgs
		var reply JunkReply

		c.Call("JunkService.Handler4",&args,&reply)
		//if reply.X != "pointer"{
		//	t.Fatalf("wrong reply from Handler4")
		//}

		if reply.X != "" {
			t.Fatalf("unexpected reply from Handler4")
		}
	}

	net.Enable("c1",true)

	{
		var args JunkArgs
		var reply JunkReply

		c.Call("JunkService.Handler5",args,&reply)
		if reply.X != "no pointer" {
			t.Fatalf("wrong reply from Handler5")
		}

	}

	cnt := net.GetTotalCount()
	if cnt != 2{
		t.Fatalf("wrong GetTotalCount()")
	}
}