package chanrpc

import (
	"MRaft/labgob"
	"go/types"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "bytes"
import "reflect"
import "log"

type reqMsg struct {
	endId interface{}
	svcMethod string
	argsType reflect.Type
	args [] byte
	replyCh chan repMsg
}

type repMsg struct {
	flag bool
	reply []byte
}

type Client struct {
	endId interface{}
	ch chan reqMsg
	done chan struct{}
}

// sned RPC request , wati for reply

func(c *Client) Call(svcMeth string , args interface{}, reply interface{}) bool{


	req := reqMsg{}

	req.endId = c.endId
	req.svcMethod = svcMeth
	req.argsType = reflect.TypeOf(args)

	// reply
	req.replyCh = make(chan repMsg)


	reqb := new(bytes.Buffer)
	reqE := labgob.NewEncoder(reqb)

	if err := reqE.Encode(args); err!= nil {
		panic(err)
	}

	req.args = reqb.Bytes()

	// send

	select{
		case c.ch <- req:
		// send
		//
		case <- c.done:
			// entire Network has been destroyed.
			return false
	}

	rep :=  <- req.replyCh
	if rep.flag {
		rB := bytes.NewBuffer(rep.reply)
		rD := labgob.NewDecoder(rB)
		if err := rD.Decode(reply); err!= nil{
			log.Fatalf("reply docode error %v\n",err)
		}
		return true
	}else {
		return false
	}

}

type Network struct{

	mu				sync.Mutex
	reliable 		bool
	longDelays		bool   // pause a long time
	longReordering 	bool

	ends			map[interface{}]*Client // 根据id找到end
	enabled			map[interface{}]bool
	servers 		map[interface{}]*Server // servers
	connections 	map[interface{}]interface{} // client id -> server id


	endCh 			chan reqMsg

	done 			chan struct{} // closed when Network is cleaned up
	count			int32 // total RPC count , for statistics
	bytes 			int64 // total bytes send, for statistics

}

func NewNetwork() * Network{
	net := & Network{}

	net.reliable = true

	net.ends = map[interface{}]*Client{}
	net.enabled = map[interface{}]bool{}
	net.servers = map[interface{}]*Server{}
	net.connections = map[interface{}](interface{}){}

	net.endCh = make(chan reqMsg)
	net.done = make(chan struct{})


	// handle all Client Call

	go func() {
		for{

			select {
				case req := <- net.endCh:

					// statictics
					atomic.AddInt32(&net.count , 1)
					atomic.AddInt64(&net.bytes,int64(len(req.args)))
					go net.processReq(req)
				case <- net.done:
					return
			}

		}

	}()

	return net
}

func (net *Network) Cleanup(){
	close(net.done)
}

func (net *Network) Reliable(flag bool){
	net.mu.Lock()
	defer net.mu.Unlock()

	net.reliable = flag
}

func (net *Network) LongReordering(flag bool){
	net.mu.Lock()
	defer net.mu.Unlock()

	net.longReordering=flag
}

func (net *Network) LongDelays(flag bool){
	net.mu.Lock()
	defer net.mu.Unlock()

	net.longDelays=flag
}

func (net *Network) readEndIdInfo(cid interface{})(enabled bool,serverId interface{},server *Server,reliable bool,longReordering bool){
	net.mu.Lock()
	defer net.mu.Unlock()

	enabled = net.enabled[cid]
	serverId = net.connections[cid]

	if serverId != nil{
		server = net.servers[serverId]
	}

	reliable = net.reliable
	longReordering = net.longReordering

	return
}

func (net *Network) isServerDead(cid interface{}, sid interface{} ,server *Server) bool {
	net.mu.Lock()
	defer net.mu.Unlock()

	if net.enabled[cid]==false || net.servers[sid] != server {
		return true
	}

	return false
}

func (net *Network) processReq(req reqMsg){
	enabled , sid , server , reliable , longReordering := net.readEndIdInfo(req.endId)

	if enabled && sid != nil && server != nil{
		// not reliable
		if reliable == false{
			ms := (rand.Int() %27)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}


		if reliable== false && (rand.Int()%1000 < 100){
			// drop request , return as if timeout
			req.replyCh <- repMsg{false,nil}
			return
		}

		// execute the request
		// ...
		// todo



	}

	return
}

func (net *Network) MakeClient(cid interface{}) *Client{
	net.mu.Lock()
	defer net.mu.Unlock()


	if _,f := net.ends[cid] ; f{
		log.Fatalf("Make Client fail, %v already exists\n",cid)
	}

	c := &Client{}

	c.endId = cid
	c.ch = net.endCh
	c.done = net.done

	net.ends[cid] = c
	// ?
	net.enabled[cid] = false
	// ?
	net.connections[cid] = nil

	return c
}


type Server struct{
	mu			sync.Mutex
	services	map[string]*Service
	// incoming RPCs
	count 		int
}

type Service struct {
	name		string
	rcvr		reflect.Value
	typ 		reflect.Type
	methods		map[string]reflect.Method
}