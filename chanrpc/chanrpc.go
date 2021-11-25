package chanrpc

import (
	"MRaft/labgob"
	"go/types"
	"math/rand"
	"strings"
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

func (net *Network) AddServer(sid interface{}, server * Server){
	net.mu.Lock()
	defer net.mu.Unlock()

	net.servers[sid] = server
}


func (net *Network) DeleteServer(sid interface{}){
	net.mu.Lock()
	defer net.mu.Unlock()

	// delete
	net.servers[sid] = nil
}

// client connect to a server
func (net *Network) Connect(cid interface{},sid interface{}){
	net.mu.Lock()
	defer net.mu.Unlock()

	net.connections[cid] = sid
}

func (net *Network) Enable(cid interface{},flag bool){
	net.mu.Lock()
	defer net.mu.Unlock()

	net.enabled[cid] = flag
}


// get a server's count of incoming RPC
func (net *Network) GetCount(sid interface{}) int {
	net.mu.Lock()
	defer net.mu.Unlock()

	server := net.servers[sid]

	return server.GetCount()
}


func (net *Network) GetTotalCount() int{
	x := atomic.LoadInt32(&net.count)
	return int(x)
}

func (net *Network) GetTotalBytes() int64{
	x := atomic.LoadInt64(&net.bytes)
	return x
}

// server is a logic server
// both Raft and k/v server can listen to the same rpc endpoint

type Server struct{
	mu			sync.Mutex
	services	map[string]*Service
	// incoming RPCs
	count 		int
}


func MakeServer() *Server{
	server := &Server{}
	server.services = map[string]*Service{}
	return server
}


func (server *Server) AddService(svc *Service){
	server.mu.Lock()
	defer server.mu.Unlock()

	server.services[svc.name] = svc
}

func (server *Server) dispatch(req reqMsg) repMsg{
	server.mu.Lock()

	server.count += 1

	dot := strings.LastIndex(req.svcMethod , ".")
	serviceName := req.svcMethod[:dot]
	methodName := req.svcMethod[dot+1:]

	service,ok := server.services[serviceName]

	server.mu.Unlock()

	if ok {
		return service.dispatch(methodName,req)
	}else{
		choices := []string{}
		for k,_ := range server.services{
			choices = append(choices,k)
		}
		log.Fatalf("Server dispatch Error: unknown service %v in %v.%v; expecting one of %v\n",serviceName,serviceName,methodName,choices)
		return repMsg{false , nil}
	}

}




func (server *Server) GetCount() int{
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.count
}


// Raft Service
// k/v Service
// a single server may have more than one Service

type Service struct {
	name		string
	serVal		reflect.Value
	serType 	reflect.Type
	methods		map[string]reflect.Method
}
