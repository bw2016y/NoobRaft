package chanrpc

import "MRaft/labgob"
import "bytes"
import "reflect"
import "sync"
import "log"
import "strings"
import "math/rand"
import "time"
import "sync/atomic"


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