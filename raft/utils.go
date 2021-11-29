package raft

import "log"

const Debug = false

func DPrintf(format string , args ... interface{})(n int, err error){
	if Debug {
		log.Printf(format,args...)
	}
	return
}