package labgob

import "testing"
import "bytes"

type T1 struct {
	T1int0 int
	T1int1 int
	T1string0 string
	T1String1 string
}

func TestGOB(t *testing.T){
	e0 := errorCount
	w: new(bytes.Buffer)
	Register()
}