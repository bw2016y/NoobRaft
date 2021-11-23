package labgob

import (
	"bytes"
	"testing"
)

type T1 struct {
	T1int0 int
	T1int1 int
	T1string0 string
	T1string1 string
}

type T2 struct {
	T2slice []T1
	T2map map[int]*T1
	T2t3 interface{}
}

type T3 struct {
	T3int999 int
}

func TestGOB(t *testing.T){
	e0 := errorCount
	w := new(bytes.Buffer)
	Register(T3{})

	{
		x0 := 0
		x1 := 1

		t1 := T1{}

		t1.T1int1 = 1
		t1.T1string1 = "aa"

		t2 := T2{}

		t2.T2slice = []T1{T1{},t1}
		t2.T2map = map[int]*T1{}
		t2.T2map[9] = &T1{1,2,"a","b"}
		t2.T2t3 = T3{111}

		e := NewEncoder(w)

		e.Encode(x0)
		e.Encode(x1)
		e.Encode(t1)
		e.Encode(t2)


	}

	data := w.Bytes()

	{
		var x0 int
		var x1 int
		var t1 T1
		var t2 T2

		r := bytes.NewBuffer(data)
		d := NewDecoder(r)

		if d.Decode(&x0) != nil || d.Decode(&x1) != nil || d.Decode(&t1)!=nil || d.Decode(&t2)!= nil {
			t.Fatalf("Decode failed")
		}

		if x0 != 0 {
			t.Fatalf("x0 decoding! %v\n",x0)
		}

		if x1 != 1 {
			t.Fatalf("x1 decoding! %v\n",x1)
		}

		if t1.T1int0 !=0 {
			t.Fatalf("wrong t1.T1int0 %v\n", t1.T1int0)
		}

		if t1.T1int1 !=1 {
			t.Fatalf("wrong t1.T1int1 %v\n", t1.T1int1)
		}

		if t1.T1string0 != "" {
			t.Fatalf("wrong t1.T1string0 %v\n", t1.T1string0)
		}

		if t1.T1string1 != "aa" {
			t.Fatalf("wrong t1.T1string1 %v\n", t1.T1string1)
		}
	}



	if errorCount != e0 {
		t.Fatalf("there were errors , but should not have been")
	}
}

type T4 struct{
	Yes int
	no int
}

func TestCapital(t *testing.T){
	e0 := errorCount

	v := []map[*T4]int{}

	w := new(bytes.Buffer)
	e := NewEncoder(w)
	e.Encode(v)

	data := w.Bytes()

	var cv []map[*T4]int
	r := bytes.NewBuffer(data)
	d := NewDecoder(r)
	d.Decode(&cv)

	if errorCount != e0 + 1{
		t.Fatalf("lower-case field not detected!")
	}
}


func TestDefault(t *testing.T){

	e0 := errorCount

	type DD struct {
		X int
	}

	dd1 := DD{}

	w := new(bytes.Buffer)
	e := NewEncoder(w)
	e.Encode(dd1)


	// decode

	data := w.Bytes()
	r := bytes.NewBuffer(data)
	d := NewDecoder(r)

	rdd1 := DD{1}
	d.Decode(&rdd1)

	if errorCount != e0+1 {

		t.Fatalf("failed to warn!!")
	}

}