package labgob

import (
	"encoding/gob"
)
import "io"
import "reflect"
import "fmt"
import "sync"
import "unicode"
import "unicode/utf8"

var mu sync.Mutex
var errorCount int
var checked map[reflect.Type]bool


type MyEncoder struct{
	gob * gob.Encoder
}

func NewEncoder(w io.Writer) *MyEncoder {
	enc := &MyEncoder{}
	enc.gob = gob.NewEncoder(w)
	return enc
}


func (enc *MyEncoder) Encode(e interface{}) error{
	checkValue(e)
	return enc.gob.Encode(e)
}

func (enc *MyEncoder) EncodeValue(value reflect.Value) error {
	checkValue(value.Interface())
	return enc.gob.EncodeValue(value)
}

type MyDecoder struct{
	gob *gob.Decoder
}

func NewDecoder(r io.Reader) * MyDecoder{
	dec := & MyDecoder{}
	dec.gob = gob.NewDecoder(r)
	return dec
}


func (dec *MyDecoder) Decode(e interface{}) error{
	checkValue(e)
	checkDefault(e)
	return dec.gob.Decode(e)
}

// ?
func Register(value interface{}){
	checkValue(value)
	gob.Register(value)
}

func RegisterName(name string ,value interface{}){
	checkValue(value)
	gob.RegisterName(name,value)
}

func checkValue(value interface{}){
	checkType(reflect.TypeOf(value))
}

func checkType(t reflect.Type){
	k := t.Kind()

	mu.Lock()

	if checked == nil{
		checked = map[reflect.Type]bool{}
	}

	if checked[t] {
		mu.Unlock()
		return
	}

	checked[t] = true
	mu.Unlock()

	switch k {
	case reflect.Struct:
		for i := 0 ; i<t.NumField() ; i++{
			f := t.Field(i)
			rune,_ := utf8.DecodeRuneInString(f.Name)
			if unicode.IsUpper(rune) == false{
				fmt.Printf("gob error , lower case found!")
				mu.Lock()
				errorCount += 1
				mu.Unlock()
			}
			checkType(f.Type)
		}
		return
	case reflect.Slice , reflect.Array , reflect.Ptr:
		checkType(t.Elem())
		return
	case reflect.Map:
		checkType(t.Elem())
		checkType(t.Key())
		return
	default:
		return
	}
}

func checkDefault(value interface{}){
	if value == nil{
		return
	}
	checkDefaultD(reflect.ValueOf(value) , 1 , "")
}


func checkDefaultD(value reflect.Value , depth int , name string){
	if depth > 3 {
		return
	}
	t := value.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		for i:=0 ; i<t.NumField();i++{
			vv := value.Field(i)
			curn := t.Field(i).Name

			if name != ""{
				curn = name +"." + curn
			}
			checkDefaultD(vv,depth+1,curn)
		}
		return
	case reflect.Ptr:
		if value.IsNil(){
			return
		}
		checkDefaultD(value.Elem(), depth+1 , name)
		return
	case reflect.Bool, reflect.Int , reflect.Int8 , reflect.Int16, reflect.Int32 , reflect.Int64, reflect.Uint , reflect.Uint8 , reflect.Uint16, reflect.Uint32 , reflect.Uint64, reflect.Uintptr , reflect.Float32 , reflect.Float64, reflect.String:
		if reflect.DeepEqual(reflect.Zero(t).Interface(),value.Interface()) == false {
			mu.Lock()
			if errorCount < 1{
				ename := name

				if ename == ""{
					ename = t.Name()
				}

				fmt.Printf("gob : Decoding to a non-default field\n")

			}
			errorCount += 1
			mu.Unlock()
		}

		return
	}
}

