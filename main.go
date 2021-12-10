package main

import (
	"dududb/model"
	"fmt"
)

func main() {
	e, err := model.NewRecord([]byte(""), []byte("value"))
	if err != nil {
		println(err.Error())
		return
	}
	fmt.Printf("%+v", e)
	println("")
	fmt.Printf("%+v", e.Meta)
	println("")
	buf, _ := e.Pack()
	fmt.Printf("%#v", buf)
	println("")
	r, _ := model.Unpack(buf)
	fmt.Printf("%#v", r)
	println("")
	fmt.Printf("%+v", r.Meta)
	println("")
}
