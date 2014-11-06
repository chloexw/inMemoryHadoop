package main

import (
	//"bytes"
	//"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	//"strings"
)

/*
	//buf := new(bytes.Buffer)

	m := make([]Number, 3)
	m[0] = Number{One: "a", Two: "b"}
	m[1] = Number{One: "c", Two: "d"}
	m[2] = Number{One: "e", Two: "f"}

	b, err := json.Marshal(m)
	fmt.Println(b, err)

	test := make([]Number, 3)
	err = json.Unmarshal(b, &test)
	fmt.Println(test)

	test1 := "   "
	fmt.Println(strings.Split(test1, " "))

	/*

		var number Number
		key := "1"
		number.One = key
		number.Two = "2"

		b, err := json.Marshal(m)
		if err != nil {
			fmt.Printf("Error: %s", err)
			return
		}
		fmt.Println(string(b))

		err = binary.Write(buf, binary.BigEndian, &b)

		if err != nil {
			fmt.Println("binary.Write failed:", err)
		}
		fmt.Println(buf.Bytes())
		res := buf.Bytes()

		test := make(map[string]int)
		err = json.Unmarshal(res, &test)
		fmt.Println(test)

}
*/

func main() {
	hmap := make(map[string]int)
	hmap["a"] = 1
	hmap["b"] = 2
	hmap["c"] = 3
	hmap["d"] = 4

	b, _ := json.Marshal(hmap)
	fmt.Println(b)
	fileName := "hmap.txt"
	newFile, createError := os.Create(fileName)
	if createError != nil {
		fmt.Println("Failed to create file ", fileName)
		return
	} else {
		fmt.Println("Create file ", fileName)
	}
	buffer := []byte(string(b))
	newFile.Write(buffer[:])
}
