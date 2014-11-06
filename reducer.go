package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Pair struct {
	key   string
	value int
}

type Reducer struct {
	reducerPort []int
	//channel to store the mapper data
	dataList chan []Pair
	result   map[string]int
}

func main() {
	var chunkNum int
	chunkNum = 5
	curr := Reducer{}
	curr.reducerPort = make([]int, chunkNum)
	curr.dataList = make(chan []Pair)
	curr.result = make(map[string]int)
	for i := 0; i < chunkNum; i++ {
		curr.reducerPort[i] = 12000 + i
	}

	for _, value := range curr.reducerPort {
		go curr.getMapperData(value)
	}
	go curr.reduceMapper(chunkNum)

	var input string
	fmt.Scanln(&input)
}

func (reducer *Reducer) getMapperData(port int) {
	newPort := strconv.Itoa(port)
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	mapper, err := net.Listen("tcp", remote)
	defer mapper.Close()
	if err != nil {
		fmt.Println("Error when listen to mapper request: ", port)
		return
	}
	for {
		tmpBuffer := make([]byte, 1024)
		conn, err := mapper.Accept()
		if err != nil {
			fmt.Println("cannot accept mapper", err)
			return
		}
		fmt.Println("Connecting mapper..")
		length, err2 := conn.Read(tmpBuffer)
		if err2 != nil {
			fmt.Println("Error when reading mapper length:", err2)
		}
		//then loop through the length
		sizeInfo := string(tmpBuffer[:length])
		index := strings.Index(sizeInfo, "|")
		size, err := strconv.Atoi(sizeInfo[:index])
		number, err1 := strconv.Atoi(sizeInfo[index+1:])
		if err != nil {
			fmt.Println("The mapper size is invalid")
			return
		}

		if err1 != nil {
			fmt.Println("The mapper number is invalid")
			return
		}

		//get the mapper data
		chunkSize := 1024
		conn1, err1 := mapper.Accept()
		if err1 != nil {
			fmt.Println("Cannot accept the mapper data msg")
			return
		}
		chunksRound := float64(size) / float64(chunkSize)
		chunks := int(math.Ceil(chunksRound))
		var msgStrBuffer bytes.Buffer
		fmt.Println("chunks ", chunks)
		for i := 0; i < chunks; i++ {
			tmpBuffer := make([]byte, chunkSize)
			length, err2 := conn1.Read(tmpBuffer)
			if err2 != nil {
				fmt.Println("Error when reading mapper chunks", i)
			}
			msgStrBuffer.Write(tmpBuffer[:length])
		}
		fmt.Println("msgStrBuffer", msgStrBuffer.Len())
		reducer.decodeMapper(msgStrBuffer, number)
	}
}

func (reducer *Reducer) decodeMapper(mapperMsg bytes.Buffer, number int) {
	//decode the data and insert it into the channel
	array := mapperMsg.Bytes()
	result := make([]Pair, number)
	err := json.Unmarshal(array, &result)
	fmt.Println("decode err", err)
	fmt.Println("here")
	reducer.dataList <- result
	fmt.Println("here1")
	fmt.Println(" decodeMapper len", len(reducer.dataList))
}

func (reducer *Reducer) reduceMapper(chunkNum int) {
	for {
		//generate the final map
		size := len(reducer.dataList)
		//fmt.Println("Reduce mapper ", size)
		if size == chunkNum {
			for i := 0; i < size; i++ {
				subData := <-reducer.dataList
				reducer.reduceSubmap(subData)
			}
			return
		}
		time.Sleep(time.Second * 5)
	}
}

func (reducer *Reducer) reduceSubmap(data []Pair) {
	for _, elem := range data {
		_, ok := reducer.result[elem.key]
		if ok {
			reducer.result[elem.key] += elem.value
		} else {
			reducer.result[elem.key] = elem.value
		}
	}
	reducer.writeToFile()
}

func (reducer *Reducer) writeToFile() {
	fileName := "mapreduce.txt"
	newFile, createError := os.Create(fileName)
	if createError != nil {
		fmt.Println("Failed to create file ", fileName)
		return
	} else {
		fmt.Println("Create file ", fileName)
	}
	buffer, err := json.Marshal(reducer.result)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Write file!")
	newFile.Write(buffer[:])
}
