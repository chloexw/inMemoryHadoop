package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Pair struct {
	key   string
	value int
}

type Worker struct {
	basePort        int
	msgPort         int
	sizePort        int
	replicaPort     int
	editPort        int
	reducerPort     int
	grabPort        int
	fileLocation    map[string]map[int]string
	replicaLocation map[string]map[int]string
}

func main() {
	worker := Worker{
		basePort:        8080,
		msgPort:         9080,
		sizePort:        8180,
		replicaPort:     10000,
		editPort:        11000,
		reducerPort:     12000,
		grabPort:        15000,
		fileLocation:    make(map[string]map[int]string),
		replicaLocation: make(map[string]map[int]string),
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("invalid worker id")
		os.Exit(0)
	}
	worker.reducerPort += id
	go worker.replyStatus(id)
	msgPort := strconv.Itoa(worker.msgPort + id)
	go worker.receiveMsg(id, msgPort, false)
	//receive replica
	replicaPort := strconv.Itoa(worker.replicaPort + id)
	go worker.receiveMsg(id, replicaPort, true)

	editPort := strconv.Itoa(worker.editPort + id)
	//accept any edit request
	go worker.editWorker(editPort)

	go worker.replyGrabRequest(worker.grabPort + id)

	var input string
	fmt.Scanln(&input)
}

func (curr *Worker) listAllFiles() {
	data := make(map[string][]int)
	replica := make(map[string][]int)
	for key, value := range curr.fileLocation {
		sz := len(value)
		ary := make([]int, sz)
		i := 0
		for index, _ := range value {
			ary[i] = index
			i += 1
		}
		data[key] = ary
	}
	fmt.Println("fileLocation: ", data)

	for key, value := range curr.replicaLocation {
		sz := len(value)
		ary := make([]int, sz)
		i := 0
		for index, _ := range value {
			ary[i] = index
			i += 1
		}
		replica[key] = ary
	}
	fmt.Println("replicaLocation: ", replica)
}

func (curr *Worker) replyStatus(workerId int) {
	newPort := strconv.Itoa(workerId + curr.basePort)
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	leaderMsg, err := net.Listen("tcp", remote)
	defer leaderMsg.Close()
	if err != nil {
		fmt.Println("Error when listen: ", remote)
		os.Exit(1)
	}
	for {
		//var res string
		conn, err := leaderMsg.Accept()
		if err != nil {
			fmt.Println("Error when accepting the leader: ", err)
			os.Exit(0)
		}
		fmt.Println("New connection got: ", conn.RemoteAddr())
		res := "Connection ok for " + newPort
		conn.Write([]byte(res))
	}
}

func (curr *Worker) editWorker(newPort string) {
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	leaderMsg, err := net.Listen("tcp", remote)
	defer leaderMsg.Close()
	if err != nil {
		fmt.Println("Error when listen to edit request: ", remote)
		os.Exit(1)
	}
	for {
		conn, err := leaderMsg.Accept()
		if err != nil {
			fmt.Println("Cannot accept connection.")
		}
		tmpBuffer := make([]byte, 1000)
		length, err2 := conn.Read(tmpBuffer)
		if err2 != nil {
			fmt.Println("Error when reading chunks", length)
		}
		command := string(tmpBuffer[:length])
		index := strings.Index(command, "|")
		fmt.Println("Files stored before deletion: ")
		curr.listAllFiles()
		if index == -1 {
			//delete request
			_, ok := curr.fileLocation[command]
			_, ok1 := curr.replicaLocation[command]
			if ok {
				delete(curr.fileLocation, command)
			}
			if ok1 {
				delete(curr.replicaLocation, command)
			}
		} else {
			originalFile := command[:index]
			newFile := command[index+1:]
			if curr.checkValid(newFile) {
				value, ok := curr.fileLocation[originalFile]
				value1, ok1 := curr.replicaLocation[originalFile]
				if ok {
					curr.fileLocation[newFile] = value
					delete(curr.fileLocation, originalFile)
				}
				if ok1 {
					curr.replicaLocation[newFile] = value1
					delete(curr.replicaLocation, originalFile)
				}
			} else {
				fmt.Println("The edit request failed: the new file name is duplicated.")
				continue
			}
		}
		fmt.Println("edit success", newPort)
		res := "success"
		conn.Write([]byte(res))
		fmt.Println("Files stored after deletion: ")
		curr.listAllFiles()
	}
}

func (curr *Worker) checkValid(fileName string) bool {
	//check if the file name is duplicate
	_, ok := curr.fileLocation[fileName]
	_, ok1 := curr.replicaLocation[fileName]
	return !ok && !ok1
}

func (curr *Worker) receiveMsg(workerId int, newPort string, replica bool) {
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	leaderMsg, err := net.Listen("tcp", remote)
	defer leaderMsg.Close()
	if err != nil {
		fmt.Println("Error when listen: ", remote)
		os.Exit(1)
	}
	for {
		conn, err := leaderMsg.Accept()
		tmpBuffer := make([]byte, 1000)
		length, err2 := conn.Read(tmpBuffer)
		if err2 != nil {
			fmt.Println("Error when reading chunks", length)
		}
		//then loop through the length
		chunks, err := strconv.Atoi(string(tmpBuffer[:length]))
		if err != nil {
			fmt.Println("The chunks is invalid")
			return
		}

		conn1, err1 := leaderMsg.Accept()
		if err1 != nil {
			fmt.Println("Cannot accept the leader msg")
			return
		}
		var msgStrBuffer bytes.Buffer
		for i := 0; i < chunks; i++ {
			tmpBuffer := make([]byte, 1000)
			length, err2 := conn1.Read(tmpBuffer)
			if err2 != nil {
				fmt.Println("Error when reading chunks", length)
			}
			currStr := string(tmpBuffer[:length])
			msgStrBuffer.WriteString(currStr)
		}
		curr.recordMsg(workerId, msgStrBuffer, replica)
		msgStrBuffer.Truncate(0)
	}
}

func (curr *Worker) recordMsg(workerId int, msgStrBuffer bytes.Buffer, replica bool) {
	//get the file name
	index0 := strings.Index(msgStrBuffer.String(), "#")
	pathName := msgStrBuffer.String()[:index0]
	//get the chunk id
	index := strings.Index(msgStrBuffer.String(), "|")
	chunkidStr := msgStrBuffer.String()[index0+1 : index]
	chunkId, err := strconv.Atoi(chunkidStr)
	if err != nil {
		fmt.Println("invalid chunk id", workerId)
		return
	}
	chunkData := msgStrBuffer.String()[index+1:]
	var fileName string
	if replica {
		if _, ok := curr.replicaLocation[pathName]; !ok {
			curr.replicaLocation[pathName] = make(map[int]string)
		}
		//record into replica map: path => (chunkId -> chunkData)
		curr.replicaLocation[pathName][chunkId] = chunkData
		//record for test
		fileName = pathName + "_replica_" + strconv.Itoa(workerId) + "_" + strconv.Itoa(chunkId)
	} else {
		if _, ok1 := curr.fileLocation[pathName]; !ok1 {
			curr.fileLocation[pathName] = make(map[int]string)
		}
		//record into the map: path => (chunkId -> chunkData)
		curr.fileLocation[pathName][chunkId] = chunkData
		//record for test
		fileName = pathName + "data_" + strconv.Itoa(workerId) + "_" + strconv.Itoa(chunkId)
	}

	newFile, createError := os.Create(fileName)
	if createError != nil {
		fmt.Println("Failed to create file ", fileName)
		return
	} else {
		fmt.Println("Create file ", fileName)
	}
	buffer := []byte(chunkData)
	//send to mapper
	newFile.Write(buffer[:])
	curr.mapInputFiles(chunkData)
	return
}

func (curr *Worker) mapInputFiles(input string) {
	dict_array := strings.Split(input, " ")
	pairs := make([]Pair, len(dict_array))
	for _, word := range dict_array {
		pairs = append(pairs, Pair{word, 1})
	}
	//encode to byte array
	result, err := json.Marshal(pairs)
	if err != nil {
		fmt.Println("Encoding error")
	}
	curr.sendReducer(result, len(dict_array))
}

func (curr *Worker) sendReducer(array []byte, number int) {
	size := len(array)
	sizestr := strconv.Itoa(size) + "|" + strconv.Itoa(number)
	//send size msg
	curr.sendReducerMsg([]byte(sizestr))
	//send the file data
	curr.sendReducerMsg(array)
}

func (curr *Worker) sendReducerMsg(array []byte) {
	var newPort = strconv.Itoa(curr.reducerPort)
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	for {
		con, err := net.Dial("tcp", remote)
		if err != nil {
			fmt.Println("cannot connect to reducer port", newPort)
			//then continue to try
		}
		defer con.Close()
		//send mapper data
		fmt.Println("sending reducer", len(array))
		in, err := con.Write(array)
		if err != nil {
			fmt.Println("Error when sending the mapper data to reducer", in)
			return
		}
		return
	}
}

func (curr *Worker) replyGrabRequest(port int) {
	fmt.Println("Grab:", port)
	newPort := strconv.Itoa(port)
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	leaderMsg, err := net.Listen("tcp", remote)
	defer leaderMsg.Close()
	if err != nil {
		fmt.Println("Error when listen: ", remote)
		os.Exit(1)
	}
	for {
		conn, err := leaderMsg.Accept()
		tmpBuffer := make([]byte, 1024)
		length, err2 := conn.Read(tmpBuffer)
		if err2 != nil {
			fmt.Println("Error when reading chunks when getting grab request", length)
		}
		//then loop through the length
		requestMsg := string(tmpBuffer[:length])
		index := strings.Index(requestMsg, "|")
		chunkId, err := strconv.Atoi(requestMsg[:index])
		if err != nil {
			fmt.Println("The chunkId is invalid")
			return
		}
		fileName := requestMsg[index+1:]

		//get data
		var size int
		var data []byte
		value, ok := curr.fileLocation[fileName][chunkId]
		if !ok {
			value1, ok1 := curr.replicaLocation[fileName][chunkId]
			if !ok1 {
				fmt.Println("Cannot find the data of the requested file", chunkId)
				return
			}
			data = []byte(value1)
		} else {
			data = []byte(value)
		}
		//reply size data first
		size = len(data)
		conn.Write([]byte(strconv.Itoa(size)))

		//send the complete data
		conn1, err1 := leaderMsg.Accept()
		if err1 != nil {
			fmt.Println("Cannot accept the leader msg")
			return
		}
		buffer := make([]byte, 1024)
		length2, err := conn1.Read(buffer)
		if err != nil {
			fmt.Println("Error when accepting the request after sending size info", chunkId)
			return
		}
		msg := string(buffer[:length2])
		fmt.Println(msg)
		//send the data back
		conn1.Write(data)
	}
}
