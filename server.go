package main

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var chunkSize int64 = 6627
var REPLICA_FACTOR = 3

type Leader struct {
	ports           []int  //the base worker port
	msgPorts        []int  //the worker port for msg
	portState       []bool //the port state list
	replicaPorts    []int
	editPorts       []int
	fileGrabingPort []int
	//the file slice map to worker: chunk id => worker
	//file name => array(chunkid => worker id)
	workerMsgMap map[string][]int
	//file name => map(chunkid => worker id array)
	replicaMap map[string]map[int][]int
}

func main() {
	leader := new(Leader)
	//base ports to check worker status
	paths := os.Args[1]
	//file list
	p_array := strings.Split(paths, ",")
	chunkNum := 5
	leader.initailize(chunkNum)
	for _, path := range p_array {
		file, readError := os.Open(path)
		if readError != nil {
			fmt.Println("Cannot read file")
			return
		}
		defer file.Close()
		//set the state ping
		leader.splitFiles(file, chunkNum, path)
	}
	for index, value := range leader.ports {
		go leader.checkWorkerStatus(value, index)
	}
	fmt.Println(leader.workerMsgMap)
	//leader.getFile("ps.txt")
	//fmt.Println("List all files: ", leader.listStoredFiles())
	//fmt.Println("replica", leader.replicaMap)
	//leader.editFile("ps.txt|ps2.txt", false)

	//fmt.Println("List all files after deletion: ", leader.listStoredFiles())
	//fmt.Println("Replica after deletion", leader.replicaMap)

	//test get function

	var input string
	fmt.Scanln(&input)
}

func (manager *Leader) getFile(fileName string) {
	if !manager.checkValid(fileName) {
		fmt.Println("The file doesn't exist in system.")
		return
	}
	var fileBuffer bytes.Buffer
	chunks := len(manager.workerMsgMap[fileName])
	for i := 0; i < chunks; i++ {
		port := manager.getPortIdFileGrab(i, fileName)
		if port < 0 {
			return
		}
		tmp := manager.getDataFromWorker(port, i, fileName)
		fileBuffer.Write(tmp)
	}
	//convert to file
	receivedFile, err := os.Create("receivedFile.txt")
	if err != nil {
		fmt.Println("Cannot create received file.")
		return
	}
	defer receivedFile.Close()
	receivedFile.Write(fileBuffer.Bytes())
	fmt.Println("Write file successfully!")
}

func (manager *Leader) getPortIdFileGrab(chunkId int, fileName string) int {
	//check the msg group
	var result int
	id := manager.workerMsgMap[fileName][chunkId]
	if manager.portState[id] {
		result = id
	} else {
		//go to the replica map
		fmt.Println("Search the replica group for " + strconv.Itoa(chunkId) + ", " + fileName)
		var re_id int
		re_id = -1
		list := manager.replicaMap[fileName][chunkId]
		for _, value := range list {
			if manager.portState[value] {
				re_id = value
			}
		}
		if re_id == -1 {
			fmt.Println("Cannot find a working port when getting files")
			return -1
		}
		result = re_id
	}
	return manager.fileGrabingPort[result]
}

func (manager *Leader) getDataFromWorker(port int, chunkId int, fileName string) []byte {
	size := manager.getSizeDataFromWorker(port, chunkId, fileName)
	fmt.Println("size "+strconv.Itoa(chunkId), size)
	var newPort = strconv.Itoa(port)
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	for {
		con, err := net.Dial("tcp", remote)
		if err != nil {
			fmt.Println("cannot connect to worker grab port", port)
			return nil
		}
		defer con.Close()
		in, err := con.Write([]byte("data"))
		if err != nil {
			fmt.Println("Error when sending the msg to worker grab port..", in)
			return nil
		}
		chunks := int(math.Ceil(float64(size) / float64(1024)))
		msg := make([]byte, 1024)
		var dataBuffer bytes.Buffer
		fmt.Println("Chunks: ", chunks)
		for j := 0; j < chunks; j++ {
			readLen, errRead := con.Read(msg)
			if errRead != nil {
				fmt.Println("Cannot read msg when getting data from worker", chunkId)
				return nil
			}
			dataBuffer.Write(msg[:readLen])
		}
		return dataBuffer.Bytes()
	}
}

func (manager *Leader) getSizeDataFromWorker(port int, chunkId int, fileName string) int {
	var newPort = strconv.Itoa(port)
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	for {
		con, err := net.Dial("tcp", remote)
		if err != nil {
			fmt.Println("cannot connect to worker grab port when getting size info", port)
			return -1
		}
		defer con.Close()
		//send file name and the chunk id
		var chunkInfo = strconv.Itoa(chunkId) + "|" + fileName
		in, err := con.Write([]byte(chunkInfo))
		if err != nil {
			fmt.Println("Error when sending the msg to worker grab port..when getting size info", in)
			return -1
		}
		msg := make([]byte, 1024)
		length, err := con.Read(msg)
		if err != nil {
			fmt.Println("Error when reading the msg from worker grab port when getting size info", newPort)
			return -1
		}
		var str1 = string(msg[:length])
		size, err := strconv.Atoi(str1)
		if err != nil {
			fmt.Println("The size got back is invalid for " + strconv.Itoa(chunkId))
			return -1
		}
		return size
	}
}

func (manager *Leader) checkValid(fileName string) bool {
	_, ok := manager.workerMsgMap[fileName]
	_, ok1 := manager.replicaMap[fileName]
	return ok && ok1
}

func (manager *Leader) listStoredFiles() []string {
	keys := make([]string, 0, len(manager.workerMsgMap))
	for k := range manager.workerMsgMap {
		keys = append(keys, k)
	}
	return keys
}

func (manager *Leader) getWorkerIdList(fileName string) []int {
	list := manager.workerMsgMap[fileName]
	subMap := manager.replicaMap[fileName]
	for _, value := range subMap {
		prev := append(list[:], value[:]...)
		list = prev
	}
	return list
}

func (manager *Leader) editFile(fileName string, rm bool) {
	var originalFile, newFile string
	if !rm {
		index := strings.Index(fileName, "|")
		newFile = fileName[index+1:]
		originalFile = fileName[:index]
	} else {
		originalFile = fileName
		newFile = fileName
	}
	value, ok := manager.workerMsgMap[originalFile]
	value1, ok1 := manager.replicaMap[originalFile]
	if !ok {
		fmt.Println("The requested file " + fileName + " doesn't exist in worker msg map.")
		return
	}
	if !ok1 {
		fmt.Println("The requested file " + fileName + " doesn't exist in replica msg map.")
		return
	}

	if !rm {
		manager.workerMsgMap[newFile] = value
		manager.replicaMap[newFile] = value1
	}
	list := manager.getWorkerIdList(originalFile)
	flag := make(map[int]bool)
	for _, value := range list {
		_, ok := flag[value]
		if !ok {
			port := manager.editPorts[value]
			manager.editWorkerRecord(fileName, port)
			flag[value] = true
		}
	}
	delete(manager.workerMsgMap, originalFile)
	delete(manager.replicaMap, originalFile)
}

func (manager *Leader) editWorkerRecord(fileName string, port int) bool {
	var newPort = strconv.Itoa(port)
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	for {
		con, err := net.Dial("tcp", remote)
		if err != nil {
			fmt.Println("cannot connect to worker edit port", port)
			return false
		}
		defer con.Close()
		var str = fileName
		var msg = make([]byte, 1024)
		in, err := con.Write([]byte(str))
		if err != nil {
			fmt.Println("Error when sending the msg to worker edit port..", in)
			return false
		}

		_, err2 := con.Read(msg)
		if err2 != nil {
			fmt.Println("Error when reading the msg from worker edit port", newPort)
			return false
		}
		return true
	}
}

func (manager *Leader) initailize(chunkNum int) {
	//how many ports we need to open?
	//initailize the ports list
	manager.ports = make([]int, chunkNum)
	manager.portState = make([]bool, chunkNum)
	manager.msgPorts = make([]int, chunkNum)
	manager.editPorts = make([]int, chunkNum)
	manager.fileGrabingPort = make([]int, chunkNum)

	manager.workerMsgMap = make(map[string][]int)
	manager.replicaPorts = make([]int, chunkNum)
	manager.replicaMap = make(map[string]map[int][]int)
	for i := 0; i < chunkNum; i++ {
		manager.ports[i] = 8080 + i
		manager.portState[i] = true
		//this port is for sending files
		manager.msgPorts[i] = 9080 + i
		manager.replicaPorts[i] = 10000 + i
		manager.editPorts[i] = 11000 + i
		manager.fileGrabingPort[i] = 15000 + i
	}
}

func (manager *Leader) checkWorkerStatus(port int, index int) {
	var newPort = strconv.Itoa(port)
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	for {
		con, err := net.Dial("tcp", remote)
		if err != nil {
			manager.portState[index] = false
			continue
		}
		defer con.Close()
		var str = "Are you alive?"
		var msg = make([]byte, 1024)
		in, err := con.Write([]byte(str))
		if err != nil {
			fmt.Println("Error when sending the msg to worker..", in)
			manager.portState[index] = false
			continue
		}

		length, err := con.Read(msg)
		if err != nil {
			fmt.Println("Error when reading the msg from worker", newPort)
			manager.portState[index] = false
			continue
		}
		var str1 = string(msg[0:length])
		fmt.Println("Received message: ", str1)
		//the worker is alive
		manager.portState[index] = true
		time.Sleep(time.Second * 5)
	}
}

func (manager *Leader) splitFiles(file *os.File, chunkNum int, path string) {
	//initialize
	manager.workerMsgMap[path] = make([]int, chunkNum)
	manager.replicaMap[path] = make(map[int][]int)
	for i := 0; i < chunkNum; i++ {
		buffer := make([]byte, chunkSize)
		//add path and chunk id here
		idStr := path + "#" + strconv.Itoa(i) + "|"
		bufferId := []byte(idStr)

		index, bufferErr := file.Read(buffer)
		if bufferErr != nil {
			fmt.Println("Failed to read buffer file for chunk ", i)
		}
		if index <= 0 {
			return
		}
		//combine them together
		msgBuffer := append(bufferId[:], buffer[:index]...)
		workerId := i
		workerPort := manager.msgPorts[workerId]
		//record the chunkid -> worker id
		manager.workerMsgMap[path][i] = workerId
		//send the file to the worker
		manager.sendFile(msgBuffer[:], workerPort, workerId)
		fmt.Println("Send chunk " + strconv.Itoa(i))
		manager.sendReplica(msgBuffer[:], i, workerId, chunkNum, path)
		fmt.Println("send replica " + strconv.Itoa(i))
	}
}

func (manager *Leader) sendReplica(msg []byte, chunkId int, workerId int, chunkNum int, path string) {
	//get the array of the replica list
	replicaList := manager.assignReplica(chunkNum, chunkId)
	//record chunkid => replica
	manager.replicaMap[path][chunkId] = replicaList
	for _, value := range replicaList {
		//value: worker id
		workerPort := manager.replicaPorts[value]
		go manager.sendFile(msg, workerPort, workerId)
	}
}

func (manager *Leader) assignReplica(chunkNum int, chunkId int) []int {
	array := make([]int, chunkNum)
	slot := 0
	for i := 0; i < chunkNum; i++ {
		if manager.portState[i] && i != chunkId {
			array[slot] = i
			slot += 1
		}
	}
	result := make([]int, REPLICA_FACTOR)
	if slot < 3 {
		fmt.Println("Available replica worker is less than expected")
		return nil
	} else if slot == 3 {
		for j := 0; j < REPLICA_FACTOR; j++ {
			result[j] = array[j]
		}
	} else {
		min := 0
		for j := 0; j < REPLICA_FACTOR; j++ {
			tmp := rand.Intn(slot-min) + min
			result[j] = array[tmp]
			//exchange
			if min != tmp {
				value := array[min]
				array[min] = array[tmp]
				array[tmp] = value
			}
			min += 1
		}
	}
	return result
}

func (manager *Leader) sendMessage(msg []byte, workerPort int, workerId int) {
	var newPort = strconv.Itoa(workerPort)
	var (
		host   = "127.0.0.1"
		remote = host + ":" + newPort
	)
	con, err := net.Dial("tcp", remote)
	if err != nil {
		fmt.Println("CANNOT CONNECT TO", workerId)
	}
	defer con.Close()
	//the worker has rceived the size
	response, err := con.Write(msg)
	if err != nil {
		manager.portState[workerId] = false
		fmt.Println("Error when sending the msg to worker..", response)
		return
	}
	return
}

func (manager *Leader) sendFile(msg []byte, workerPort int, workerId int) {
	//connect to the worker
	size := len(msg)
	chunks := size / 1000
	if size%1000 != 0 {
		chunks += 1
	}
	chunkstr := strconv.Itoa(chunks)
	//send the chunk size
	manager.sendMessage([]byte(chunkstr), workerPort, workerId)
	//send the file chunk data
	manager.sendMessage(msg, workerPort, workerId)
}
