package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"time"

	//"ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// sort from mr.sequential
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
OuterFor:
	for {
		reply := RequestTaskReply{}
		args := RequestTaskArgs{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			// When we failed to process RPC, coordinator finished and break
			// so worker should finish and break as well
			break OuterFor
		}
		TaskType := reply.AssignT.Type
		switch TaskType {
		case Map:
			mapTask(reply.NReduce, &reply.AssignT, mapf)
		case Reduce:
			//PROCESS reduce task
			// find all files corresponding to this reduce task
			var filenames []string
			//list contents of current dir
			files, _ := os.ReadDir(".")
			for _, file := range files {
				filename := file.Name()
				start := "mr-"
				end := fmt.Sprintf("-%d", reply.AssignT.ReduceId)
				//helper function to check if the file has the same partition ID and put in same bucket
				if checkfilename(filename, start, end) {
					filenames = append(filenames, filename)
				}
			}
			// do reduce job for those tasks within the current "filenames"
			//put all the keyvalues into the array of KeyValue
			keyV := shuffle(filenames)
			//call reduceTask to do rest of reduce works
			reduceTask(keyV, &reply.AssignT, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		}
		// make FinishWork RPC call
		fiArgs := &FinishTaskArgs{TaskId: reply.AssignT.TaskId}
		fiReply := &FinishTaskReply{}
		call("Coordinator.FinishTask", &fiArgs, &fiReply)

	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

// helper functions
func reduceTask(keyV []KeyValue, task *Task, reducef func(string, []string) string) error {
	name := fmt.Sprintf("mr-out-%d", task.ReduceId)
	//create temporary file
	ofile, _ := os.Create(name)
	i := 0
	for i < len(keyV) {
		j := i + 1
		for j < len(keyV) && keyV[j].Key == keyV[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, keyV[k].Value)
		}
		output := reducef(keyV[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", keyV[i].Key, output)
		i = j
	}
	ofile.Close()
	return nil
}
func shuffle(filenames []string) []KeyValue {
	var keyV []KeyValue
	for _, namefile := range filenames {
		file := openFile(namefile)
		//from hints
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				/*if err == io.EOF {
					fmt.Println("Reached end of file")

				}*/
				break
				//log.Fatal("Error reading record:", err)
			}
			keyV = append(keyV, kv)
		}
		file.Close()
	}
	//keyV contains all key value pairs, then sort
	// copy from mrsequential.go
	sort.Sort(ByKey(keyV))
	return keyV
}

func mapTask(partitionN int, task *Task, mapf func(string, string) []KeyValue) error {
	filename := task.Filename
	//helper methods from mr.sequential
	//openFile(filename)
	//defer file.Close()
	content := readFile(filename)
	kvdocument := mapf(filename, string(content))
	//the inner slice contains the key value pair while the outer slice is a partition which may
	//contains multiple key value pairs. The NReduce indicates the # of partitions in the entire
	//intermediate
	intermediate := make([][]KeyValue, partitionN)
	tID := task.TaskId
	//id := strconv.Itoa(tID)
	//iterates over keyvalue document in the map task
	for _, kv := range kvdocument {
		//hash(key) mod #reducers == partition key
		//partition#=reducer#
		partition := ihash(kv.Key) % partitionN
		intermediate[partition] = append(intermediate[partition], kv)
	}
	// intermediate[i] --> "mr-MaptaskId-i" while i = reduceTaskId
	// json encode from Hints
	for i := 0; i < partitionN; i++ {
		intermfilename := fmt.Sprintf("mr-%d-%d", tID, i)
		// Create the file directly with the file name
		interfile, err := os.Create(intermfilename)
		if err != nil {
			/*if err == io.EOF {
				fmt.Println("Reached end of file")
				//break
			}
			//log.Fatal("Error reading record:", err)
			*/
			log.Fatalf("cannot create file: %v", err)
		}
		// Use JSON encoder to encode key-value pairs into the file
		//write the contents in json into the intermediate.
		enc := json.NewEncoder(interfile)
		for _, keyvalue := range intermediate[i] {
			enc.Encode(&keyvalue)
		}
		interfile.Close()
	}
	return nil
}

func checkfilename(filename, start, end string) bool {
	return strings.HasPrefix(filename, start) && strings.HasSuffix(filename, end)
}

func openFile(filename string) *os.File {
	file, err := os.Open(filename)
	if err != nil {
		/*if err == io.EOF {
			fmt.Println("Reached end of file")
			//break
		}
		//log.Fatal("Error reading record:", err)
		*/
		log.Fatalf("cannot open %v", filename)
	}
	return file
}
func readFile(filename string) []byte {
	file := openFile(filename)
	content, err := io.ReadAll(file)
	if err != nil {
		/*
			if err == io.EOF {
				fmt.Println("Reached end of file")
				//break
			}
			//log.Fatal("Error reading record:", err)
		*/
		log.Fatalf("cannot read: %v", err)
	}
	file.Close()
	return content
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
/*func CallExample() {

    // declare an argument structure.
    args := ExampleArgs{}

    // fill in the argument(s).
    args.X = 99

    // declare a reply structure.
    reply := ExampleReply{}

    // send the RPC request, wait for the reply.
    call("Coordinator.Example", &args, &reply)

    // reply.Y should be 100.
    fmt.Printf("reply.Y %v\n", reply.Y)
}
*/
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
