package mr

/* 没有问题 */
import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(task *Task, mapf func(string, string) []KeyValue) {
	fileName := task.FileName
	reduceCnt := task.ReduceCnt
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Error Open File: ", fileName)
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		log.Fatal("Error Read File: ", fileName)
	}
	intermediate := mapf(fileName, string(content))
	wc := make([][]KeyValue, reduceCnt)
	for _, kv := range intermediate {
		hash := ihash(kv.Key) % reduceCnt
		wc[hash] = append(wc[hash], kv)
	}

	for i := 0; i < reduceCnt; i++ {
		tmp_file := fmt.Sprintf("mr-tmp-%d-%d", task.ID, i)
		tf, err := os.CreateTemp("tmp", tmp_file)
		if err != nil {
			log.Fatal("Error in creating template file: ", tmp_file)
		}
		encoder := json.NewEncoder(tf)
		for _, kv := range wc[i] {
			err := encoder.Encode(kv)
			if err != nil {
				log.Fatal("Error Encoding")
			}
		}
		tf.Close()
		os.Rename(tf.Name(), tmp_file)
	}
	callDone(task)
}

func doReduceTask(task *Task, reducef func(string, []string) string) {
	dir, _ := os.Getwd()
	entries, _ := os.ReadDir(dir)

	ans := make(map[string][]string)

	for _, entry := range entries {
		filename := entry.Name()
		if strings.HasPrefix(filename, "mr-tmp") && strings.HasSuffix(filename, strconv.Itoa(task.ID)) {
			file, err := os.Open(filepath.Join(dir, filename))
			if err != nil {
				log.Fatal("Fail To Open File: ", filename)
			}
			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				ans[kv.Key] = append(ans[kv.Key], kv.Value)
			}
			file.Close()
		}
	}

	outFile := "mr-out-" + strconv.Itoa(task.ID)
	tmpFileName := fmt.Sprintf("mr-out-%d", task.ID)
	tmpFile, err := os.CreateTemp("tmp", tmpFileName)
	if err != nil {
		log.Fatal("Create Temp File Error: ", tmpFileName)
	}
	var res []KeyValue
	for k, v := range ans {
		res = append(res, KeyValue{k, reducef(k, v)})
	}
	sort.Sort(ByKey(res))

	for _, kv := range res {
		_, err = fmt.Fprintln(tmpFile, kv.Key+" "+kv.Value)
		if err != nil {
			log.Fatal("Error Append Temp File")
		}
	}
	tmpFile.Close()
	os.Rename(tmpFile.Name(), outFile)
	callDone(task)
}

func poolTask() Task {
	args := Task{}
	reply := Task{}
	call("Coordinator.PoolTask", &args, &reply)
	return reply
}

func callDone(task *Task) {
	call("Coordinator.CallDone", task, task)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	os.MkdirAll("tmp", 0755) //如果存在不操作

	for {
		task := poolTask()
		switch task.Phase {
		case MapPhase:
			log.Println("Map Worker Begin ", task.ID)
			doMapTask(&task, mapf)
			log.Println("Map Worker End", task.ID)
		case ReducePhase:
			log.Println("Reduce Worker Begin", task.ID)
			doReduceTask(&task, reducef)
			log.Println("Reduce Worker End", task.ID)
		default:
			log.Println("Exit Worker")
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	return false
}
