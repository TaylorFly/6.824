package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

/**
* 当前所处时期
 */
type CurrentPhase int

const (
	MapPhase CurrentPhase = iota
	ReducePhase
	ExitPhase
)

// 这里的首字母一定要大写
type Task struct {
	ID        int
	FileName  string
	Phase     CurrentPhase
	ReduceCnt int
}
type State int

const (
	Ready State = iota
	Running
	Finish
)

type TaskState struct {
	State     State
	Task      *Task
	startTime time.Time
}

var tmpLock sync.Mutex

func createTmp() {
	tmpLock.Lock()
	defer tmpLock.Unlock()
	if _, err := os.Stat("tmp"); os.IsNotExist(err) {
		_ = os.Mkdir("tmp", 0755)
	}
}
func createTempFile(dir, pattern string) (*os.File, error) {
	createTmp()
	tmpLock.Lock()
	defer tmpLock.Unlock()
	return os.CreateTemp(dir, pattern)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
