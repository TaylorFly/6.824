package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

const HeartBeatTerm = -1

type Role int

const (
	Leader = iota
	Candidater
	Follower
)

const HeartBeatTimeout = 120 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int // 当前的任期
	votedFor    int // 投票给谁
	log         []LogEntry

	commitIndex int // 最新的提交的log entry
	lastApplied int // 最后一个应用(持久化)的log entry

	//每次选举之后重新初始化
	nextIndex  []int //发送的下一个log entry的索引
	matchIndex []int // Follower的log与Leader的log同步到第几个

	role     Role //当前的角色
	overtime time.Duration
	timer    *time.Timer //负责心跳

	applyCh chan ApplyMsg
}

type AppendEntryArgs struct {
	Term     int //Leader的Term
	LeaderID int //Leader的ID

	// 让Follower确定append log的位置
	PrevLogIndex int //上一个Log的index
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int // Leader确定多数的Follower接收到Log之后将自己的commitindex+1之后发送，就是这个LeaderCommit
}

type AppendEntryReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 自己的term比较小
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // 这一步也是清除之前的vote
		rf.role = Follower
	}

	if rf.votedFor != -1 {
		reply.VoteGranted = rf.votedFor == args.CandidateID
		reply.Term = rf.currentTerm //
		return
	}

	var vote bool

	// 当前Term更大
	if rf.currentTerm > args.Term {
		vote = false
	} else {
		lastIndex := len(rf.log) - 1

		if lastIndex == -1 {
			vote = true
		} else {
			lastLog := rf.log[lastIndex]
			LastLogTerm := lastLog.Term

			if LastLogTerm < args.LastLogTerm {
				vote = true
			} else if LastLogTerm > args.LastLogTerm {
				vote = false
			} else {
				vote = lastIndex <= args.LastLogIndex
			}
		}
	}

	reply.VoteGranted = vote
	if vote {
		reply.Term = rf.currentTerm
		rf.role = Follower
		rf.votedFor = args.CandidateID
		rf.timer.Reset(rf.overtime)
	} else {
		reply.Term = rf.currentTerm
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return index, term, false
	}
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	index = len(rf.log)
	term = rf.currentTerm
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendAppenEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	reply.Term = rf.currentTerm

	//失败情况1：当前的Term更大
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	// 当前的term更小（等于）Leader的，那么确定是Follower,统一处理一下
	rf.role = Follower
	rf.votedFor = -1
	rf.timer.Reset(rf.overtime)
	rf.currentTerm = args.Term

	// 失败情况二：日志错误
	// 2.1：prev太长, 下次在log的末尾插入
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	// 2.2：日志冲突
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 否则找到第一个term为prev index处的term，
		// 因为一个term只有一个Leader,要错认为全错
		term := rf.log[args.PrevLogIndex].Term
		index := 0
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != term {
				index = i + 1
				break
			}
		}
		reply.ConflictIndex = index
		reply.ConflictTerm = term
		reply.Success = false
		return
	}

	// 空的消息，说明是确认自己Leader身份或者告诉Follower自己的CommitIndex更新完成，让Follower更新一下
	if len(args.Entries) == 0 {
		// Heartbeat: update commitIndex
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
		}
		reply.Success = true
		return
	}

	// prev也是正确的，那么就更新当前的log
	if args.PrevLogIndex == -1 || rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// 如果prev是正确的，或者当前的log为空，直接append
		// 截断并追加
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}

			rf.mu.Lock()
			if rf.role == Leader {
				//心跳
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go func(server int) {
						rf.mu.Lock()

						// 准备提交给该server的log
						entries := make([]LogEntry, 0)

						if rf.nextIndex[server] < len(rf.log) {
							entries = append(entries, rf.log[rf.nextIndex[server]:]...)
						}
						// 上一个log的信息
						prevLogIndex := rf.nextIndex[server] - 1
						prevLogTerm := -1

						if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
							prevLogTerm = rf.log[prevLogIndex].Term
						}
						appendEntryArg := AppendEntryArgs{
							Term:         rf.currentTerm,
							LeaderID:     rf.me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogTerm,
							Entries:      entries,
							LeaderCommit: rf.commitIndex,
						}
						rf.mu.Unlock()
						reply := AppendEntryReply{}
						ok := rf.sendAppenEntries(server, &appendEntryArg, &reply)

						if ok {
							rf.mu.Lock()
							if reply.Success {
								rf.nextIndex[server] = prevLogIndex + 1 + len(entries)
								rf.matchIndex[server] = rf.nextIndex[server] - 1

								// 超过半数的Follower确定了同步的位置才更新Leader的commitIndex
								for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
									if rf.log[n].Term != rf.currentTerm {
										continue
									}
									acceptNum := 1
									for i := 0; i < len(rf.peers); i++ {
										if i != rf.me && rf.matchIndex[i] >= n {
											acceptNum++
										}
									}
									if acceptNum > len(rf.peers)/2 {
										rf.commitIndex = n
										break
									}
								}

							} else {
								if reply.Term > rf.currentTerm {
									rf.role = Follower
									rf.votedFor = -1
									rf.timer.Reset(rf.overtime)
								} else {
									rf.nextIndex[server] = reply.ConflictIndex
								}
							}
							rf.mu.Unlock()
						}
					}(i)
				}
				rf.timer.Reset(HeartBeatTimeout)
			} else {
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.mu.Unlock()
				voteNum := 1
				finish := 1
				var voteMu sync.Mutex
				cond := sync.NewCond(&voteMu)

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					go func(server int) {
						rf.mu.Lock()
						args := RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateID:  rf.me,
							LastLogIndex: len(rf.log) - 1,
							LastLogTerm:  0,
						}
						if len(rf.log) > 0 {
							args.LastLogTerm = rf.log[len(rf.log)-1].Term
						}
						rf.mu.Unlock()
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(server, &args, &reply)
						voteMu.Lock()
						finish++
						voteMu.Unlock()
						if ok {
							rf.mu.Lock()
							if reply.VoteGranted {
								voteMu.Lock()
								voteNum++
								voteMu.Unlock()
							} else if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.role = Follower
							}
							rf.mu.Unlock()
						}
						cond.Broadcast()
					}(i)
				}

				go func() {
					voteMu.Lock()
					defer voteMu.Unlock()
					for voteNum <= len(rf.peers)/2 && finish < len(rf.peers) {
						cond.Wait()
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if voteNum > len(rf.peers)/2 {
						// 选举结束且超过半数
						rf.role = Leader
						// 变成Leader, 重置
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.matchIndex[i] = -1
							// 这里应该分别是log的长度和0
							rf.nextIndex[i] = len(rf.log)
						}

						rf.timer.Reset(HeartBeatTimeout)
					} else {
						//如果选举失败，随机设置选举超时 !!
						rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
						rf.timer.Reset(rf.overtime)
					}
				}()
			}
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.commitIndex >= len(rf.log) {
				fmt.Println("Fuck Length")
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.applyCh <- msg
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower

	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer = time.NewTimer(rf.overtime)

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
