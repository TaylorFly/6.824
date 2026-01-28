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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type Role int

const (
	Leader = iota
	Candidater
	Follower
)

const HeartBeatTimeout = 100 * time.Millisecond

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

	lastIncludedIndex int
	lastIncludedTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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

func (rf *Raft) logIndex(index int) int {
	return index - rf.lastIncludedIndex
}
func (rf *Raft) realIndex(index int) int {
	return index + rf.lastIncludedIndex
}
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}
func (rf *Raft) resetElectionTimer() {
	rf.overtime = time.Duration(250+rand.Intn(250)) * time.Millisecond
	rf.timer.Stop()
	rf.timer.Reset(rf.overtime)
}
func (rf *Raft) lastLogTerm() int {
	lastLogIndex := rf.lastLogIndex()
	if lastLogIndex <= rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	} else {
		return rf.log[rf.logIndex(lastLogIndex)].Term
	}
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

// since() is no longer needed as Debug() handles timing

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var log []LogEntry
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// Decode error
	} else {
		rf.currentTerm = currentTerm
		rf.log = log
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
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
// 服务端发现日志太长，会创建一个包含自己当前状态的快照
// 然后调用Snapshot，告诉Raft可以丢弃index之前的日志了
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logIndex := rf.logIndex(index)
	if logIndex < 0 || logIndex >= len(rf.log) {
		return
	}
	rf.lastIncludedTerm = rf.log[logIndex].Term
	rf.log = rf.log[logIndex+1:]
	rf.lastIncludedIndex = index

	rf.persistStateAndSnapshot(snapshot)
}

// Follower接受快照
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.role = Follower
	rf.timer.Reset(rf.overtime)

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	// 这里是 >=
	if args.LastIncludedIndex >= rf.lastIncludedIndex+len(rf.log) {
		rf.log = make([]LogEntry, 1)
		rf.log[0].Term = args.LastIncludedTerm
	} else {
		logIndex := args.LastIncludedIndex - rf.lastIncludedIndex
		rf.log = rf.log[logIndex:]
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.persistStateAndSnapshot(args.Data)
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	rf.persist()
	index = rf.lastLogIndex()
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
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "S%d <- S%d RequestVote T%d, myTerm=%d", rf.me, args.CandidateID, args.Term, rf.currentTerm)
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	// 自己的term比较小
	if rf.currentTerm < args.Term {
		// 如果对方的term更大，那么自己就变成follower，但是不着急把票投给对方
		// 因为可能因为网络分区导致对方的Term更大，但是无法保证对方的日志比自己新
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	if rf.votedFor != -1 {
		reply.VoteGranted = rf.votedFor == args.CandidateID
		return
	}

	var vote bool

	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	if lastLogTerm < args.LastLogTerm {
		vote = true
	} else if lastLogTerm > args.LastLogTerm {
		vote = false
	} else {
		vote = lastLogIndex <= args.LastLogIndex
	}

	reply.VoteGranted = vote
	if vote {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = args.CandidateID
		rf.resetElectionTimer()
		rf.persist()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.ConflictIndex = 0
	reply.ConflictTerm = 0
	reply.Term = rf.currentTerm
	//失败情况1：当前的Term更大
	Debug(dLog, "S%d <- S%d AppendEntries T%d, myTerm=%d", rf.me, args.LeaderID, args.Term, rf.currentTerm)
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	// 当前的term更小（等于）Leader的，那么确定是Follower,统一处理一下
	rf.role = Follower
	rf.votedFor = -1
	rf.currentTerm = args.Term
	rf.persist()
	rf.resetElectionTimer()

	// 失败情况二：日志错误
	// 2.1：prev太长, 下次在log的末尾插入
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}

	if args.PrevLogIndex < rf.lastIncludedIndex {
		if args.PrevLogIndex+len(args.Entries) <= rf.lastIncludedIndex {
			reply.Success = true
			reply.ConflictIndex = rf.lastIncludedIndex + 1
			return
		}
		entriesToSkip := rf.lastIncludedIndex - args.PrevLogIndex
		args.Entries = args.Entries[entriesToSkip:]
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
	}
	// 2.2：日志冲突
	if rf.log[rf.logIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// 否则找到第一个term为prev index处的term，
		// 因为一个term只有一个Leader,要错认为全错
		term := rf.log[rf.logIndex(args.PrevLogIndex)].Term
		index := args.PrevLogIndex
		for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
			if rf.log[rf.logIndex(i)].Term != term {
				index = i + 1
				break
			}
			index = i
		}
		reply.ConflictIndex = index
		reply.ConflictTerm = term
		reply.Success = false
	} else {
		// 这种做法是错误的，比如Leader发送了[1, 2], 由于网络波动，后发的[1, 2, 3]先到，并且commitIndex被应用到状态机
		// [1, 2]随后到，发现prevlogindex处的term相同，这样会删除3,导致错误
		// rf.log = rf.log[:args.PrevLogIndex+1]
		// rf.log = append(rf.log, args.Entries...)

		// Raft:
		// If an existing entry conflicts with a new one (same index but different terms)
		// delete the existing entry and all that follow it (§5.3)
		// 也就是如果有log不匹配的点则删去
		i := 0
		for ; i < len(args.Entries); i++ {
			logIndex := rf.logIndex(args.PrevLogIndex + 1 + i)
			if logIndex >= len(rf.log) {
				break
			}
			if rf.log[logIndex].Term != args.Entries[i].Term {
				rf.log = rf.log[:logIndex]
				break
			}
		}
		if i < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[i:]...)
		}

		reply.Success = true
		rf.persist()

		// Leader的CommitIndex 保证大多数节点提交了
		// 但是当前节点可能没有提交，如果更新的话，可能导致状态机被错误应用
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.lastLogIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.lastLogIndex()
			}
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		for !rf.killed() {
			select {
			case <-rf.timer.C:

				rf.mu.Lock()
				if rf.role == Leader {
					//心跳
					rf.mu.Unlock()
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						if rf.role != Leader {
							rf.mu.Unlock()
							break
						}
						go func(server int) {
							rf.mu.Lock()

							// 如果Follower的日志很落后的时候，Leader中在此之前的log已经被压缩了，无法发送，Follower需要去压缩在这之前的log
							if rf.nextIndex[server] <= rf.lastIncludedIndex {
								Debug(dSnap, "S%d -> S%d InstallSnapshot lastIdx=%d lastTerm=%d", rf.me, server, rf.lastIncludedIndex, rf.lastIncludedTerm)
								args := InstallSnapshotArgs{
									Term:              rf.currentTerm,
									LeaderId:          rf.me,
									LastIncludedIndex: rf.lastIncludedIndex,
									LastIncludedTerm:  rf.lastIncludedTerm,
									Data:              rf.persister.ReadSnapshot(),
								}
								rf.mu.Unlock()
								reply := InstallSnapshotReply{}
								ok := rf.sendInstallSnapshot(server, &args, &reply)

								if ok {
									rf.mu.Lock()
									if reply.Term > rf.currentTerm {
										rf.role = Follower
										rf.votedFor = -1
										rf.currentTerm = reply.Term
										rf.resetElectionTimer()
									} else {
										rf.nextIndex[server] = rf.lastIncludedIndex + 1
										rf.matchIndex[server] = rf.lastIncludedIndex
									}
									rf.mu.Unlock()
								}
								return
							}

							// 准备提交给该server的log
							entries := make([]LogEntry, 0)

							if rf.nextIndex[server] <= rf.lastLogIndex() {
								entries = append(entries, rf.log[rf.logIndex(rf.nextIndex[server]):]...)
							}
							// 上一个log的信息
							prevLogIndex := rf.nextIndex[server] - 1
							prevLogTerm := rf.log[rf.logIndex(prevLogIndex)].Term

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

							Debug(dLog, "S%d -> S%d Sending AppendEntries T%d", rf.me, server, rf.currentTerm)

							ok := rf.sendAppenEntries(server, &appendEntryArg, &reply)

							if ok {
								rf.mu.Lock()
								if len(entries) == 0 {
									Debug(dTimer, "S%d -> S%d Heartbeat T%d reply=%v", rf.me, server, rf.currentTerm, reply)
								}
								Debug(dLog2, "S%d -> S%d AppendEntries OK", rf.me, server)
								if reply.Success {
									rf.nextIndex[server] = prevLogIndex + 1 + len(entries)
									rf.matchIndex[server] = rf.nextIndex[server] - 1

									// 超过半数的Follower确定了同步的位置才更新Leader的commitIndex
									for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
										if rf.logIndex(n) < 0 {
											Debug(dCommit, "S%d commitIdx=%d includeIdx=%d", rf.me, rf.commitIndex, rf.lastIncludedIndex)
										}
										if rf.log[rf.logIndex(n)].Term != rf.currentTerm {
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
									Debug(dLog2, "S%d -> S%d AppendEntries fail, replyTerm=%d myTerm=%d", rf.me, server, reply.Term, rf.currentTerm)
									if reply.Term > rf.currentTerm {
										Debug(dTerm, "S%d stepping down, T%d > T%d", rf.me, reply.Term, rf.currentTerm)
										Debug(dLeader, "S%d Leader -> Follower", rf.me)
										rf.role = Follower
										rf.currentTerm = reply.Term
										rf.votedFor = -1
										rf.persist()
										rf.resetElectionTimer()
									} else {
										rf.nextIndex[server] = reply.ConflictIndex
									}
								}
								rf.mu.Unlock()
							} else {
								return
							}
						}(i)
					}
					rf.mu.Lock()
					if rf.role == Leader {
						rf.timer.Reset(HeartBeatTimeout)
					}
					rf.mu.Unlock()
				} else {
					rf.currentTerm += 1
					rf.role = Candidater
					rf.votedFor = rf.me
					rf.persist()
					rf.resetElectionTimer()
					savedCurrentTerm := rf.currentTerm
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
							if rf.role != Candidater {
								rf.mu.Unlock()
								voteMu.Lock()
								finish++
								voteMu.Unlock()
								cond.Broadcast()
								return
							}

							args := RequestVoteArgs{
								Term:         rf.currentTerm,
								CandidateID:  rf.me,
								LastLogIndex: rf.lastLogIndex(),
								LastLogTerm:  0,
							}
							if len(rf.log) > 0 {
								args.LastLogTerm = rf.log[len(rf.log)-1].Term
							} else if args.LastLogIndex >= 0 {
								args.LastLogTerm = rf.lastIncludedTerm
							}
							rf.mu.Unlock()
							reply := RequestVoteReply{}
							ok := rf.sendRequestVote(server, &args, &reply)

							rf.mu.Lock()
							Debug(dVote, "S%d -> S%d RequestVote T%d, result=%v", rf.me, server, rf.currentTerm, reply)
							rf.mu.Unlock()

							voteMu.Lock()
							finish++
							// voteMu.Unlock() // Don't unlock yet, need to increment voteNum or just finish

							if ok {
								if reply.VoteGranted {
									// voteMu.Lock() already held
									voteNum++
									// voteMu.Unlock()
								} else {
									rf.mu.Lock()
									if reply.Term > rf.currentTerm {
										rf.currentTerm = reply.Term
										rf.votedFor = -1
										rf.role = Follower
										rf.persist()
										// voteNum = 0 // Optional, but effectively stops election
										// finish = len(rf.peers) // Speed up termination
									}
									rf.mu.Unlock()
								}
							}
							voteMu.Unlock()
							cond.Broadcast()
						}(i)
					}

					go func() {
						voteMu.Lock()
						for voteNum <= len(rf.peers)/2 && finish < len(rf.peers) {
							cond.Wait()
						}
						success := voteNum > len(rf.peers)/2
						voteMu.Unlock()
						rf.mu.Lock()
						if success && rf.role == Candidater && rf.currentTerm == savedCurrentTerm {
							// 选举结束且超过半数
							Debug(dLeader, "S%d Achieved Majority for T%d, becoming Leader", rf.me, rf.currentTerm)
							rf.role = Leader
							rf.timer.Reset(HeartBeatTimeout)
							// 变成Leader, 重置
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							for i := 0; i < len(rf.peers); i++ {
								rf.matchIndex[i] = 0
								// 这里应该分别是log的长度和0
								rf.nextIndex[i] = rf.lastLogIndex() + 1
							}
						} else {
							//如果选举失败，随机设置选举超时 !!
							rf.resetElectionTimer()
						}
						rf.mu.Unlock()
					}()
				}
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
			realIndex := rf.logIndex(rf.lastApplied)
			if realIndex < 0 || realIndex >= len(rf.log) {
				continue
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[realIndex].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
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
	// rf.commitIndex = -1
	// rf.lastApplied = -1
	// rf.lastIncludedIndex = -1
	// rf.lastIncludedTerm = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}

	rf.role = Follower

	rf.overtime = time.Duration(250+rand.Intn(250)) * time.Millisecond
	rf.timer = time.NewTimer(rf.overtime)

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// After restoring state, update lastApplied and commitIndex based on snapshot
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
