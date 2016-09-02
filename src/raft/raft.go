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

import "sync"
import "labrpc"
// import "log"
import "bytes"
import "encoding/gob"
import "fmt"
import "math/rand"
import "time"


type STATE int
const (
	FOLLOWER STATE 	= 	0
	CANDIDATE 	  	=	1
	LEADER			=	2
)
func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func timeToCan() time.Duration {
	return time.Duration(150 + rand.Intn(150))*time.Millisecond
}
func timeToRe() time.Duration {
	return time.Duration(150 + rand.Intn(150))*time.Millisecond
}
func resetTimer(timer *time.Timer, f func() time.Duration) {
	timer.Reset(f())
}
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Entry struct {
	term int
	command int
	index int
}
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	state 		STATE
	currentTerm int
	votedFor  	int
	log			map[int]Entry

	// For follower
	timer		*time.Timer

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term 			int		// candidate's term
	CadidateId 		int		// candidate's request vote
	LastLogIndex 	int		// index of candidate's last log entry
	LastLogTerm 	int		// term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int			// current term, for candidate to update itself
	VoteGranted	bool	// true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		if rf.state != FOLLOWER {
			rf.InitFollower()
		}
	}
	reply.Term = rf.currentTerm
	LastLogIndex := rf.lastIndex()
	LastLogTerm := rf.log[LastLogIndex].term
	var upToDate bool
	if LastLogTerm != args.LastLogTerm {
		upToDate = args.LastLogTerm >= LastLogTerm
	} else {
		upToDate = args.LastLogIndex >= LastLogIndex
	}
	if args.Term >= rf.currentTerm && rf.votedFor < 0 && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CadidateId
		resetTimer(rf.timer, timeToCan)
	} else {
		reply.VoteGranted = false
	}
	fmt.Println(args.CadidateId, "to", rf.me, reply.VoteGranted)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term 			int
	LeaderId 		int
	PrevLogIndex 	int
	PrevLogTerm 	int
	Entries 		map[int]Entry
	LeaderCommit 	int
}

type AppendEntriesReply struct {
	Term 		int
	Success 	bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// This is not right
	resetTimer(rf.timer, timeToCan)
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		if rf.state != FOLLOWER {
			rf.InitFollower()
		}
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	}
	entry, ok := rf.log[args.PrevLogIndex]
	if len(args.Entries) != 0{
		if !ok||entry.term != args.PrevLogTerm {
			reply.Success = false
		}
	}
	if reply.Success == false {
		return
	}
	reply.Success = true
	// find conflict
	var conflict bool
	i := args.PrevLogIndex
	for {
		entry0, ok0 := args.Entries[i]
		if !ok0 {
			// no conflict
			break
		}
		entry1, ok1 := rf.log[args.PrevLogIndex]
		if !ok1 {
			// find conflict
			conflict = true
			break
		}
		if entry0.term != entry1.term {
			conflict = true
			break;
		}
		i++
	}
	j := i
	if conflict {
		for {
			_, ok = rf.log[j]
			if ok {
				delete(rf.log,j)
			} else {
				break
			}
			j++
		}
	}
	for {
		entry, ok = args.Entries[i]
		if ok {
			rf.log[j] = entry
		} else {
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndex())
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func (rf *Raft) updateTerm(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1
}
func (rf *Raft) lastIndex() int {
	return len(rf.log)
}
func (rf *Raft) InitFollower() {
	rf.state = FOLLOWER
	rf.votedFor = -1
	resetTimer(rf.timer,timeToCan)
}
func (rf *Raft) InitCandidate() {
	rf.state = CANDIDATE
	rf.timer = time.NewTimer(0*time.Second)
	time.Sleep(time.Nanosecond)
}
func (rf *Raft) InitLeader() {
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	value := rf.lastIndex()
	for i,_ := range rf.nextIndex {
		rf.nextIndex[i] = value + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}
func (rf *Raft) Run() {
	START:
	switch rf.state {
	case FOLLOWER:
		select{
		case <-rf.timer.C:
			rf.mu.Lock()
			rf.InitCandidate()
			rf.mu.Unlock()
			goto START
		default:
			goto START
		}
	case CANDIDATE:
		select{
		case <- rf.timer.C:
			rf.mu.Lock()
			fmt.Println("CANDIDATE")
			resetTimer(rf.timer, timeToRe)
			fmt.Println("CANDIDATE")
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.mu.Unlock()
			num := 0
			index := rf.lastIndex()
			args := RequestVoteArgs{rf.currentTerm,rf.me,index,rf.log[index].term}
			for i:=0;i<len(rf.peers);i++ {
				if i == rf.me {
					continue
				}
				reply := RequestVoteReply{}
				rf.sendRequestVote(i,args,&reply)
				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.InitFollower()
					rf.mu.Unlock()
					goto START
				}
				if reply.VoteGranted {
					num++
				}
			}
			if 2*num > len(rf.peers) {
				rf.mu.Lock()
				rf.InitLeader()
				rf.mu.Unlock()
				goto START
			}
		default:
			goto START
		}
	case LEADER:
		var Entries map[int]Entry
		// heart beat
		PrevLogIndex := 0
		PrevLogTerm := 0
		args := AppendEntriesArgs{rf.currentTerm,rf.me,PrevLogIndex,PrevLogTerm,Entries,rf.commitIndex}
		for i:=0;i<len(rf.peers);i++ {
			if i == rf.me {
				continue
			}
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(i,args,&reply)

		}
		goto START
	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.log = make(map[int]Entry)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.timer = time.NewTimer(time.Hour)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.InitFollower()
	go rf.Run()
	return rf
}
