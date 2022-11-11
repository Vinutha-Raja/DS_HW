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
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
	"fmt"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	CurrentTerm int 
	VotedFor int 
	Log []LogEntry
	CommitIndex int
	LastApplied int
	NextIndex []int
	MatchIndex []int
	CandidateId int
	LastAppendEntryTime int64
	State string
	TotalVotes int
	VoteTerm int // Term for which the voting is already done 
	ElectionTimeout int64
	ElectionTimeoutNum int
	ApplyCh chan ApplyMsg
	AllowApplyToSM bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type AppendEntryArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm  int
}

//
//Log entry : contains term and command
//
type LogEntry struct {
	Command interface{}
	CurrentTerm int 
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.State == "Leader" {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)


}


//
// restore previously persisted state.
//
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var VotedFor int 
	var Log []LogEntry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&VotedFor) != nil ||  d.Decode(&Log) != nil {
	   fmt.Printf("Server %d Error reading persisted data", rf.CandidateId)
	} else {
	  rf.CurrentTerm = currentTerm
	  rf.VotedFor = VotedFor
	  rf.Log = Log
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int 
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int 
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// fmt.Printf("\ngot request vote got by %d, from %d\n", rf.CandidateId, args.CandidateId)
	reply.CurrentTerm = rf.CurrentTerm
	if args.Term >= rf.CurrentTerm {
		// if not voted or voted for the same candidate in the same term 
		// and the consistency check passes

		var mylastindex int = len(rf.Log) - 1
		var mylastTerm int = rf.Log[mylastindex].CurrentTerm

        // TODO : last index >= or >
		if (rf.VoteTerm < args.Term || (rf.VoteTerm == args.Term && rf.VotedFor == args.CandidateId)) && !(mylastTerm > args.LastLogTerm || (args.LastLogTerm == mylastTerm && mylastindex > args.LastLogIndex)){
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.persist()
			rf.VoteTerm = args.Term
			rf.CurrentTerm = args.Term
			rf.persist()
			rf.State = "Follower"
			rand.Seed(time.Now().UnixNano())
			rf.ElectionTimeoutNum = rand.Intn(500 - 400 + 1) + 400
			rf.ElectionTimeout = time.Now().UnixNano() / 1000000 + int64(rf.ElectionTimeoutNum)
			// fmt.Printf("\nCandidate %d voting for %d in term %d\n", rf.CandidateId, args.CandidateId, args.Term)
		}else {
			// currentTerm is less than Term
			if args.Term > rf.CurrentTerm {
				rf.CurrentTerm = args.Term
				rf.persist()
				rf.State = "Follower"
				// rand.Seed(time.Now().UnixNano())
				// rf.ElectionTimeoutNum = rand.Intn(500 - 400 + 1) + 400
				// rf.ElectionTimeout = time.Now().UnixNano() / 1000000 + int64(rf.ElectionTimeoutNum)
			}
			// fmt.Printf("LOG not complete \n")
			reply.VoteGranted = false
		}
	} else {
		// term is less than currentTerm
		// fmt.Printf("not granting vote term is less than currentTerm\n")
		reply.VoteGranted = false
	}
	// fmt.Printf("\n Vote granted value for Candidate %d voting for %d in term %d is %v", rf.CandidateId, args.CandidateId, args.Term, reply.VoteGranted)
	rf.mu.Unlock()
	return
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	
	if ok {
		rf.mu.Lock()
		if reply.VoteGranted {
			rf.TotalVotes = rf.TotalVotes + 1
			// fmt.Printf("\nUpdating total vote count for candidate %d, total vote %d", rf.CandidateId, rf.TotalVotes)
		}
		if reply.CurrentTerm > rf.CurrentTerm {
			rf.CurrentTerm = reply.CurrentTerm
			rf.persist()
			rf.State = "Follower"
			// rand.Seed(time.Now().UnixNano())
			// rf.ElectionTimeoutNum = rand.Intn(500 - 400 + 1) + 400
			// rf.ElectionTimeout = time.Now().UnixNano() / 1000000 + int64(rf.ElectionTimeoutNum)
			// fmt.Printf("\nupdating term from response ****")
		}
		
		// Check if it has received majority votes, if yes make it a leader and send heartbeats
	
		var total_servers int = len(rf.peers)
	
		if rf.TotalVotes >= (total_servers/2 + 1) && rf.State == "Candidate" {
			// fmt.Printf("\nTotal votes for candidate %d in term %d is %d with peer count: %d ", rf.CandidateId, rf.CurrentTerm, rf.TotalVotes, len(rf.peers))
			rf.State = "Leader"
            // initialise nextIndex and matchIndex
			rf.MatchIndex = make([]int, len(rf.peers))
			rf.NextIndex = make([]int, len(rf.peers))
			for i := range rf.peers {
				rf.NextIndex[i] = len(rf.Log)
			}

			// fmt.Printf("\n Candidate %d becoming leader in term %d", rf.CandidateId, rf.CurrentTerm)
			go rf.sendPeriodicHeartBeats()
		}
		rf.mu.Unlock()
		
	}
	return ok
}


func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	// fmt.Printf("\nAppend Entry received by %d from %d for term %d ", rf.CandidateId, args.LeaderId, args.Term)
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return 
	} else {
		// fmt.Printf("\nSetting append Entry time")
		rf.LastAppendEntryTime = time.Now().UnixNano() / int64(time.Millisecond)
		rf.State = "Follower"
		rf.CurrentTerm = args.Term
		rf.persist()
		reply.Success = true
	}

    // //TODO check >= ?
	// if args.Term >= rf.CurrentTerm  {
	// 	fmt.Printf("\nSetting append Entry time")
	// 	rf.LastAppendEntryTime = time.Now().UnixNano() / int64(time.Millisecond)
	// 	rf.State = "Follower"
	// 	// rand.Seed(time.Now().UnixNano())
	// 	// rf.ElectionTimeoutNum = rand.Intn(500 - 400 + 1) + 400
	// 	// rf.ElectionTimeout = time.Now().UnixNano() / 1000000 + int64(rf.ElectionTimeoutNum)
	// 	rf.CurrentTerm = args.Term
	// }
	reply.ConflictIndex = 0
	reply.ConflictTerm = 0
	// prevLogIndex is > mylastLogIndex
	myLastIndex := len(rf.Log) - 1
	if args.PrevLogIndex > myLastIndex  {
		reply.ConflictIndex = myLastIndex + 1
		reply.Success = false
		// fmt.Printf("\n Server %d, args.PrevLogIndex > myLastIndex, log %v", rf.CandidateId, rf.Log)
		rf.mu.Unlock()
		return
	}

	myLastTerm := 0
	if args.PrevLogIndex >= 0 {
		myLastTerm = rf.Log[args.PrevLogIndex].CurrentTerm
	}

	if myLastTerm != args.PrevLogTerm {
		reply.ConflictTerm = myLastTerm
		reply.ConflictIndex = args.PrevLogIndex
		for i := args.PrevLogIndex; i >= 0 && rf.Log[i].CurrentTerm == myLastTerm; i-- {
			reply.ConflictIndex = i
		}
		rf.Log = rf.Log[:args.PrevLogIndex]
		rf.persist()
		reply.Success = false
		// fmt.Printf("\n Server %d, myLastTerm != args.PrevLogTerm", rf.CandidateId)
		rf.mu.Unlock()
		return
	}
    // If consistency check passes then append the logs 
	commit_id := args.PrevLogIndex + 1
	start := 0
	for ; commit_id < myLastIndex + 1 && start < len(args.Entries); commit_id, start = commit_id + 1, start + 1 {
		if args.Entries[start].CurrentTerm != rf.Log[commit_id].CurrentTerm {
			break
		}
	}
	args.Entries = args.Entries[start:]
	rf.Log = rf.Log[:commit_id]
	rf.persist()
	if len(args.Entries) > 0 {
		rf.Log = append(rf.Log, args.Entries...)
		rf.persist()
	}
	// fmt.Printf("\n Commit index before for server %d is %d", rf.CandidateId, rf.CommitIndex)
	// Check for commitIndex update 
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, len(rf.Log) - 1 )
	}
	// fmt.Printf("\n Commit index after for server %d is %d", rf.CandidateId, rf.CommitIndex)
	if rf.CommitIndex > rf.LastApplied && !rf.AllowApplyToSM {
		rf.AllowApplyToSM = true
		// fmt.Printf("\n Commit index changed for server %d", rf.CandidateId)
		go rf.sendCommittedEntries()
	}

	reply.Success = true
	rf.mu.Unlock()

}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) sendCommittedEntries() {
	rf.mu.Lock()
	// for rf.LastApplied < rf.CommitIndex {
	// 	rf.LastApplied += 1
	// 	comm := ApplyMsg{}
	// 	comm.CommandValid = true
	// 	comm.Command = rf.Log[rf.LastApplied].Command
	// 	comm.CommandIndex = rf.LastApplied 
	// 	fmt.Printf("***********Candidate %d, Committing entry comm.CommandIndex: %d, comm.CommandIndex: %v\n", rf.CandidateId, comm.CommandIndex, comm.Command)
	// 	rf.ApplyCh <- comm
	// }
	lastApplied := rf.LastApplied
	commitIndex := rf.CommitIndex
	rf.mu.Unlock()
	for i := lastApplied + 1; i <= commitIndex; i++ {
		rf.mu.Lock()
		log := rf.Log[i].Command
		rf.mu.Unlock()
		// fmt.Printf("\nCandidate %d applying value %v on channel with commitIndex %d", rf.CandidateId, log, i)
		rf.ApplyCh <- ApplyMsg{CommandIndex: i, Command: log, CommandValid: true}
	}
	rf.mu.Lock()
	rf.LastApplied = commitIndex
	rf.AllowApplyToSM = false
	rf.mu.Unlock()
} 

func (rf *Raft) findEntriesToBeCommitted (){
	for commit_id := len(rf.Log) - 1; commit_id > rf.CommitIndex; commit_id-- {
		majority := 1
		if rf.Log[commit_id].CurrentTerm == rf.CurrentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.MatchIndex[i] >= commit_id {
					majority++
				}
			}
		}
		if majority > len(rf.peers)/2 {
			// fmt.Printf("\nCandidate %d, Current Commit Id %d ", rf.CandidateId, rf.CommitIndex)
			rf.CommitIndex = commit_id
			// fmt.Printf("\nCandidate %d, New Commit Id %d ", rf.CandidateId, rf.CommitIndex)
			break
		}
	}
    // Check if a new Commit Id is found and apply only if there is no other apply is being done
	if rf.CommitIndex > rf.LastApplied && !rf.AllowApplyToSM {
		rf.AllowApplyToSM = true
		go rf.sendCommittedEntries()
	}
}


func (rf *Raft) sendHeartBeats(server int, args *AppendEntryArgs, reply *AppendEntryReply) {

	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	if ok {
		rf.mu.Lock()
		// fmt.Printf("\nRecieved appendEntry response from server %d , by %d", server, rf.CandidateId)
		if reply.Term > rf.CurrentTerm {
			// fmt.Printf("\nCandidate %d  updating tetm from response ***", rf.CandidateId)
			rf.CurrentTerm = reply.Term
			rf.persist()
			rf.State = "Follower"
			// rand.Seed(time.Now().UnixNano())
			// rf.ElectionTimeoutNum = rand.Intn(500 - 400 + 1) + 400
			// rf.ElectionTimeout = time.Now().UnixNano() / 1000000 + int64(rf.ElectionTimeoutNum)
			rf.mu.Unlock()
			return 
		}
        // If my term changed after sending the RPC
		if args.Term != rf.CurrentTerm {
			// fmt.Printf("\nserver %d, is not in the same state as before sending RPC to %d ", rf.CandidateId, server)
            rf.mu.Unlock()
			return
		}

		if reply.Success {
			// fmt.Printf("\n Server %d, Before rf.MatchIndex: %d, rf.NextIndex: %d", server, rf.MatchIndex[server], rf.NextIndex[server])
			rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.NextIndex[server] = rf.MatchIndex[server] + 1
			// fmt.Printf("\n Server %d, After rf.MatchIndex: %d, rf.NextIndex: %d", server, rf.MatchIndex[server], rf.NextIndex[server])
			// check what can be committed
			// fmt.Printf("\nserver %d, received success response from %d", rf.CandidateId, server)
			rf.findEntriesToBeCommitted()
		} else {
			i := len(rf.Log) - 1
			for ; i > 0; i-- {
				if reply.ConflictTerm == rf.Log[i].CurrentTerm {
					break
				}
			}
			if i <= 0 {
				rf.NextIndex[server] = reply.ConflictIndex
				// fmt.Printf("\nCandidate %d, rf.NextIndex[server] : %d ", server, rf.NextIndex[server])
			} else {
				rf.NextIndex[server] = i + 1
				// fmt.Printf("\nCandidate %d, rf.NextIndex[server] : %d ", server, rf.NextIndex[server])
			}

		}
		rf.mu.Unlock()

	}

}

func (rf *Raft) sendPeriodicHeartBeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.State == "Leader" {
			var peersCount int = len(rf.peers) 

			// fmt.Printf("\nCandidate %d sending appendEntry for term %d", rf.CandidateId, rf.CurrentTerm)
			for i := 0; i < peersCount; i++ {
				if i != rf.me {
					args := AppendEntryArgs{}
					args.Term = rf.CurrentTerm
					args.LeaderId = rf.CandidateId
					args.PrevLogIndex = rf.NextIndex[i] - 1
					args.PrevLogTerm = rf.Log[args.PrevLogIndex].CurrentTerm
					args.LeaderCommit = rf.CommitIndex
					entries := rf.Log[rf.NextIndex[i]:]
					args.Entries = make([]LogEntry, len(entries))
					copy(args.Entries, entries)
					// fmt.Printf("\nCANDIDATE %d, lENGTH OF ENTRIES: %d to %d", rf.CandidateId, len(entries), i)
					reply := AppendEntryReply{}
					go rf.sendHeartBeats(i, &args, &reply)
				}
			}	
		}
        rf.mu.Unlock()
        time.Sleep(100 * time.Millisecond)
	}
	
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.State == "Leader" {
		// fmt.Printf("client command received by : %d\n", rf.CandidateId)
        rf.Log = append(rf.Log, LogEntry{command, rf.CurrentTerm})
		rf.persist()
		// rf.NextIndex[rf.me] = rf.NextIndex[rf.me] + 1
		// rf.MatchIndex[rf.me] = rf.MatchIndex[rf.me] + 1
		index = len(rf.Log) - 1
		term = rf.CurrentTerm
		isLeader = true
		// fmt.Printf("\n************candidate %d, command %v, index %d term %d\n", rf.CandidateId, command, index, term)

	}
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// Waits for Election Time Out. 
		// Checks if it received any appendEntry RPC. 
		// If not becomes candidate
		// Starts Election

		rand.Seed(time.Now().UnixNano())
		rf.mu.Lock()
		rf.ElectionTimeoutNum = rand.Intn(500 - 400 + 1) + 400
		rf.ElectionTimeout = time.Now().UnixNano() / 1000000 + int64(rf.ElectionTimeoutNum)
		// fmt.Printf("\nElection Timeout: %d current time : %d of server: %d ", rf.ElectionTimeout, time.Now().UnixNano() / 1000000, rf.CandidateId)
		// rf.mu.Unlock()
		// Sleep until Election Timeout happens
		var timeout_temp int64 = rf.ElectionTimeout
		rf.mu.Unlock()
        for timeout_temp -  (time.Now().UnixNano() / 1000000) > 0 {
			// fmt.Printf("sleeping")
			time.Sleep(time.Until(time.Unix(timeout_temp/1000, 0)))
			rf.mu.Lock()
			timeout_temp = rf.ElectionTimeout
			rf.mu.Unlock()
		}

		// if rf.ElectionTimeout -  (time.Now().UnixNano() / 1000000) > 0 {
		// 	rf.mu.Unlock()
		// 	time.Sleep(time.Until(time.Unix(rf.ElectionTimeout/1000, 0)))
		// }
		rf.mu.Lock()
		var currentTime int64 = time.Now().UnixNano()/ int64(time.Millisecond)

		if (currentTime - rf.LastAppendEntryTime > int64(rf.ElectionTimeoutNum) && rf.State != "Leader") {
			// fmt.Printf("\nCandidate %d Dint receive appendEntry hence starting election",rf.CandidateId)
			rf.State = "Candidate"
		}

		if (rf.State ==  "Candidate") {
			
            //start election
		    rf.CurrentTerm = rf.CurrentTerm + 1
			rf.persist()
			rf.TotalVotes = 1
			// fmt.Printf("\nSTARTING ELECTION: %d in term %d with votecount %d", rf.CandidateId, rf.CurrentTerm, rf.TotalVotes)
			rf.VoteTerm = rf.CurrentTerm

		    // Send request vote RPC to all the peers
		    var peersCount int = len(rf.peers) 
		    for i := 0; i < peersCount; i++ {
				if i != rf.me {
					args := RequestVoteArgs{}
					args.Term = rf.CurrentTerm
					args.CandidateId = rf.CandidateId
					args.LastLogIndex = len(rf.Log) - 1
					args.LastLogTerm = rf.Log[len(rf.Log) - 1].CurrentTerm
					reply := RequestVoteReply{}
					go rf.sendRequestVote(i, &args, &reply)
			    }
		    }		   
		}
		rf.mu.Unlock()
		
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	

	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.State = "Follower"
	rf.CandidateId = me
	rf.VotedFor = -1
	rf.TotalVotes = 0

    // 2B init
    rf.CommitIndex = 0
	rf.LastApplied = 0
	// rf.CurrentIndex = 0
	rf.Log = make([]LogEntry, 0)
	rf.Log = append(rf.Log, LogEntry{CurrentTerm: 0})
	rf.ApplyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	fmt.Printf("\nCalling ticker")
	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
