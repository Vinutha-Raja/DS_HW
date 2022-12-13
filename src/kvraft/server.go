package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op_name string
	Key string 
	Value string
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
    keyValueStrore map[string]string
	requestIdMap map[int64]bool
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, state := kv.rf.GetState()
	if !state {
		reply.Err = "NotALeader"
		return
	 }

	 kv.mu.Lock()
	 _, requestExist := kv.requestIdMap[args.RequestId]
	 kv.mu.Unlock()

	 if requestExist {
		kv.mu.Lock()
		 reply.Value = kv.keyValueStrore[args.Key]
		 kv.mu.Unlock()
		 reply.Err = "OK"
	 }

	 log_entry := Op{}
	 log_entry.Op_name = "Get"
	 log_entry.Key = args.Key
	 log_entry.Value = ""
	 log_entry.RequestId = args.RequestId
	 kv.rf.Start(log_entry)

	 var currentTime int64 = time.Now().Unix()
	 for (time.Now().Unix() - currentTime < 2) {
		 kv.mu.Lock()
		 _, requestExist := kv.requestIdMap[args.RequestId]
		 kv.mu.Unlock()
		 if requestExist {
			kv.mu.Lock()
			reply.Value = kv.keyValueStrore[args.Key]
			kv.mu.Unlock()
			reply.Err = "OK"
			return
		 }
		 time.Sleep(100 * time.Millisecond)
	 }
	 reply.Err = "TimedOut"
	 return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, state := kv.rf.GetState()
	if !state {
       reply.Err = "NotALeader"
	   return
	}

	kv.mu.Lock()
	_, requestExist := kv.requestIdMap[args.RequestId]
	kv.mu.Unlock()

	if requestExist {
		reply.Err = "OK"
		return
	}

	log_entry := Op{}
	log_entry.Op_name = args.Op
	log_entry.Key = args.Key
	log_entry.Value = args.Value
	log_entry.RequestId = args.RequestId
	kv.rf.Start(log_entry)
	
	var currentTime int64 = time.Now().Unix()
	for (time.Now().Unix() - currentTime < 2) {
		kv.mu.Lock()
		_, requestExist := kv.requestIdMap[args.RequestId]
		kv.mu.Unlock()
		if requestExist {
			reply.Err = "OK"
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	
	reply.Err = "TimedOut"
    return

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readCommitedEntries() {
	for m := range kv.applyCh {
		if m.CommandValid{
			var op_entry Op
			op_entry, ok := m.Command.(Op)
			if ok {
				kv.stroreToDB(op_entry)
			}
		}
	}
}

func (kv *KVServer) stroreToDB(op_entry Op){
	kv.mu.Lock()
	_, requestExist := kv.requestIdMap[op_entry.RequestId]
	if !requestExist {
		kv.requestIdMap[op_entry.RequestId] = true
		if op_entry.Op_name == "Put"{
			kv.keyValueStrore[op_entry.Key] = op_entry.Value
		}
		if op_entry.Op_name == "Append"{
			kv.keyValueStrore[op_entry.Key] = kv.keyValueStrore[op_entry.Key] + op_entry.Value
		}
	} 
	kv.mu.Unlock()
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.keyValueStrore = make(map[string]string)
	kv.requestIdMap = make(map[int64]bool)
	// You may need initialization code here.
    go kv.readCommitedEntries()
	return kv
}
