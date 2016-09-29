package pbservice

// How to ensure kvs are not lost while Primary shifts?
// twice forward?
import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

import "errors"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	currentView	viewservice.View
	kvs			map[string]string
	history		map[int64]bool
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// There is no duplicate for get, 
	// for that get may fail and retry due to network problems
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.currentView.Primary != pb.me {
		reply.Err = ErrWrongServer
	} else {
		// This is tricky, Forward nothing to Backup to check
		reply.Err = OK
		if pb.currentView.Backup != "" {
			Fargs := ForwardArgs{make(map[string]string),make(map[int64]bool)}
			Freply := ForwardReply{}
			ok := call(pb.currentView.Backup,"PBServer.ProcessForward",Fargs,&Freply)
			if !ok {
				return errors.New("Get Can't connect to Backup")
			}
			reply.Err = Freply.Err
		}
		if reply.Err == OK {
			value, ok := pb.kvs[args.Key]
			if !ok {
				reply.Err = ErrNoKey
				reply.Value = ""
			} else {
				reply.Value = value
			}
		}
	}
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// fmt.Println(pb.kvs["1"])
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// Dup
	if pb.history[args.Count] {
		reply.Err = DuplicateMsg
		return nil
	}
	if pb.currentView.Primary != pb.me {
		reply.Err = ErrWrongServer
	} else {
		var value string
		if args.Op == "Put" {
			value = args.Value
		} else {
			value = pb.kvs[args.Key] + args.Value
		}
		reply.Err = OK
		kvs := make(map[string]string)
		kvs[args.Key] = value
		history := make(map[int64]bool)
		history[args.Count] = true
		Fargs := ForwardArgs{kvs,history}
		Freply := ForwardReply{}	
		if pb.currentView.Backup != "" {
			ok := call(pb.currentView.Backup,"PBServer.ProcessForward",Fargs,&Freply)
			// fmt.Println(pb.currentView.Backup)
			if ok {
				reply.Err = Freply.Err
			} else {
				return errors.New("Can't connect Backup")
			}
		}
		if reply.Err == OK{
			pb.history[args.Count] = true
			pb.kvs[args.Key] = value
		}
	}
	return nil
}

func (pb *PBServer) ProcessForward(args *ForwardArgs, reply *ForwardReply) error {
	// Directly forward the `result` to Backup
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.currentView.Backup != pb.me {
		reply.Err = ErrWrongServer
	} else {
		for k, v := range args.Kvs{
			pb.kvs[k] = v
		}
		for k,v := range args.History{
			pb.history[k] = v
		}
		reply.Err = OK
	}
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//

func (pb *PBServer) tick() {

	// Your code here.
	// Use while to forward 
	// What if Backup dead?
	pb.mu.Lock()
	defer pb.mu.Unlock()
	view, err := pb.vs.Ping(pb.currentView.Viewnum)

	if err == nil{
		// If a server becomes the backup in a new view, the primary should send the kvs
		if view.Backup != "" && view.Backup != pb.currentView.Backup && view.Primary == pb.me {
			kvs := pb.kvs
			args := ForwardArgs{kvs,pb.history}
			reply := ForwardReply{}
			ok := false
			for !ok{
				ok = call(view.Backup,"PBServer.ProcessForward",args,&reply)
				fmt.Println("Forward Again")
			}
		}
		pb.currentView = view
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.kvs = make(map[string]string)
	pb.history = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
