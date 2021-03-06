package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	currentView View
	ack 	 	bool
	recentPing	map[string]time.Time
	mDead		map[string]bool
}

func (vs *ViewServer) PromoteBackup() {
	if vs.currentView.Backup != "" {
		vs.currentView.Primary = vs.currentView.Backup
		vs.currentView.Backup = ""
	}
}
//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.
	if args.Me == vs.currentView.Primary && args.Viewnum == vs.currentView.Viewnum {
		vs.ack = true
	}
	vs.recentPing[args.Me] = time.Now()
	vs.mDead[args.Me] = false
	// Elect Primary 
	// Change Primary
	// Change Backup
	if args.Viewnum == 0 {
		if vs.currentView.Primary == "" {
				vs.currentView.Primary = args.Me
				vs.currentView.Viewnum++
				vs.ack = false
		} else if vs.currentView.Primary == args.Me {
			if vs.currentView.Backup != "" && vs.ack{
				vs.mDead[args.Me] = true
				vs.PromoteBackup()
				vs.getNewB()
				vs.currentView.Viewnum++
				vs.ack = false
			}
		} else if vs.currentView.Backup == ""  {
			if vs.ack {
				vs.currentView.Backup = args.Me
				vs.currentView.Viewnum++
				vs.ack = false
			}
		}
	}
	reply.View = vs.currentView
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.
	reply.View = vs.currentView
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) getNewB() {
	for m, _ := range vs.mDead {
		if !vs.mDead[m] && m != vs.currentView.Primary {
			vs.currentView.Backup = m
			break
		}
	}
}
func (vs *ViewServer) tick() {
	// Your code here.
	// If Someone dead, get rid of it
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for m, _ := range vs.mDead {
		if time.Since(vs.recentPing[m]) > DeadPings*PingInterval {
			vs.mDead[m] = true
		}
	}
			// update view only if ack
	if vs.ack {
		if vs.mDead[vs.currentView.Primary] {
			vs.PromoteBackup()
			vs.getNewB()
			vs.currentView.Viewnum++
			vs.ack = false
		} else if vs.currentView.Backup == "" || vs.mDead[vs.currentView.Backup] {
			vs.currentView.Backup = ""
			vs.getNewB()
			vs.currentView.Viewnum++
			vs.ack = false
		}
	}
}
//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.recentPing = make(map[string]time.Time)
	vs.mDead = make(map[string]bool)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
