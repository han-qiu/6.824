package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	DuplicateMsg   = "DuplicateMsg"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op	  string
	Count	int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Count	int64
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type ForwardArgs struct {
	Kvs		map[string]string
	History	map[int64]bool
}
type ForwardReply struct {
	Err	Err
}

