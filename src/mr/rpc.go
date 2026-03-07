package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Task Task
}

type ReportTaskArgs struct {
	TaskID   int
	TaskType TaskType
}

type ReportTaskReply struct {
}
