package status

import (
	"sync/atomic"
)

type Status int32

const (
	InactiveStatus Status = iota
	WaitingStatus
	StartingStatus
	ActiveStatus
	CompletedStatus
	ErrorStatus
)

//func (st *Status) GetString() string {
//	switch st.Get() {
//	case InactiveStatus:
//		return color.Error.SprintFunc()("inactive")
//	case WaitingStatus:
//		return color.Active.SprintFunc()("waiting")
//	case StartingStatus:
//		return color.Active.SprintFunc()("starting")
//	case ActiveStatus:
//		return color.Active.SprintFunc()("active")
//	case ErrorStatus:
//		return color.Error.SprintFunc()("error")
//	default:
//		return color.Error.SprintFunc()("unknown")
//	}
//}

func (st *Status) GetString() string {
	switch st.Get() {
	case InactiveStatus:
		return "inactive"
	case WaitingStatus:
		return "waiting"
	case StartingStatus:
		return "starting"
	case ActiveStatus:
		return "active"
	case CompletedStatus:
		return "completed"
	case ErrorStatus:
		return "error"
	default:
		return "unknown"
	}
}

func (st *Status) Get() Status {
	return Status(atomic.LoadInt32((*int32)(st)))
}

func (st *Status) Waiting() {
	st.set(WaitingStatus)
}

func (st *Status) Starting() {
	st.set(StartingStatus)
}

func (st *Status) Active() {
	st.set(ActiveStatus)
}

func (st *Status) Completed() {
	st.set(CompletedStatus)
}

func (st *Status) Error() {
	st.set(ErrorStatus)
}

func (st *Status) set(s Status) {
	atomic.StoreInt32((*int32)(st), int32(s))
}
