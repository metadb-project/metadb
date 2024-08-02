package status

import (
	"sync/atomic"
)

type Stream int32

const (
	StreamInactive Stream = iota
	StreamWaiting
	StreamStarting
	StreamActive
	StreamError
)

func (st *Stream) GetString() string {
	switch st.Get() {
	case StreamInactive:
		return "inactive"
	case StreamWaiting:
		return "waiting"
	case StreamStarting:
		return "starting"
	case StreamActive:
		return "active"
	case StreamError:
		return "error"
	default:
		return "unknown"
	}
}

func (st *Stream) Get() Stream {
	return Stream(atomic.LoadInt32((*int32)(st)))
}

func (st *Stream) Waiting() {
	st.set(StreamWaiting)
}

func (st *Stream) Starting() {
	st.set(StreamStarting)
}

func (st *Stream) Active() {
	st.set(StreamActive)
}

func (st *Stream) Error() {
	st.set(StreamError)
}

func (st *Stream) set(s Stream) {
	atomic.StoreInt32((*int32)(st), int32(s))
}

type Sync int32

const (
	SyncNormal Sync = iota
	SyncSnapshot
	SyncSnapshotComplete
)

func (sy *Sync) GetString() string {
	switch sy.Get() {
	case SyncNormal:
		return "normal"
	case SyncSnapshot:
		return "snapshot"
	case SyncSnapshotComplete:
		return "snapshot_complete"
	default:
		return "unknown"
	}
}

func (sy *Sync) Get() Sync {
	return Sync(atomic.LoadInt32((*int32)(sy)))
}

func (sy *Sync) Snapshot() {
	sy.set(SyncSnapshot)
}

func (sy *Sync) SnapshotComplete() {
	sy.set(SyncSnapshotComplete)
}

func (sy *Sync) set(s Sync) {
	atomic.StoreInt32((*int32)(sy), int32(s))
}
