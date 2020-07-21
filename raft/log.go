// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"sync"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	firstIndex uint64
	lastIndex  uint64
	snapTerm   uint64
	mu         sync.Mutex
}

func (l *RaftLog) GetCommitIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.GetCommitIndexUnlock()
}
func (l *RaftLog) GetCommitIndexUnlock() uint64 {
	return l.committed
}
func (l *RaftLog) SetCommitIndex(commited uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.committed = commited
}

func (l *RaftLog) GetAppliedIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.applied
}
func (l *RaftLog) SetAppliedIndex(applied uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.applied = applied
}

func (l *RaftLog) GetStabledIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stabled
}
func (l *RaftLog) SetStabledIndex(stabled uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stabled = stabled
}
func (l *RaftLog) appendEntries(et []*pb.Entry, term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ets := range et {
		if term != 0 {
			ets.Term = term
			ets.Index = l.lastIndex + 1
		}
		l.entries = append(l.entries, *ets)
		l.lastIndex = ets.Index

	}
}
func (l *RaftLog) deleteEntries(lastIndex uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lastIncludedIndex := l.firstIndex - 1
	if lastIndex > lastIncludedIndex {
		l.entries = l.entries[:lastIndex-lastIncludedIndex]
		l.lastIndex = lastIndex
	} else if lastIndex == lastIncludedIndex {
		l.entries = make([]pb.Entry, 0)
		l.lastIndex = lastIncludedIndex
	}
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	log := &RaftLog{
		storage:         storage,
		committed:       0,
		applied:         0,
		stabled:         0,
		pendingSnapshot: nil,
	}
	log.firstIndex, _ = storage.FirstIndex()
	log.snapTerm, _ = storage.Term(log.firstIndex - 1)
	lastIndex, _ := storage.LastIndex()
	log.lastIndex = lastIndex
	et, _ := storage.Entries(log.firstIndex, lastIndex+1)

	log.entries = append(log.entries, et...)
	log.stabled = lastIndex
	if memstorage, ok := storage.(*MemoryStorage); ok {
		log.applied = log.firstIndex - 1
		log.committed = min(memstorage.hardState.Commit, log.LastIndexUnLock())
	} else {
		hs, _, _ := log.storage.InitialState()
		log.applied = log.firstIndex - 1
		log.committed = min(hs.Commit, log.LastIndexUnLock())
	}

	return log
}
func (l *RaftLog) SetFirstIndex(firstIndex uint64, snapTerm uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Println("fir", firstIndex, l.firstIndex)
	if firstIndex <= l.LastIndexUnLock() {
		l.entries = l.entries[firstIndex-l.firstIndex:]
	} else {
		l.entries = make([]pb.Entry, 0)
		l.lastIndex = firstIndex - 1
	}
	l.firstIndex = firstIndex
	l.snapTerm = snapTerm
	l.maybeCompact()
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}
func (l *RaftLog) getEntriesUnLock(lo uint64) []*pb.Entry {
	offset := l.firstIndex

	ents := make([]*pb.Entry, 0)
	hi := l.LastIndexUnLock()
	if lo < offset || lo > hi {
		return ents
	}
	//fmt.Println("getEntries:112", "lo:", lo, "hi:", hi, "off:", offset)
	for i := lo - offset; i <= hi-offset; i++ {
		ents = append(ents, &l.entries[i])
	}

	// if len(l.entries) == 1 && len(ents) != 0 {
	// 	return nil
	// }
	return ents
}

func (l *RaftLog) getEntries(lo uint64) []*pb.Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.getEntriesUnLock(lo)
}
func (l *RaftLog) unstableEntriesUnLock() []pb.Entry {
	if l.stabled == l.LastIndexUnLock() {
		return make([]pb.Entry, 0)
	}
	et := l.getEntriesUnLock(l.stabled + 1)
	// offset := l.firstIndex
	// fmt.Println("================stable", l.stabled, offset, len(et), l.LastIndexUnLock())
	ents := make([]pb.Entry, 0)

	for i := range et {
		ents = append(ents, *et[i])
	}

	return ents
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.unstableEntriesUnLock()
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEntsUnLock() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents = make([]pb.Entry, 0)
	offset := l.firstIndex
	// fmt.Println("l.applied", l.applied, "commited", l.committed,
	// 	"offset:", offset,
	// 	"len(ent)", len(l.entries))

	for i := l.applied + 1 - offset; i < l.committed+1-offset; i++ {
		//		fmt.Println("\nl.applied", ents, i)

		ents = append(ents, l.entries[i])
	}
	//fmt.Println("\nend ents", ents)

	return ents
}
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextEntsUnLock()
}
func (l *RaftLog) LastIndexUnLock() uint64 {
	//offset := l.firstIndex

	//fmt.Println("offset:", offset)
	// if l.lastIndex != uint64(len(l.entries))-1+offset {
	// 	panic(l.lastIndex*10000 + (uint64(len(l.entries)) - 1 + offset))
	// }
	return l.lastIndex
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	l.mu.Lock()
	defer l.mu.Unlock()
	//fmt.Println("storageIndex:", storageIndex)
	return l.LastIndexUnLock()

}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// l.mu.Lock()
	// defer l.mu.Unlock()
	if i == 0 {
		return 0, nil
	}
	if l.LastIndexUnLock() < i {
		return 0, errors.New(" l.LastIndex() < i request log index out of range")
	}
	offset := l.firstIndex
	//fmt.Println("off", offset)
	if i == offset-1 {
		return l.snapTerm, nil
	}
	if i < offset-1 {
		return 0, errors.New("!i<rf.offset")
	}
	//fmt.Println("index,offset,last", i, offset, l.LastIndexUnLock())
	return l.entries[i-offset].Term, nil
}
