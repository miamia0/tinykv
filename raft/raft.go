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
	"math/rand"
	"sort"
	"sync"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	// StateFollower State of follower
	StateFollower StateType = iota
	// StateCandidate State of candidate
	StateCandidate
	// StateLeader State of leader
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	heartbeatTick int
	electionTick  int
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	candidateVoteGot  int
	candidateVoteLoss int
	mu                sync.Mutex
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	rf := &Raft{
		id:               c.ID,
		Term:             0,
		heartbeatTick:    c.HeartbeatTick,
		electionTick:     c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		RaftLog:          newLog(c.Storage),
	}

	rf.State = StateFollower
	rf.Prs = make(map[uint64]*Progress)
	rf.votes = make(map[uint64]bool)
	initialState, cs, _ := c.Storage.InitialState()
	rf.Term = initialState.Term
	rf.Vote = initialState.Vote

	rf.leadTransferee = 0
	for _, peer := range c.peers {

		rf.Prs[peer] = &Progress{}
		rf.Prs[peer].Next = initialState.Commit + 1
		rf.Prs[peer].Match = initialState.Commit
	}
	for _, node := range cs.Nodes {
		rf.Prs[node] = &Progress{}
		rf.Prs[node].Next = initialState.Commit + 1
		rf.Prs[node].Match = initialState.Commit
	}
	rf.Prs[rf.id] = &Progress{Next: initialState.Commit, Match: initialState.Commit}
	return rf
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	PrevLogIndex := r.Prs[to].Next - 1
	PrevLogTerm, err := r.RaftLog.Term(PrevLogIndex)
	if err != nil {
		fmt.Println(to, r.RaftLog.lastIndex, PrevLogTerm, PrevLogIndex)
		panic(err)
	}
	appendMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Index:   PrevLogIndex,
		LogTerm: PrevLogTerm,
		Entries: r.RaftLog.getEntries(r.Prs[to].Next),
		Commit:  r.RaftLog.GetCommitIndex(),
	}
	if r.State != StateLeader {
		return false
	}
	r.SendMessage(appendMsg)

	return true
}
func (r *Raft) sendSnapshot(to uint64) bool {

	snapShot, err := r.RaftLog.storage.Snapshot()
	for err != nil {
		snapShot, err = r.RaftLog.storage.Snapshot()

	}
	appendMsg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		Term:     r.Term,
		From:     r.id,
		To:       to,
		Snapshot: &snapShot,
		Commit:   r.RaftLog.GetCommitIndex(),
	}
	log.Debugf("%d send snap shot to %d %v ", r.id, to, snapShot.GetMetadata())

	if r.State != StateLeader {
		return false
	}
	r.SendMessage(appendMsg)

	return true
}
func (r *Raft) sendDiffData(to uint64) {
	lastIncludedIndex := r.RaftLog.firstIndex - 1
	if r.Prs[to].Next <= lastIncludedIndex {
		r.sendSnapshot(to)
	} else {

		r.sendAppend(to)
	}

}
func (r *Raft) sendInitHeartbeat(to uint64) {
	// Your Code Here (2A).
	PrevLogIndex := r.Prs[to].Next - 1
	//	fmt.Println(r.id, "sendInitHeartbeat to ", to)
	PrevLogTerm, _ := r.RaftLog.Term(PrevLogIndex)
	heartbeatMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Index:   PrevLogIndex,
		LogTerm: PrevLogTerm,
		//Index:   r.RaftLog.LastIndex(),
		Commit: 0,
	}
	if r.State != StateLeader {
		return
	}
	r.SendMessage(heartbeatMsg)
	return
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	PrevLogIndex := r.Prs[to].Next - 1
	PrevLogTerm, _ := r.RaftLog.Term(PrevLogIndex)
	heartbeatMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Index:   PrevLogIndex,
		LogTerm: PrevLogTerm,
		Commit:  r.RaftLog.GetCommitIndex(),
	}
	//didn't init
	if r.Prs[to].Match == 0 {
		log.Debugf("%d didnot init %d", r.id, to)
		heartbeatMsg.Commit = 0
	}

	if r.State != StateLeader {
		return
	}
	r.SendMessage(heartbeatMsg)
	return
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		//	fmt.Println(r.electionElapsed)
		r.electionElapsed = (r.electionElapsed + 1) % r.electionTimeout
		if r.electionElapsed == 0 {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id})
		}

	case StateLeader:
		if r.heartbeatElapsed == 0 {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, To: r.id, From: r.id})
		}
		r.heartbeatElapsed = (r.heartbeatElapsed + 1) % r.heartbeatTimeout
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.electionTimeout = r.electionTick + rand.Intn(r.electionTick)
	r.State = StateFollower
	r.Term = term
	r.Vote = 0
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.electionTimeout = r.electionTick + rand.Intn(r.electionTick)
	r.State = StateCandidate
	r.Vote = r.id
	r.Term = r.Term + 1
	r.votes = make(map[uint64]bool)
	r.candidateVoteGot = 0
	r.candidateVoteLoss = 0

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.State = StateLeader
	r.Lead = r.id
	r.leadTransferee = 0
	for i := range r.Prs {
		r.Prs[i].Next = r.RaftLog.GetCommitIndex() + 1
		r.Prs[i].Match = r.RaftLog.GetCommitIndex()
	}
	noopMsg := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Term:    r.Term,
		From:    r.id,
		To:      r.id,
		Entries: []*pb.Entry{{Term: r.Term, Index: r.RaftLog.LastIndex() + 1}},
		Commit:  r.RaftLog.GetCommitIndex(),
	}
	log.Debugf("%d become leader", r.id)
	r.StepUnLock(noopMsg)
}
func (r *Raft) StepUnLock(m pb.Message) error {

	switch r.State {
	case StateFollower:

		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHup:
			r.handleElection(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleLeaderTransferFollower(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgHup:
			r.handleElection(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppendResponse:
			r.handlegAppendResponse(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgBeat:
			r.handleSendHeartBeats(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleLeaderTransferLeader(m)
		}
	}
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if r.Prs[r.id] == nil {
		return nil //errors.New("removed peer")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.StepUnLock(m)
}

// SendMessage send message
func (r *Raft) SendMessage(msg pb.Message) {
	msg.From = r.id
	msg.Term = r.Term
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
		return
	}
	if !m.Reject && r.votes[m.From] == false {
		r.candidateVoteGot++
		r.votes[m.From] = true
		if r.candidateVoteGot*2 > len(r.Prs) || len(r.Prs) == 1 {
			r.becomeLeader()
		}
	} else if m.Reject && r.votes[m.From] == false {
		r.candidateVoteLoss++
		r.votes[m.From] = true
		if r.candidateVoteLoss*2 > len(r.Prs) {
			r.becomeFollower(m.Term, 0)
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {

	reply := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Reject:  true,
		Term:    r.Term,
	}
	candidateLastLogIndex := r.RaftLog.LastIndex()
	candidateLastLogTerm, _ := r.RaftLog.Term(candidateLastLogIndex)
	if r.Term < m.Term {
		r.becomeFollower(m.Term, 0)
	}
	if m.Term == r.Term && 0 != r.Vote && r.Vote != m.From {
	} else if m.Term < r.Term {
	} else if m.LogTerm < candidateLastLogTerm {
	} else if m.LogTerm == candidateLastLogTerm && m.Index < candidateLastLogIndex {
	} else {
		r.becomeFollower(m.Term, 0)
		r.Vote = m.From
		reply.Reject = false
	}
	r.SendMessage(reply)
}

//handleElection handle MessageType_MsgHup,
//state: stateCandidate
func (r *Raft) handleElection(m pb.Message) {
	r.becomeCandidate()
	log.Debugf("%d become candidate in %d", r.id, r.Term)
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		return
	}
	RequestVoteMsg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		Term:    r.Term,
		From:    r.id,
		Index:   lastLogIndex,
		LogTerm: lastLogTerm,
	}

	for to := range r.Prs {
		if to == r.id {
			r.candidateVoteGot++
			r.Vote = r.id
			r.votes[r.id] = true
			if r.candidateVoteGot*2 > len(r.Prs) || len(r.Prs) == 1 {
				r.becomeLeader()
			}

		} else {
			RequestVoteMsg.To = to
			r.SendMessage(RequestVoteMsg)
		}
	}
}

//send init HeartBeats when create new peer
func (r *Raft) SendinitHeartBeats() {
	for to := range r.Prs {
		if to != r.id {
			r.sendInitHeartbeat(to)

		}
	}
}

//handleSendHeartBeats handle MessageType_MsgBeat
//state: stateLeader
func (r *Raft) handleSendHeartBeats(m pb.Message) {
	lastIncludedIndex := r.RaftLog.firstIndex - 1
	for to := range r.Prs {
		if to != r.id {
			if r.Prs[to].Match == 0 {
				r.sendInitHeartbeat(to)
			} else if r.Prs[to].Next <= lastIncludedIndex {
				r.sendSnapshot(to)
			} else {
				r.sendHeartbeat(to)
			}

		}
	}
}

//handlePropose handle MessageType_MsgPropose
func (r *Raft) handlePropose(m pb.Message) {
	if r.leadTransferee != 0 {
		return
	}

	lastIncludedIndex := r.RaftLog.firstIndex - 1
	r.RaftLog.appendEntries(m.Entries, r.Term)
	r.updateCommitIndex()
	for to := range r.Prs {
		if to != r.id {
			if r.Prs[to].Next <= lastIncludedIndex {
				r.sendSnapshot(to)
			} else {

				r.sendAppend(to)
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.From != r.Lead {
		return
	}
	reply := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}

	if m.Term < r.Term {
		reply.Reject = true
	}
	if m.Term > r.Term {
		//	r.RaftLog.deleteEntries(r.RaftLog.committed)
	}
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	PrevLogTerm, PrevLogIndex := m.LogTerm, m.Index

	if PrevLogIndex >= r.RaftLog.firstIndex && r.checkPrevLog(PrevLogTerm, PrevLogIndex) == false {
		reply.Reject = true
		reply.Index = PrevLogIndex - 1
	} else {
		r.Vote = m.From
		r.Lead = m.From
		if m.Commit > r.RaftLog.committed {
			newCommit := min(m.GetCommit(), r.RaftLog.LastIndex())
			r.RaftLog.SetCommitIndex(newCommit)
		}
		reply.Index = r.RaftLog.LastIndex()
	}
	r.SendMessage(reply)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.RaftLog.deleteEntries(r.RaftLog.committed)
		r.becomeFollower(m.Term, 0)
		return
	}
	//detect entries need to send
	if m.Index < r.RaftLog.LastIndex() {
		r.Prs[m.From].Next = m.Index + 1
		lastIncludedIndex := r.RaftLog.firstIndex - 1
		if r.Prs[m.From].Next <= lastIncludedIndex {
			r.sendSnapshot(m.From)
		} else {

			r.sendAppend(m.From)
		}
	}
}
func (r *Raft) handlegAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		//back to commited log resp
		r.RaftLog.deleteEntries(r.RaftLog.committed)
		r.becomeFollower(m.Term, 0)
		return
	}
	if !m.Reject {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = r.Prs[m.From].Match + 1

		r.updateCommitIndex()
		if r.leadTransferee != 0 {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				To:      r.leadTransferee,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
			}
			r.SendMessage(msg)
		}

	} else {
		//m.Index is the index that follower asumed matched
		r.Prs[m.From].Next = m.Index + 1
		lastIncludedIndex := r.RaftLog.firstIndex - 1

		if r.Prs[m.From].Next <= lastIncludedIndex {
			r.sendSnapshot(m.From)
		} else {
			r.sendAppend(m.From)
		}
	}

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	reply := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	if m.Term < r.Term {
		reply.Reject = true
		r.SendMessage(reply)
		return
	}
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	PrevLogTerm, PrevLogIndex := m.LogTerm, m.Index
	lastIncludedIndex := r.RaftLog.firstIndex - 1
	if r.checkPrevLog(PrevLogTerm, PrevLogIndex) == false {
		reply.Index = PrevLogIndex - 1
		var ConflictIndex, ConflictTerm uint64
		if r.RaftLog.LastIndex() < PrevLogIndex {
			ConflictIndex = r.RaftLog.LastIndex()
			ConflictTerm = 0
		} else {
			ConflictTerm, _ = r.RaftLog.Term(PrevLogIndex)
			ConflictIndex = min(r.RaftLog.LastIndex(), PrevLogIndex-1)

			for index := PrevLogIndex; index >= lastIncludedIndex; index-- {
				if explicitLogTerm, _ := r.RaftLog.Term(index); explicitLogTerm != ConflictTerm {
					ConflictIndex = index
					break
				}
				if index == 0 {
					break
				}
			}
		}
		reply.Index = ConflictIndex
		reply.Reject = true
	} else {
		r.Vote = m.From
		r.Lead = m.From
		for index := range m.Entries { // update entries
			newEntrieID := PrevLogIndex + uint64(index) + 1
			if r.RaftLog.LastIndex() >= newEntrieID {
				if term, _ := r.RaftLog.Term(newEntrieID); m.Entries[index].Term != term {
					if newEntrieID-lastIncludedIndex >= 1 {
						r.RaftLog.deleteEntries(newEntrieID - 1)
						if uint64(len(r.RaftLog.entries))+lastIncludedIndex < r.RaftLog.stabled {
							r.RaftLog.stabled = r.RaftLog.lastIndex
						}
						r.RaftLog.appendEntries(m.Entries[index:], 0)
						break

					}
				}
			} else {
				r.RaftLog.appendEntries(m.Entries[index:], 0)
				break
			}
		}
		if m.Commit > r.RaftLog.committed {
			newCommit := min(m.Commit, PrevLogIndex+uint64(len(m.Entries)))
			r.RaftLog.SetCommitIndex(newCommit)
		}
		reply.Reject = false
		reply.Term = r.Term
		reply.Index = r.RaftLog.LastIndex()
	}
	r.SendMessage(reply)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	reply := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	if m.Term < r.Term {
		reply.Reject = true
		r.SendMessage(reply)
		return
	}
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	lastIncludedIndex := r.RaftLog.firstIndex - 1
	leaderIndex := m.GetSnapshot().GetMetadata().GetIndex()
	leaderTerm := m.GetSnapshot().GetMetadata().GetTerm()
	if leaderIndex < lastIncludedIndex { ///???
		reply.Reject = true
		reply.Index = lastIncludedIndex
	} else if leaderIndex == lastIncludedIndex {
		reply.Reject = false
		reply.Index = r.RaftLog.LastIndex()

	} else {
		r.Vote = m.From
		r.Lead = m.From
		nodes := m.GetSnapshot().GetMetadata().GetConfState().GetNodes()
		r.RaftLog.maybeCompact()
		r.RaftLog.SetFirstIndex(leaderIndex+1, leaderTerm)
		r.RaftLog.pendingSnapshot = m.GetSnapshot()
		r.RaftLog.firstIndex = leaderIndex + 1
		r.RaftLog.snapTerm = leaderTerm
		if leaderIndex > r.RaftLog.committed {
			newCommit := leaderIndex
			r.RaftLog.SetCommitIndex(newCommit)
			r.RaftLog.SetAppliedIndex(newCommit)
			r.RaftLog.SetStabledIndex(newCommit)
		}
		for _, node := range nodes {
			commited := r.RaftLog.GetCommitIndex()
			if r.Prs[node] == nil {
				r.Prs[node] = &Progress{
					Match: commited,
					Next:  commited + 1,
				}
			}
		}
		reply.Reject = false
		reply.Term = r.Term
		reply.Index = r.RaftLog.LastIndex()

	}
	r.SendMessage(reply)

}
func (r *Raft) checkTransferee(transferee uint64) bool {
	if r.RaftLog.LastIndex() > r.Prs[transferee].Match {
		return false
	}
	return true
}
func (r *Raft) checkvaild(transferee uint64) bool {
	if r.Prs[transferee] == nil {
		return false
	}
	return true
}
func (r *Raft) handleLeaderTransferLeader(m pb.Message) {
	transferee := m.GetFrom()
	if !r.checkvaild(transferee) {
		return
	}
	if !r.checkTransferee(transferee) {
		r.sendDiffData(transferee)
		r.leadTransferee = transferee
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      transferee,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	r.SendMessage(msg)
}
func (r *Raft) handleLeaderTransferFollower(m pb.Message) {
	if m.From == r.id {
		r.StepUnLock(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id})
	}
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	r.StepUnLock(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if r.Prs[id] == nil {
		r.Prs[id] = &Progress{Match: 0, Next: 1}
	}
	if r.State == StateLeader {
		r.sendInitHeartbeat(id)
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	if id != r.id && r.State == StateLeader {
		r.updateCommitIndex()
	}
}

//========utils========

func (r *Raft) updateCommitIndex() {
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	tmpIndex := make([]uint64, len(r.Prs))

	i := 0
	for _, pro := range r.Prs {
		tmpIndex[i] = pro.Match
		i = i + 1

	}

	sort.Sort(uint64Slice(tmpIndex))
	N := tmpIndex[(len(tmpIndex)-1)/2]

	if N > r.RaftLog.committed {
		if commitTerm, _ := r.RaftLog.Term(N); commitTerm == r.Term {
			r.RaftLog.SetCommitIndex(N)
			lastIncludedIndex := r.RaftLog.firstIndex - 1
			if r.State == StateLeader {
				for to := range r.Prs {
					if to != r.id {
						if r.Prs[to].Next <= lastIncludedIndex {
							r.sendSnapshot(to)
						} else {
							r.sendAppend(to)
						}
					}
				}
			}
		}
	}

}
func (r *Raft) checkPrevLog(term uint64, index uint64) bool {
	if r.RaftLog.LastIndex() < index {
		return false
	} else if explicitLogTerm, _ := r.RaftLog.Term(index); explicitLogTerm != term {
		return false
	}
	return true

}
