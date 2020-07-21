package raftstore

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}
func (d *peerMsgHandler) sendHeartBeat(region *metapb.Region, leaderPeer *metapb.Peer) {
	if !d.IsLeader() {
		return
	}

	_ = d.ctx.schedulerClient.RegionHeartbeat(
		&schedulerpb.RegionHeartbeatRequest{
			Region:       region,
			Leader:       leaderPeer,
			PendingPeers: d.CollectPendingPeers(),
			//ApproximateSize: *d.ApproximateSize,
		})
	d.ctx.schedulerClient.StoreHeartbeat(
		nil,
		&schedulerpb.StoreStats{
			StoreId: d.storeID(),
		})

}
func (d *peerMsgHandler) handleSplit(req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	var splitReq *raft_cmdpb.SplitRequest = req.Split
	resp := newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{}

	resp.AdminResponse.CmdType = raft_cmdpb.AdminCmdType_Split

	if engine_util.ExceedEndKey(req.Split.SplitKey, d.Region().EndKey) {
		return resp
	}
	peers := make([]*metapb.Peer, 0)

	peersLen := len(splitReq.NewPeerIds)
	if len(d.Region().Peers) < peersLen {
		peersLen = len(d.Region().Peers)
	}

	for i := 0; i < peersLen; i++ {
		peers = append(peers,
			&metapb.Peer{
				Id:      uint64(splitReq.NewPeerIds[i]),
				StoreId: d.Region().Peers[i].StoreId,
			})
	}

	newRegion := &metapb.Region{
		Id:       splitReq.NewRegionId,
		StartKey: splitReq.SplitKey,
		EndKey:   d.Region().EndKey,
		Peers:    peers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: d.Region().GetRegionEpoch().GetConfVer(),
			Version: d.Region().GetRegionEpoch().GetVersion() + 1,
		},
	}
	newLocalRegion := &metapb.Region{
		Id:       d.regionId,
		StartKey: d.Region().StartKey,
		EndKey:   splitReq.SplitKey,
		Peers:    d.Region().Peers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: d.Region().GetRegionEpoch().GetConfVer(),
			Version: d.Region().GetRegionEpoch().GetVersion() + 1,
		},
	}
	//fmt.Println("newRegion", newRegion)
	d.peerStorage.region = newLocalRegion

	//create Peer

	newPeer, _ := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if d.IsLeader() {
		d.sendHeartBeat(d.Region(), d.peer.Meta)
		d.sendHeartBeat(newRegion, newPeer.Meta)
	}

	d.ctx.storeMeta.setRegion(newRegion, newPeer)
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	d.ctx.storeMeta.setRegion(d.Region(), d.peer)
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	d.ctx.router.register(newPeer)

	_ = d.ctx.router.send(newPeer.regionId, message.Msg{RegionID: newPeer.regionId, Type: message.MsgTypeStart})
	kvWB.SetMeta(meta.RegionStateKey(splitReq.NewRegionId), &rspb.RegionLocalState{State: rspb.PeerState_Normal, Region: newRegion})
	kvWB.SetMeta(meta.RegionStateKey(d.regionId), &rspb.RegionLocalState{State: rspb.PeerState_Normal, Region: d.Region()})

	//newPeer.RaftGroup.Raft.SendinitHeartBeats()
	resp.AdminResponse.Split = &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{newRegion}}

	return resp
}

func (d *peerMsgHandler) HandleAdminreq(req *raft_cmdpb.AdminRequest, resps *raft_cmdpb.RaftCmdResponse, kvWB *engine_util.WriteBatch) {
	resps.AdminResponse = &raft_cmdpb.AdminResponse{}
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		{
			compactLog := req.CompactLog
			CompactIndex := compactLog.GetCompactIndex()
			firstIndex, _ := d.peer.peerStorage.FirstIndex()
			if CompactIndex >= firstIndex {
				d.peer.peerStorage.applyState.TruncatedState = &rspb.RaftTruncatedState{
					Index: CompactIndex,
					Term:  compactLog.GetCompactTerm(),
				}
				d.ScheduleCompactLog(firstIndex, CompactIndex)
				d.peer.RaftGroup.Raft.RaftLog.SetFirstIndex(CompactIndex+1, compactLog.CompactTerm)
			}
			resps.AdminResponse.CmdType = raft_cmdpb.AdminCmdType_CompactLog

		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		transferleaderReq := req.TransferLeader
		if d.IsLeader() || d.PeerId() == transferleaderReq.GetPeer().Id {
			d.RaftGroup.TransferLeader(transferleaderReq.GetPeer().Id)
		}
		resps.AdminResponse.CmdType = raft_cmdpb.AdminCmdType_TransferLeader

	case raft_cmdpb.AdminCmdType_ChangePeer:
		changePeerReq := req.ChangePeer
		if d.IsLeader() {
			var buf = make([]byte, 8)
			binary.BigEndian.PutUint64(buf, req.ChangePeer.Peer.StoreId)
			if d.peerStorage.AppliedIndex() > d.RaftGroup.Raft.PendingConfIndex {
				d.RaftGroup.Raft.PendingConfIndex = d.peerStorage.raftState.LastIndex + 1
				d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
					ChangeType: changePeerReq.ChangeType,
					NodeId:     changePeerReq.Peer.Id,
					Context:    buf,
				})
			}
		}
		resps.AdminResponse.ChangePeer = &raft_cmdpb.ChangePeerResponse{Region: d.Region()}
		resps.AdminResponse.CmdType = raft_cmdpb.AdminCmdType_ChangePeer

	case raft_cmdpb.AdminCmdType_Split:
		{
			resps = d.handleSplit(req, kvWB)
		}
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.peer.RaftGroup.HasReady() {
		return
	}
	ready := d.peer.RaftGroup.Ready()

	regions, _ := d.peer.peerStorage.SaveReadyState(&ready)

	kvWB := engine_util.WriteBatch{}
	if regions != nil {
		d.peerStorage.region = regions.Region
		if d.IsLeader() {
			d.sendHeartBeat(regions.Region, d.Meta)
		}
		fmt.Println(d.PeerId(), "set new Region", d.Region())
		d.ctx.storeMeta.setRegion(d.Region(), d.peer)
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	}

	d.peer.Send(d.ctx.trans, ready.Messages)
	// raftWB := engine_util.WriteBatch{}
	for _, entry := range ready.CommittedEntries {
		if entry.Index <= d.peer.peerStorage.AppliedIndex() {
			continue
		}
		resps := newCmdResp()
		resps.Header.CurrentTerm = d.peer.Term()
		if entry.EntryType == eraftpb.EntryType_EntryConfChange {
			var cc eraftpb.ConfChange
			cc.Unmarshal(entry.Data)
			newRegion := d.peerStorage.region
			fmt.Println(d.PeerId(), "excute EntryType_EntryConfChange", cc)
			if cc.ChangeType == eraftpb.ConfChangeType_AddNode {
				{
					newPeer := &metapb.Peer{
						Id:      cc.NodeId,
						StoreId: binary.BigEndian.Uint64(cc.Context),
					}
					newPeers := make([]*metapb.Peer, 0)
					for _, p := range d.Region().Peers {
						if p.Id != cc.NodeId {
							newPeers = append(newPeers, p)
						}
					}
					fmt.Println("add newpeer", newPeer)
					newPeers = append(newPeers, newPeer)
					newLocalRegion := &metapb.Region{
						Id:       d.regionId,
						StartKey: d.Region().StartKey,
						EndKey:   d.Region().EndKey,
						Peers:    newPeers,
						RegionEpoch: &metapb.RegionEpoch{
							ConfVer: d.Region().RegionEpoch.ConfVer + 1,
							Version: d.Region().RegionEpoch.Version,
						},
					}
					d.peerStorage.region = newLocalRegion
					d.insertPeerCache(newPeer)
					d.PeersStartPendingTime[cc.NodeId] = time.Now()
					d.ctx.storeMeta.setRegion(d.Region(), d.peer)
					d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
					kvWB.SetMeta(meta.RegionStateKey(d.regionId), &rspb.RegionLocalState{State: rspb.PeerState_Normal, Region: d.Region()})
				}
			}
			if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
				fmt.Println(d.PeerId(), d.Region(), '\n', d.peerCache)
				if cc.NodeId == d.PeerId() && d.MaybeDestroy() {
					d.destroyPeer()
					return
				} else {
					d.removePeerCache(cc.GetNodeId())
					newPeers := make([]*metapb.Peer, 0)
					delete(d.peerCache, cc.NodeId)
					for _, p := range d.Region().Peers {
						if p.Id != cc.GetNodeId() {
							newPeers = append(newPeers, p)
						}
					}
					newLocalRegion := &metapb.Region{
						Id:       d.regionId,
						StartKey: d.Region().StartKey,
						EndKey:   d.Region().EndKey,
						Peers:    newPeers,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: d.Region().RegionEpoch.ConfVer + 1,
							Version: d.Region().RegionEpoch.Version,
						},
					}
					d.peerStorage.region = newLocalRegion
					kvWB.SetMeta(meta.RegionStateKey(d.regionId), &rspb.RegionLocalState{State: rspb.PeerState_Normal, Region: d.Region()})
					d.ctx.storeMeta.setRegion(d.Region(), d.peer)
					d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})

				}
			}
			if d.IsLeader() {
				d.sendHeartBeat(newRegion, d.Meta)
			}
			d.RaftGroup.ApplyConfChange(cc)
		} else if entry.Data == nil {
			if d.IsLeader() {
				d.sendHeartBeat(d.Region(), d.Meta)
			}
		} else {
			var cc raft_cmdpb.RaftCmdRequest
			cc.Unmarshal(entry.Data)

			err := d.preProposeRaftCommand(&cc)
			if err != nil {
				resps = ErrResp(err)
			}
			if d.IsLeader() && resps.Header.Error != nil && resps.Header.Error.EpochNotMatch != nil {
				d.sendHeartBeat(d.Region(), d.Meta)
			}

			//handleadminirequst
			if cc.AdminRequest != nil {
				req := cc.AdminRequest
				d.HandleAdminreq(req, resps, &kvWB)
			}

			for _, req := range cc.Requests {
				resp := &raft_cmdpb.Response{
					CmdType: req.CmdType,
				}
				switch req.CmdType {
				case raft_cmdpb.CmdType_Invalid:
					{
					}
				case raft_cmdpb.CmdType_Get:
					{
						fmt.Println(util.CheckKeyInRegion(req.Get.Key, d.Region()))
						if err := util.CheckKeyInRegion(req.Get.Key, d.Region()); err != nil {
							if resps.Header.Error == nil {
								resps.Header.Error = &errorpb.Error{KeyNotInRegion: &errorpb.KeyNotInRegion{}}
							} else {
								resps.Header.Error.KeyNotInRegion = &errorpb.KeyNotInRegion{}
							}
						} else {
							_value, _ := d.peerStorage.Get(req.Get)

							resp.Get = &raft_cmdpb.GetResponse{
								Value: _value,
							}
						}
					}
				case raft_cmdpb.CmdType_Put:
					d.peerStorage.Put(req.Put, &kvWB)

					d.SizeDiffHint += uint64(req.Put.Size())
					*d.ApproximateSize += uint64(req.Put.Size())
				case raft_cmdpb.CmdType_Delete:
					if err := util.CheckKeyInRegion(req.Delete.Key, d.Region()); err != nil {
						resps.Header.Error = &errorpb.Error{KeyNotInRegion: &errorpb.KeyNotInRegion{}}
					} else {
						d.peerStorage.Delete(req.Delete, &kvWB)
					}

					resp.Put = &raft_cmdpb.PutResponse{}

				case raft_cmdpb.CmdType_Snap:
					{
						if d.peerStorage.region.Id != cc.Header.RegionId {
						} else {
							resp.Snap = &raft_cmdpb.SnapResponse{
								Region: d.Region(),
							}
						}
					}
				}
				resps.Responses = append(resps.Responses, resp)
			}
			if d.IsLeader() {
				var cb *message.Callback
				for i, p := range d.proposals {
					if p.term == entry.GetTerm() && p.index == entry.GetIndex() {
						cb = p.cb
						if len(d.proposals) == 1 {
							d.proposals = make([]*proposal, 0)
						} else {
							d.proposals = d.proposals[i+1:]
						}
					}
				}
				if cb != nil {
					if len(cc.Requests) != 0 && cc.Requests[0].CmdType == raft_cmdpb.CmdType_Snap {
						cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
					}
					cb.Done(resps)
				}
			} else {
				d.proposals = make([]*proposal, 0)
			}
		}
		d.peerStorage.applyState.AppliedIndex = entry.Index
		kvWB.SetMeta(meta.ApplyStateKey(d.peerStorage.region.Id), &d.peerStorage.applyState)
		kvWB.WriteToDB(d.ctx.engine.Kv)
		kvWB.Reset()
		//apply_state_key
	}
	if d.IsLeader() && rand.Intn(20) == 0 {
		d.sendHeartBeat(d.Region(), d.Meta)
	}
	d.RaftGroup.Advance(ready)

}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {

	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	data, _ := msg.Marshal()
	if d.IsLeader() {
		d.peer.RaftGroup.Propose(data)
		d.proposals = append(d.proposals, &proposal{
			term:  d.peer.RaftGroup.Raft.Term,
			index: d.peer.RaftGroup.Raft.RaftLog.LastIndex(),
			cb:    cb,
		})
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(firstIndex uint64, truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	// log.Debugf("%s handle raft message %s from %d to %d",
	// 	d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	//	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	//	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}

	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {

	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
