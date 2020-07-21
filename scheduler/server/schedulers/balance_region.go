// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeSizeData struct {
	storeID   uint64
	storeSize int64
}

type storeSize []storeSizeData

func (s storeSize) Len() int           { return len(s) }
func (s storeSize) Less(i, j int) bool { return s[i].storeSize > s[j].storeSize }
func (s storeSize) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func getDistinctReplica() {

}

func getRegion(cluster opt.Cluster, storeID uint64) *core.RegionInfo {
	var region *core.RegionInfo = nil
	cluster.GetPendingRegionsWithLock(storeID, func(tree core.RegionsContainer) {
		region = tree.RandomRegion(make([]byte, 0), make([]byte, 0))
	})
	if region == nil {
		cluster.GetFollowersWithLock(storeID, func(tree core.RegionsContainer) {
			region = tree.RandomRegion(make([]byte, 0), make([]byte, 0))
		})
	}
	if region == nil {
		cluster.GetLeadersWithLock(storeID, func(tree core.RegionsContainer) {
			region = tree.RandomRegion(make([]byte, 0), make([]byte, 0))
		})
	}
	if region == nil {
		return nil
	}
	if len(region.GetPeers()) > cluster.GetMaxReplicas() {
		return nil
	}
	return region
}

//TODO banlance leader
//TODO banlance replica
//TODO banlance region in diff node
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	stores := cluster.GetStores()
	var tmpStoreSize storeSize

	//all of the aval stores
	for _, store := range stores {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			tmpStoreSize = append(tmpStoreSize, storeSizeData{store.GetID(), store.GetRegionSize()})
		}

		fmt.Println(store)

	}
	fmt.Println(tmpStoreSize)
	if len(tmpStoreSize) == 0 {
		return nil
	}
	sort.Sort(tmpStoreSize)
	maxStore := tmpStoreSize[0]
	log.Debugf("chose store %d", maxStore)
	var region *core.RegionInfo

	region = getRegion(cluster, maxStore.storeID)
	if region == nil {
		return nil
	}

	if len(region.GetPeers()) < cluster.GetMaxReplicas() {
		return nil
	}

	if maxStore.storeSize <= tmpStoreSize[tmpStoreSize.Len()-1].storeSize+region.GetApproximateSize()*2 {
		return nil
	}
	newStoreID := uint64(0)
	for i := tmpStoreSize.Len() - 1; i >= 1; i-- {
		storeID := tmpStoreSize[i].storeID
		if region.GetStorePeer(storeID) == nil {
			newStoreID = storeID
			break
		}
	}

	if newStoreID == 0 {
		return nil
	}
	newPeer, _ := cluster.AllocPeer(newStoreID)
	op, _ := operator.CreateMovePeerOperator("", cluster, region, operator.OpBalance, maxStore.storeID, newStoreID, newPeer.Id)
	return op
}
