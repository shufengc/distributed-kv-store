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
	"sort"

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

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	maxDownTime := cluster.GetMaxStoreDownTime()
	stores := cluster.GetStores()

	// Filter suitable stores: up and down time < max
	var suitable []*core.StoreInfo
	for _, store := range stores {
		if store.IsUp() && store.DownTime() < maxDownTime && !store.IsTombstone() {
			suitable = append(suitable, store)
		}
	}
	if len(suitable) < 2 {
		return nil
	}

	// Sort by region size descending (biggest first)
	sort.Slice(suitable, func(i, j int) bool {
		return suitable[i].GetRegionSize() > suitable[j].GetRegionSize()
	})

	// Try to find a region to move from the biggest stores
	for i := 0; i < balanceRegionRetryLimit && i < len(suitable); i++ {
		sourceStore := suitable[i]
		sourceID := sourceStore.GetID()

		// Select region: pending > follower > leader
		var region *core.RegionInfo
		cluster.GetPendingRegionsWithLock(sourceID, func(container core.RegionsContainer) {
			if container != nil {
				region = container.RandomRegion(nil, nil)
			}
		})
		if region == nil {
			cluster.GetFollowersWithLock(sourceID, func(container core.RegionsContainer) {
				if container != nil {
					region = container.RandomRegion(nil, nil)
				}
			})
		}
		if region == nil {
			cluster.GetLeadersWithLock(sourceID, func(container core.RegionsContainer) {
				if container != nil {
					region = container.RandomRegion(nil, nil)
				}
			})
		}
		if region == nil {
			continue
		}

		// Skip under-replicated regions; let replica checker add replicas first
		if len(region.GetPeers()) < cluster.GetMaxReplicas() {
			continue
		}

		// Select target: smallest region size, exclude source and stores that already have the region
		var targetStore *core.StoreInfo
		for j := len(suitable) - 1; j >= 0; j-- {
			candidate := suitable[j]
			if candidate.GetID() == sourceID {
				continue
			}
			if region.GetStorePeer(candidate.GetID()) != nil {
				continue
			}
			targetStore = candidate
			break
		}
		if targetStore == nil {
			continue
		}

		targetID := targetStore.GetID()

		// Value check: sourceSize - targetSize > 2 * region.ApproximateSize
		sourceSize := sourceStore.GetRegionSize()
		targetSize := targetStore.GetRegionSize()
		if sourceSize-targetSize <= 2*region.GetApproximateSize() {
			continue
		}

		// Create MovePeer operator
		newPeer, err := cluster.AllocPeer(targetID)
		if err != nil {
			continue
		}
		op, err := operator.CreateMovePeerOperator(
			"balance-region",
			cluster,
			region,
			operator.OpBalance,
			sourceID,
			targetID,
			newPeer.GetId(),
		)
		if err != nil {
			continue
		}
		return op
	}

	return nil
}
