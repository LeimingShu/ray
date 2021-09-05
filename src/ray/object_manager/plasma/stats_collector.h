// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/lifecycle_meta_store.h"

namespace plasma {

// ObjectStatsCollector subscribes to plasma store state changes
// and calculate the store statistics.
//
// TODO(scv119): move other stats from PlasmaStore/ObjectStore/
// ObjectLifeCycleManager into this class.
class ObjectStatsCollector {
 public:
  // Called after a new object is created.
  void OnObjectCreated(const LocalObject &object, const LifecycleMetadata &metadata);

  // Called after an object is sealed.
  void OnObjectSealed(const LocalObject &object, const LifecycleMetadata &metadata);

  // Called BEFORE an object is deleted.
  void OnObjectDeleting(const LocalObject &object, const LifecycleMetadata &metadata);

  // Called after an object's ref count is bumped by 1.
  void OnObjectRefIncreased(const LocalObject &object, const LifecycleMetadata &metadata);

  // Called after an object's ref count is decreased by 1.
  void OnObjectRefDecreased(const LocalObject &object, const LifecycleMetadata &metadata);

  // Debug dump the stats.
  void GetDebugDump(std::stringstream &buffer) const;

 private:
  friend struct ObjectStatsCollectorTest;

  int64_t num_objects_spillable_ = 0;
  int64_t num_bytes_spillable_ = 0;
  int64_t num_objects_unsealed_ = 0;
  int64_t num_bytes_unsealed_ = 0;
  int64_t num_objects_in_use_ = 0;
  int64_t num_bytes_in_use_ = 0;
  int64_t num_objects_evictable_ = 0;
  int64_t num_bytes_evictable_ = 0;

  int64_t num_objects_created_by_worker_ = 0;
  int64_t num_bytes_created_by_worker_ = 0;
  int64_t num_objects_restored_ = 0;
  int64_t num_bytes_restored_ = 0;
  int64_t num_objects_received_ = 0;
  int64_t num_bytes_received_ = 0;
  int64_t num_objects_errored_ = 0;
  int64_t num_bytes_errored_ = 0;
};

}  // namespace plasma