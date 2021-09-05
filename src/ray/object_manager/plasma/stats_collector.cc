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

#include "ray/object_manager/plasma/stats_collector.h"

namespace plasma {

void ObjectStatsCollector::OnObjectCreated(const LocalObject &obj,
                                           const LifecycleMetadata & /* unused */) {
  const auto kDataSize = obj.GetObjectInfo().data_size;
  const auto kSource = obj.GetSource();

  if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker) {
    num_objects_created_by_worker_++;
    num_bytes_created_by_worker_ += kDataSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::RestoredFromStorage) {
    num_objects_restored_++;
    num_bytes_restored_ += kDataSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet) {
    num_objects_received_++;
    num_bytes_received_ += kDataSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::ErrorStoredByRaylet) {
    num_objects_errored_++;
    num_bytes_errored_ += kDataSize;
  }

  RAY_CHECK(!obj.Sealed());
  num_objects_unsealed_++;
  num_bytes_unsealed_ += kDataSize;
}

void ObjectStatsCollector::OnObjectSealed(const LocalObject &obj,
                                          const LifecycleMetadata &metadata) {
  RAY_CHECK(obj.Sealed());
  const auto kDataSize = obj.GetObjectInfo().data_size;

  num_objects_unsealed_--;
  num_bytes_unsealed_ -= kDataSize;

  if (metadata.ref_count == 1) {
    if (obj.GetSource() == plasma::flatbuf::ObjectSource::CreatedByWorker) {
      num_objects_spillable_++;
      num_bytes_spillable_ += kDataSize;
    }
  }

  // though this won't happen in practice but add it here for completeness.
  if (metadata.ref_count == 0) {
    num_objects_evictable_++;
    num_bytes_evictable_ += kDataSize;
  }
}

void ObjectStatsCollector::OnObjectDeleting(const LocalObject &obj,
                                            const LifecycleMetadata &metadata) {
  const auto kDataSize = obj.GetObjectInfo().data_size;
  const auto kSource = obj.GetSource();

  if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker) {
    num_objects_created_by_worker_--;
    num_bytes_created_by_worker_ -= kDataSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::RestoredFromStorage) {
    num_objects_restored_--;
    num_bytes_restored_ -= kDataSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet) {
    num_objects_received_--;
    num_bytes_received_ -= kDataSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::ErrorStoredByRaylet) {
    num_objects_errored_--;
    num_bytes_errored_ -= kDataSize;
  }

  if (metadata.ref_count > 0) {
    num_objects_in_use_--;
    num_bytes_in_use_ -= kDataSize;
  }

  if (!obj.Sealed()) {
    num_objects_unsealed_--;
    num_bytes_unsealed_ -= kDataSize;
    return;
  }

  // obj sealed
  if (metadata.ref_count == 1 &&
      kSource == plasma::flatbuf::ObjectSource::CreatedByWorker) {
    num_objects_spillable_--;
    num_bytes_spillable_ -= kDataSize;
  }

  if (metadata.ref_count == 0) {
    num_objects_evictable_--;
    num_bytes_evictable_ -= kDataSize;
  }
}

void ObjectStatsCollector::OnObjectRefIncreased(const LocalObject &obj,
                                                const LifecycleMetadata &metadata) {
  const auto kDataSize = obj.GetObjectInfo().data_size;
  const auto kSource = obj.GetSource();
  const bool kSealed = obj.Sealed();

  // object ref count bump from 0 to 1
  if (metadata.ref_count == 1) {
    num_objects_in_use_++;
    num_bytes_in_use_ += kDataSize;

    if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
      num_objects_spillable_++;
      num_bytes_spillable_ += kDataSize;
    }

    if (kSealed) {
      num_objects_evictable_--;
      num_bytes_evictable_ -= kDataSize;
    }
  }

  // object ref count bump from 1 to 2
  if (metadata.ref_count == 2 &&
      kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
    num_objects_spillable_--;
    num_bytes_spillable_ -= kDataSize;
  }
}

void ObjectStatsCollector::OnObjectRefDecreased(const LocalObject &obj,
                                                const LifecycleMetadata &metadata) {
  const auto kDataSize = obj.GetObjectInfo().data_size;
  const auto kSource = obj.GetSource();
  const bool kSealed = obj.Sealed();

  // object ref count decrease from 2 to 1
  if (metadata.ref_count == 1) {
    if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
      num_objects_spillable_++;
      num_bytes_spillable_ += kDataSize;
    }
  }

  // object ref count decrease from 1 to 0
  if (metadata.ref_count == 0) {
    num_objects_in_use_--;
    num_bytes_in_use_ -= kDataSize;

    if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
      num_objects_spillable_--;
      num_bytes_spillable_ -= kDataSize;
    }

    if (kSealed) {
      num_objects_evictable_++;
      num_bytes_evictable_ += kDataSize;
    }
  }
}

void ObjectStatsCollector::GetDebugDump(std::stringstream &buffer) const {
  buffer << "- objects spillable: " << num_objects_spillable_ << "\n";
  buffer << "- bytes spillable: " << num_bytes_spillable_ << "\n";
  buffer << "- objects unsealed: " << num_objects_unsealed_ << "\n";
  buffer << "- bytes unsealed: " << num_bytes_unsealed_ << "\n";
  buffer << "- objects in use: " << num_objects_in_use_ << "\n";
  buffer << "- bytes in use: " << num_bytes_in_use_ << "\n";
  buffer << "- objects evictable: " << num_objects_evictable_ << "\n";
  buffer << "- bytes evictable: " << num_bytes_evictable_ << "\n";
  buffer << "\n";

  buffer << "- objects created by worker: " << num_objects_created_by_worker_ << "\n";
  buffer << "- bytes created by worker: " << num_bytes_created_by_worker_ << "\n";
  buffer << "- objects restored: " << num_objects_restored_ << "\n";
  buffer << "- bytes restored: " << num_bytes_restored_ << "\n";
  buffer << "- objects received: " << num_objects_received_ << "\n";
  buffer << "- bytes received: " << num_bytes_received_ << "\n";
  buffer << "- objects errored: " << num_objects_errored_ << "\n";
  buffer << "- bytes errored: " << num_bytes_errored_ << "\n";
}
}  // namespace plasma