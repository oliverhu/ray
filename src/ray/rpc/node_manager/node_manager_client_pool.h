// Copyright 2020 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/rpc/node_manager/node_manager_client.h"

using absl::optional;
using std::shared_ptr;

namespace ray {
namespace rpc {

typedef std::function<std::shared_ptr<NodeManagerWorkerClient>(const std::string &, const int)>
    NodeManagerClientFactoryFn;

class NodeManagerClientPool {
 public:
  NodeManagerClientPool() = delete;

  optional<shared_ptr<NodeManagerWorkerClient>> GetByID(const std::string &address, const int port);

  shared_ptr<NodeManagerWorkerClient> GetOrConnect(const std::string &address, const int port);

  void Disconnect(const std::string &address, const int port);
 private:
  absl::Mutex mu_;
  NodeManagerClientFactoryFn defaultClientFactory(rpc::ClientCallManager &ccm) const {
    return [&](const std::string &address, const int port) {
      return std::shared_ptr<NodeManagerWorkerClient>(NodeManagerWorkerClient::make(address, port, ccm));
    };
  };
  NodeManagerClientFactoryFn client_factory_;
  absl::flat_hash_map<std::string, shared_ptr<NodeManagerWorkerClient>> client_map_
      GUARDED_BY(mu_);

};

} // namespace rpc
} // namespace ray
