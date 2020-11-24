#include "ray/rpc/node_manager/node_manager_client_pool.h"

namespace ray {
namespace rpc {

optional<shared_ptr<NodeManagerWorkerClient>> NodeManagerClientPool::GetByID(const std::string &address, const int port) {
  absl::MutexLock lock(&mu_);
  auto id = address + ":" + std::to_string(port);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
      return {};
  }
  return it->second;
}

shared_ptr<NodeManagerWorkerClient> NodeManagerClientPool::GetOrConnect(
    const std::string &address, const int port) {
  absl::MutexLock lock(&mu_);
  auto id = address + ":" + std::to_string(port);
  auto it = client_map_.find(id);
  if (it != client_map_.end()) {
    return it->second;
  }
  auto connection = client_factory_(address, port);
  client_map_[id] = connection;

  RAY_LOG(INFO) << "Connected to " << address << ":" << port;
  return connection;
}

void NodeManagerClientPool::Disconnect(const std::string &address, const int port) {
  absl::MutexLock lock(&mu_);
  auto id = address + ":" + std::to_string(port);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return;
  }
  client_map_.erase(it);
}

} // namespace rpc
} // namespace ray