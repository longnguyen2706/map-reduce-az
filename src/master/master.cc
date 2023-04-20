#include <iostream>
#include <memory>
#include <string>
#include <chrono>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <csignal>
#include <iostream>
#include <fstream>
#include <future>
#include <optional>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <mutex>
#include <queue>

#include "../azure_storage.hpp"
#include "../leader_election.hpp"
#include "../function_helper.hpp"
#include "../etcd_backup.hpp"
#include "../http_server.hpp"

#include "../dnslookup.h"
#include "../dto.hpp"
#include "mrmsg.grpc.pb.h"

using namespace std::chrono;

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using mrmsg::File;
using mrmsg::MrService;
using mrmsg::MapRequest;
using mrmsg::MrResponse;
using mrmsg::MrStatus;
using mrmsg::PingWorkerRequest;
using mrmsg::PingWorkerResponse;
using mrmsg::ReduceRequest;
using mrmsg::WorkerInfo;
using mrmsg::WorkerStatus;

using namespace std::chrono_literals;
using json = nlohmann::json;

// Define mutex
std::mutex t_mutex; // task, worker
std::mutex r_mutex; // request
volatile std::atomic<bool> do_exit(false);

void signal_handler(int signal)
{
  std::cout << "Received kill signal " << signal << std::endl;
  do_exit = true;
  exit(1);
}

std::string get_time_string()
{
  auto now = std::chrono::system_clock::now();
  auto in_time_t = std::chrono::system_clock::to_time_t(now);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), "%d-%m-%Y %H-%M-%S");
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////////////

enum WorkerState
{
  IDLE,
  BUSY,
  DEAD
};

enum MasterRole
{
  LEADER,
  FOLLOWER
};

enum Phase
{
  MAP,
  REDUCE
};

////////////////////////////////////////////////////////////////////////////////////////
class RpcClient
{
public:
  RpcClient(std::shared_ptr<Channel> channel)
      : stub_(MrService::NewStub(channel)) {}

  /// @brief  Send a mapper request to the RPC server
  /// @param index
  std::future<MrResponse> async_mapper_assign(
      MapRequest request)
  {
    std::cout << "Sending map task: " << request.id() << std::endl;

    // Container for the data we expect from the server.
    return std::async(std::launch::async, [this, request]
                      {
        MrResponse reply;
        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        // ClientContext context;
        ClientContext context;
        auto cq = std::make_unique<CompletionQueue>();
        std::chrono::system_clock::time_point deadline =
          std::chrono::system_clock::now() + std::chrono::milliseconds(FH::TASK_TIMEOUT);
        context.set_deadline(deadline);

        Status status;
        std::unique_ptr<ClientAsyncResponseReader<MrResponse>> response_reader(
          stub_->PrepareAsyncMapperAssign(&context, request, cq.get()));
        
        response_reader->StartCall();
        response_reader->Finish(&reply, &status, (void *)(intptr_t) 1);
        void* got_tag;
            bool ok = false;
            cq->Next(&got_tag, &ok);
            if (ok && got_tag == (void*)1) {
                if (status.ok()) {
                  // std::cout << "Status for task "<<request.id() 
                  // <<" : " << FH::print_mr_status(reply.status()) << std::endl;
                } else {
                  // std::cout << "RPC failed for task: " <<request.id() << std::endl;
                  reply.set_id(request.id());
                  reply.set_status(MrStatus::FAILURE);
                }
              }
          return reply; });
  }

  // send a reducer request to the RPC server
  std::future<MrResponse> async_reducer_assign(
      ReduceRequest request)
  {
    std::cout << "Sending reduce  task: " << request.id() << std::endl;

    // Container for the data we expect from the server.
    return std::async(std::launch::async, [this, request]
                      {
        MrResponse reply;
        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        // ClientContext context;
        ClientContext context;
        auto cq = std::make_unique<CompletionQueue>();
        std::chrono::system_clock::time_point deadline =
          std::chrono::system_clock::now() + std::chrono::milliseconds(FH::TASK_TIMEOUT);
        context.set_deadline(deadline);

        Status status;
        std::unique_ptr<ClientAsyncResponseReader<MrResponse>> response_reader(
          stub_->PrepareAsyncReducerAssign(&context, request, cq.get()));

        response_reader->StartCall();
        response_reader->Finish(&reply, &status, (void *)(intptr_t) 1);

        void* got_tag;
            bool ok = false;
            cq->Next(&got_tag, &ok);
            if (ok && got_tag == (void*)1) {
                if (status.ok()) {
                  // std::cout << "Status for task "<<request.id() 
                  // <<" : " << FH::print_mr_status(reply.status()) << std::endl;
                } else {
                  // std::cout << "RPC failed for task: " <<request.id() << std::endl;
                  reply.set_id(request.id());
                  reply.set_status(MrStatus::FAILURE);
                }
          }
          
    return reply; });
  }

  // ping workers and update the map
  std::future<PingWorkerResponse> async_ping_worker(std::string worker_id, int index)
  {
    PingWorkerRequest request;
    request.set_id(worker_id);

    // Container for the data we expect from the server.
    return std::async(std::launch::async, [this, request, index]
                      {
      PingWorkerResponse reply;
      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      // ClientContext context;
      ClientContext context;
      auto cq = std::make_unique<CompletionQueue>();
      std::chrono::system_clock::time_point deadline =
          std::chrono::system_clock::now() + std::chrono::milliseconds(FH::PING_TIMEOUT);
      context.set_deadline(deadline);

      Status status;
      std::unique_ptr<ClientAsyncResponseReader<PingWorkerResponse>> response_reader(
          stub_->PrepareAsyncPingWorker(&context, request, cq.get()));

      response_reader->StartCall();
      response_reader->Finish(&reply, &status, (void *)(intptr_t)1);

      void *got_tag;
      bool ok = false;
      cq->Next(&got_tag, &ok);
      if (ok && got_tag == (void *)1)
      {
        if (status.ok())
        {
          // std::cout << "Status for ping worker " << index << " : "
          //  << FH::print_worker_status(reply.status()) << std::endl;
        }
        else
        {
          // std::cout << "RPC failed for ping worker " << index  << std::endl;
          reply.set_id(request.id());
          reply.set_status(WorkerStatus::UNHEALTHY);
        }
      }
      return reply; });
  }

private:
  std::unique_ptr<MrService::Stub> stub_;
};

/////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////
class Master
{
  /*
This wil init the master node, with provided RPC server add and
azure blob connection string
*/
public:
  Master(
      const std::string &appId,
      const std::string &rpc_port,
      const std::string &rpc_address,
      const std::string &etcd_address,
      const std::string &etcd_password,
      const std::string &azure_blob_conn,
      int fail) : _azure_storage(azure_blob_conn),
                  _etcd_address(etcd_address),
                  _rpc_port(rpc_port),
                  _rpc_address(rpc_address),
                  _fail(fail),
                  _etcd_backup(etcd_address + ":2379", etcd_password)
  {
    std::cout << "***Start Master with AppId:***" << appId << std::endl;

    // Leader election
    std::cout << "Leader election starting" << std::endl;
    _leader_election = std::make_shared<LeaderElection>(
        _etcd_address + ":2379",
        etcd_password,
        appId,
        [&](std::string id)
        {
          std::cout << "!!! Iam the leader now: " << id << std::endl;
          load_req_and_phase_from_etcd();
          load_tasks_from_etcd();
          _role = MasterRole::LEADER;
          std::cout << "Done loading previous tasks and states from etcd"
                    << std::endl;
          std::cout << " - Total tasks: " << _task_queue.size() << std::endl;
          std::cout << " - Total requests: " << _req_queue.size() << std::endl;
          std::cout << " - Current phase: " << (int)_phase << std::endl;
        });
    _leader_election->StartElection();
    std::cout << "Leader election started" << std::endl;

    // Init http server
    _http_server = std::make_shared<HttpServer>("0.0.0.0", "8080");
    _http_server->register_req_callback(
        [this](const std::string &body){
          std::cout << " ---Enqueue request ---" << std::endl;
          auto j = json::parse(body);
          auto id = get_time_string();
          std::cout << " - id: " << id << std::endl;
          std::cout << " - mapper: " << j["mapper"] << std::endl;
          std::cout << " - reducer: " << j["reducer"] << std::endl;
          std::cout << " - data: " << j["data"] << std::endl;
          std::cout << " - m_num: " << j["m_num"] << std::endl;
          std::cout << " - r_num: " << j["r_num"] << std::endl;
          // Start map reduce process
          DTO::Request req = DTO::Request(
              id, 
              j["data"],
              "output",
              j["mapper"],
              j["reducer"],
              j["m_num"],
              j["r_num"]
            );
          r_mutex.lock();
            _req_queue.push_back(req);
            store_req_and_phase_to_etcd();
          r_mutex.unlock();
          return "Done Submission!";
        });
    _http_server->register_tasks_callback(
        [this](const std::string &body){
          nlohmann::json j;
          if(_cur_req)
            j = {_cur_req->to_json()};
          for (auto r : _req_queue)
            j.push_back(r.to_json());
          return j.dump();
        });
    _http_server->register_kill_callback(
        [this](){
          do_exit = true;
          exit(1);
        });

    std::cout << "Done initialize master" << std::endl;
  }

  // get or create shard map according to current phase 
  // this one is helpful for reconstructing the shard map when master restart
  void get_or_create_shard_map(const std::string data_dir, const int shard)
  {
    if (_shard_map.empty())
    {
      //1) First get details of all the files in the input folder
      std::vector<Models::BlobItem> blobItems = FH::getBlobsFromFolder(_azure_storage, data_dir);
      FH::print_blob_info(blobItems);

      // 2) Generate shard map
      // compare _phase with Phase::MAP or Phase::REDUCE
      if (_phase == Phase::MAP)
      {
        std::cout << "Sharding for map phase" << std::endl;
        _shard_map = FH::shard_for_mapper(blobItems, shard);
      }
      else
      {
        std::cout << "Sharding for reduce phase" << std::endl;
        _shard_map = FH::shard_for_reducer(blobItems, shard);
      }
      FH::print_shard_map(_shard_map);
    }
  }

  // produce shard -> requests -> filter out requests based on ids in task queue
  int map(const std::string input_folder,
          const std::string output_folder,
          const std::string mapper_script, const int m_num,
          const int r_num,
          bool recreate_worker_tasks = true)
  {
    get_or_create_shard_map(input_folder, m_num);

    if (recreate_worker_tasks)
    {
      // update task queue
      t_mutex.lock();
        for (auto &it : _shard_map)
        {
          _task_queue.push_back(DTO::Task(it.first));
        }
      t_mutex.unlock();
    }

    // 2) loop until task queue is empty
    // every time, assign talks to available workers, update cur_tasks
    // function exits when task queue is empty and cur_tasks is empty
    while (_task_queue.size() > 0)
    {
      std::vector<std::string> workers = get_available_workers(); // TODO: get available workers
      std::cout << "No of available workers: " << workers.size() << std::endl;  
      int n = std::min(_task_queue.size(), workers.size());

      t_mutex.lock();
        for (int i = 0; i < n; i++)
        {
          DTO::Task task = _task_queue.front();
          _task_queue.pop_front();
          std::string worker = workers[i];
          std::cout << "Assigning task " << task.idx << " to worker ip " << worker << std::endl;
          _worker_state_map[worker] = WorkerState::BUSY;

          int idx = task.idx;
          task.worker_id = worker;
          task.end_time = FH::get_current_time_s() + FH::TASK_TIMEOUT / 1000;
          _cur_tasks.push_back(task);

          std::string id = FH::generate_task_id(input_folder, idx, true);
          MapRequest request = FH::create_map_request(
            id, input_folder, output_folder, mapper_script, r_num, _shard_map[idx]);

          // send request to worker
          auto client = _rpc_clients[worker];
          _task_futures.push_back(client->async_mapper_assign(
              request));
        }
      store_tasks_to_etcd();
      t_mutex.unlock();

      while (_task_futures.size() > 0)
      {
        // wait for this batch of tasks to finish
        // prevent busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    return 0;
  }

  void wait_for_task_response_th()
  {
    while (_role == MasterRole::FOLLOWER)
    {
      // busy waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    while (!do_exit)
    {
      // currently we only clear the cur_task and task_future after the batch is completed
      // better way is to update everytime we got a response
      std::vector<DTO::Task> requeue_tasks;
      for (auto &f : _task_futures)
      {
        // std::cout << "wait for task response " << _task_futures.size() << std::endl;
        MrResponse response = f.get();
        std::cout << "Response for task: " <<response.id()
                  << " status " << FH::print_mr_status(response.status()) << std::endl;
        if (response.status() != MrStatus::SUCCESS)
        {
          requeue_tasks.push_back(DTO::Task(FH::task_id_to_idx(response.id()))); //reassign failed task
        }
      }

      // push back failed task here
      t_mutex.lock();
        for (auto &t : requeue_tasks)
        {
          _task_queue.push_back(t);
        }
        // free worker 
        for (auto &t : _cur_tasks)
        {
          _worker_state_map[t.worker_id] = WorkerState::IDLE;
        }
        // clear cur_tasks
        _cur_tasks.clear();
        _task_futures.clear();
        store_tasks_to_etcd();
      t_mutex.unlock();

      sleep(FH::CHECK_INTERVAL/1000); // sleep for 1 second
    }
  }

  // TODO: the code here is largely the same as map, need to refactor
  int reduce(const std::string input_folder,
             const std::string intermediate_folder,
             const std::string output_folder,
             const std::string reducer_script, const int r_num,
             bool recreate_worker_tasks = true)
  {
    get_or_create_shard_map(intermediate_folder, r_num);

    if (recreate_worker_tasks)
    {
      // 1) Get shard map
      t_mutex.lock();
      for (auto &it : _shard_map)
      {
        _task_queue.push_back(DTO::Task(it.first));
      }
      t_mutex.unlock();
    }

    // 2) loop until task queue is empty
    // every time, assign talks to available workers, update cur_tasks
    // function exits when task queue is empty and cur_tasks is empty
    while (_task_queue.size() > 0)
    {
      std::vector<std::string> workers = get_available_workers(); // TODO: get available workers
      int n = std::min(_task_queue.size(), workers.size());

      t_mutex.lock();
      for (int i = 0; i < n; i++)
      {
        DTO::Task task = _task_queue.front();
        _task_queue.pop_front();
        std::string worker = workers[i];
        std::cout << "Assigning task " << task.idx << " to worker " << worker << std::endl;
         _worker_state_map[worker] = WorkerState::BUSY;
        int idx = task.idx;
        task.worker_id = worker;
        task.end_time = FH::get_current_time_s() + FH::TASK_TIMEOUT / 1000;
        _cur_tasks.push_back(task);

        std::string id = FH::generate_task_id(input_folder, idx, true);
        ReduceRequest request = FH::create_reduce_request(
          id, intermediate_folder, output_folder, reducer_script, r_num, _shard_map[idx]);

        // send request to worker
        auto client = _rpc_clients[worker];
        _task_futures.push_back(client->async_reducer_assign(
            request));
      }
      store_tasks_to_etcd();
      t_mutex.unlock();

      while (_task_futures.size() > 0)
      {
        // wait for this batch of tasks to finish
        // prevent busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    return 0;
  }

  std::vector<DTO::Task> get_need_reassign_tasks(
    std::set<string> dead_workers)
  {
    std::vector<DTO::Task> need_reassign_tasks;

    int cur_time = FH::get_current_time_s();
    for (auto &task : _cur_tasks)
    {
      // dead worker
      for (auto w : dead_workers)
      {
        if (task.worker_id == w)
        {
          need_reassign_tasks.push_back(task);
        }
      }
      // stale task
      if (cur_time > task.end_time)
      {
        need_reassign_tasks.push_back(task);
      }
    }
    return need_reassign_tasks;
  }

  // ping all workers to check their status and update the map
  // requeue the tasks that are assigned to dead workers or stale (expired)
  void ping_workers_and_update_tasks_th()
  {
    while (_role == MasterRole::FOLLOWER)
    {
      // busy waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    while (!do_exit)
    {
      ping_workers(); // sychronous call
      std::set<string> dead_workers = get_dead_workers();

      std::vector<DTO::Task> need_reassign_tasks = get_need_reassign_tasks(dead_workers);
      std::set<int> need_reassign_task_idx;
      for (auto task : need_reassign_tasks)
      {
        need_reassign_task_idx.insert(task.idx);
      }

      std::vector<DTO::Task> new_cur_tasks;
      for (auto task : _cur_tasks)
      {
        if (need_reassign_task_idx.find(task.idx) == need_reassign_task_idx.end())
        {
          new_cur_tasks.push_back(task);
        }
      }

      // update cur_tasks and requeue tasks
      t_mutex.lock();
        _cur_tasks = new_cur_tasks;
        for (auto task : need_reassign_tasks)
        {
          _task_queue.push_back(DTO::Task(task.idx));
          _worker_state_map[task.worker_id] = WorkerState::IDLE ; // free worker
        }
        store_tasks_to_etcd();
      t_mutex.unlock();

       // just some print out for debugging 
      for (auto task : need_reassign_tasks)
      {
        std::cout << "Reassign task " << task.idx << " & free worker "  << task.worker_id;
      }
      std::cout<< std::endl;

      sleep(FH::PING_INTERVAL/1000); // sleep
    }
  }

  // ping all workers and update worker map
  // list of workers = _worker_ips
  // worker status could be:
  // 1) dead
  // 2) idle (alive and free)
  // 3) busy (alive and assigned with task)
  void ping_workers()
  {
    std::cout << "Ping workers" << std::endl;
    std::vector<std::future<PingWorkerResponse>> ping_task_futures;
    std::vector<std::string> workers = get_workers();
    for (int i = 0; i < workers.size(); ++i)
    {
      std::string worker_ip = workers[i];
      auto client = _rpc_clients[worker_ip];
      ping_task_futures.push_back(client->async_ping_worker(worker_ip, i));
    }

    std::vector<PingWorkerResponse> responses;
    for (auto &future : ping_task_futures)
    {
      responses.push_back(future.get());
    }
    // TODO: what if some worker change ip? or new worker join?
    t_mutex.lock();
      for (auto &response : responses)
      {
        std::cout << "Worker " << FH::print_worker_status(response.status()) << std::endl;
        if (response.status() == WorkerStatus::UNHEALTHY)
        {
          _worker_state_map[response.id()] = WorkerState::DEAD; // TODO: see if we need mutex here
        } 
        else { // HEALTHY
          if (_worker_state_map[response.id()] == WorkerState::DEAD){
            _worker_state_map[response.id()] = WorkerState::IDLE; // move back to idle, ignore other status
          }
        }
      }
    t_mutex.unlock();
  }

  std::vector<string> get_workers()
  {
    std::vector<string> workers;
    for (auto ele : _worker_state_map)
    {
      workers.push_back(ele.first);
    }
    return workers;
  }

  std::vector<string> get_available_workers()
  {
    std::vector<string> available_workers;
    std::vector<string> workers = get_workers();
    for (auto w : workers)
    {
      if ( _worker_state_map[w] == WorkerState::IDLE)
      {
        available_workers.push_back(w);
      }
    }
    return available_workers;
  }

  // build a set of dead_workers
  std::set<string> get_dead_workers()
  {
    std::set<string> dead_workers;
    for (auto ele : _worker_state_map)
    {
      if (ele.second == WorkerState::DEAD)
      { // dead
        dead_workers.insert(ele.first);
      }
    }
    return dead_workers;
  }

  void do_map_reduce()
  {
    while (_role == MasterRole::FOLLOWER)
    {
      // busy waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    while (!do_exit)
    {
      // std::cout << " --- waiting for new request ---" << std::endl;
      if (!_req_queue.empty())
      {
        reinit_worker_clients(); // TODO: only do this when new worker join
        r_mutex.lock();
          DTO::Request req = _req_queue.front();
          _req_queue.pop_front();
          _cur_req = req;
          store_req_and_phase_to_etcd();
        r_mutex.unlock();
        std::cout << " --- Processing req:  ---" << req.id << req.input_dir << std::endl;

        // prep for mapping phase
        if (_first_init)
        {
          std::cout << " --- First init ---" << std::endl;
          _first_init = false;
          if (_phase == Phase::MAP)
          {
            do_map(false);
            do_reduce();
          }
          else
            do_reduce(false);
        }
        else
        {
          do_map();
          do_reduce();
        }
      }
      _cur_req = std::nullopt;
      _phase = Phase::MAP;
      store_req_and_phase_to_etcd();
      sleep(FH::CHECK_INTERVAL/1000);
    }
  }

  void do_map(bool recreate_worker_tasks = true)
  {
    _shard_map.clear();
    _phase = Phase::MAP;
    store_req_and_phase_to_etcd();
    std::cout << " --- Start map phase ---" << std::endl;
    int map_result = map(
      _cur_req->input_dir, "dfs-intermediate", _cur_req->mapper_script,
      _cur_req->num_mappers, _cur_req->num_reducers, recreate_worker_tasks);
    std::cout << " --- Done map phase ---" << std::endl;
  }

  void do_reduce(bool recreate_worker_tasks = true)
  {
    // prepare for reduce phase
    _shard_map.clear();
    _phase = Phase::REDUCE;
    store_req_and_phase_to_etcd();
    std::cout << "--- Start reduce phase ---" << std::endl;
    int reduce_result = reduce(
      _cur_req->input_dir, "dfs-intermediate", "dfs-dest",
      _cur_req->reducer_script, _cur_req->num_reducers, recreate_worker_tasks);
    std::cout << " --- Done reduce phase ---" << std::endl;
  }

  // TODO: think if we need to call this preodically
  void reinit_worker_clients()
  {
    // get the ip address of the worker nodes
    auto worker_ips = dns_lookup(_rpc_address, 4);
    // to test on local
    if (worker_ips.size() == 0)
    {
      worker_ips.push_back("localhost");
    }

    _rpc_clients.clear();
    t_mutex.lock();
      _worker_state_map.clear(); // TODO: think about it. Currently, just clear the worker info map
      for (int i = 0; i < worker_ips.size(); i++)
      {
        std::string ip = worker_ips[i];
        cout << "Worker ip: " << ip << endl;
        _rpc_clients[ip] = std::make_shared<RpcClient>(
            grpc::CreateChannel(ip + ":" + _rpc_port,
                                grpc::InsecureChannelCredentials()));
          _worker_state_map[ip] = WorkerState::IDLE;
      t_mutex.unlock();
    }

    std::cout << " --- Done init workers ---" << std::endl;
  }

  void start_http_server_th()
  {
    while (_role == MasterRole::FOLLOWER)
    {
      // busy waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // need to be async
    _http_server->start();
  }

private:
  std::string _rpc_port;
  std::string _rpc_address;
  MasterRole _role = MasterRole::FOLLOWER;

  // the key is the port number
  std::unordered_map<string, std::shared_ptr<RpcClient>> _rpc_clients; // the key is the ip address
  std::string _etcd_address;

  AzureStorage _azure_storage;
  std::shared_ptr<HttpServer> _http_server;
  std::shared_ptr<LeaderElection> _leader_election;

  // these are data structure that will be synced between masters
  // TODO: we might not need cur_tasks and cur_req
  std::deque<DTO::Task> _task_queue;  // protected by t_mutex
  std::vector<DTO::Task> _cur_tasks;  // protected by t_mutex
  std::optional<DTO::Request> _cur_req = std::nullopt ; // protected by r_mutex
  std::deque<DTO::Request> _req_queue; // protected by r_mutex
  Phase _phase;             // map or reduce
  bool _first_init = true;

  // these are data structure that will be regeneated when master restarts
  std::map<int, std::vector<File>> _shard_map;
  std::vector<std::future<MrResponse>> _task_futures;
  std::unordered_map<std::string, WorkerState> _worker_state_map; // protected by t_mutex

  // setting for artificial failure
  int _fail = 0;
  int _m_processed = 0;
  int _r_processed = 0;
  EtcdBackup _etcd_backup;

  //////////////////////// These are to deal with msg conversion ////////////////////////
  void store_tasks_to_etcd()
  {
    /// json array
    nlohmann::json j = {};
    for (auto t : _cur_tasks)
    {
      j.push_back(t.to_json());
    }
    for (auto t : _task_queue)
    {
      j.push_back(t.to_json());
    }
    _etcd_backup.put(j.dump(), BackupType::TASKS);
  }

  void store_req_and_phase_to_etcd()
  {
    nlohmann::json j;
    if (_cur_req)
      j = {_cur_req->to_json()};
    else
      j = {};
    for (auto r : _req_queue)
    {
      j.push_back(r.to_json());
    }
    _etcd_backup.put(j.dump(), BackupType::REQUESTS);
    _etcd_backup.put(std::to_string(_phase), BackupType::PHASE);
  }

  void load_tasks_from_etcd()
  {
    const auto tasks = _etcd_backup.get(BackupType::TASKS);
    if (tasks == "")
    {
      std::cout << "\n No tasks in etcd" << std::endl;
      return;
    }
    const auto j_tasks = json::parse(tasks);
    for (auto t : j_tasks)
    {
      _task_queue.push_back(DTO::Task(t));
    }
  }

  void load_req_and_phase_from_etcd()
  {
    const auto reqs = _etcd_backup.get(BackupType::REQUESTS);
    if (reqs == "")
    {
      std::cout << "\n No requests in etcd" << std::endl;
      return;
    }
    const auto j_reqs = json::parse(reqs);
    if (j_reqs.size() > 0)
    {
      /// the rest are the request queue
      for (int i = 0; i < j_reqs.size(); i++)
      {
        _req_queue.push_back(DTO::Request(j_reqs[i]));
      }
    }
    const auto phase = _etcd_backup.get(BackupType::PHASE);
    _phase = (Phase)std::stoi(phase);
  }
};

/////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char **argv)
{
  std::cout << "Starting Master Node" << std::endl;

  /////// Get the environment variables ///////
  auto tmp = getenv("RPC_ADDRESS");
  if (tmp == NULL)
  {
    std::cout << "RPC_ADDRESS is not set" << std::endl;
    exit(1);
  }
  std::string rpc_address(tmp);

  tmp = getenv("ETCD_ADDRESS");
  if (tmp == NULL)
  {
    std::cout << "ETCD_ADDRESS is not set" << std::endl;
    exit(1);
  }
  std::string etcd_address(tmp);

  tmp = getenv("ETCD_ROOT_PASSWORD");
  if (tmp == NULL)
  {
    std::cout << "ETCD_ROOT_PASSWORD is not set" << std::endl;
    exit(1);
  }
  std::string etcd_password(tmp);
  std::cout << "Etcd address: " << etcd_address << " password: "
            << etcd_password << std::endl;

  tmp = getenv("BLOB_CONNECTION");
  if (tmp == NULL)
  {
    std::cout << "BLOB_CONNECTION is not set" << std::endl;
    exit(1);
  }
  const std::string connectionString(tmp);

  tmp = getenv("FAIL");
  if (tmp == NULL)
  {
    std::cout << "FAIL is not set" << std::endl;
    exit(1);
  }
  int fail = atoi(tmp);
  std::cout << "Fail: " << fail << std::endl;

  // NOTE: we use POD_IP as the master id
  tmp = getenv("POD_IP");
  std::string master_id;
  if (tmp == NULL)
  {
    srand(time(NULL));
    const int id = rand() % 1000;
    auto appId = std::to_string(id);
    std::cout << "POD_IP is not set, use generated ID: " 
              << appId << std::endl;
  }
  else
  {
    std::cout << "POD_IP: " << tmp << std::endl;
    master_id = tmp;
  }

  Master master(
      master_id,
      "50051",
      rpc_address,
      etcd_address,
      etcd_password,
      connectionString,
      fail);

  // start http (blocking) in another thread
  std::thread t_m(&Master::start_http_server_th, &master);

  // wait for task response
  std::thread t_t(&Master::wait_for_task_response_th, &master);

  // periodically ping workers and update tasks
  std::thread t_w(&Master::ping_workers_and_update_tasks_th, &master);

  // loop forever to process the request on main thread
  master.do_map_reduce();

  signal(SIGINT, signal_handler);
  while (1)
  {
    std::this_thread::sleep_for(1s);
  }
  return 0;
}
