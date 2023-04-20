#include <iostream>
#include <memory>
#include <string>
#include <chrono>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

// include etcd client
#include "etcd/Client.hpp"
#include "etcd/KeepAlive.hpp"
#include "etcd/Watcher.hpp"

#include <glog/logging.h>
#include <fstream>

#include "../azure_storage.hpp"
#include "../function_helper.hpp"
#include "mrmsg.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using mrmsg::File;
using mrmsg::MrService;
using mrmsg::MapRequest;
using mrmsg::MrResponse;
using mrmsg::PingWorkerRequest;
using mrmsg::PingWorkerResponse;
using mrmsg::ReduceRequest;

/////////////////////////////////////////////////////////////////////////////////////
std::string SCRIPT_DIR = "dfs-scripts";

// This is the impl for the callback
class GreeterServiceImpl final : public MrService::Service
{
public:
  // Consructor
  explicit GreeterServiceImpl(const std::string &azure_blob_conn, int fail)
      : _azure_storage(azure_blob_conn)
  {
    _azure_blob_conn = azure_blob_conn;
    _fail = fail;
  }

private:
  // Handle map request
  Status MapperAssign(ServerContext *context, const MapRequest *request,
                      MrResponse *reply) override
  {
    std::cout << "Mapper Req for server " << request->id()
              << "  ;read from: " << request->input_dir()
              << "   ; write from: " << request->output_dir()
              << "   ; script name: " << request->script_name()
              << std::endl;
    _task_count++;
    // download script from azure blob storage
    if (_fail> 0 && _task_count>= _fail){
            std::cout<<"Force Fail the map task"<<std::endl;
        FH::populate_mr_response(reply, request-> id(), MrStatus::FAILURE );
        return Status::CANCELLED;
    }
    
    try
    {
      auto script_file_path = download_script(SCRIPT_DIR, request->script_name());

      // create a temporary outfile
      std::ofstream outfile;
      std::string outfile_name = request->input_dir() + request->id() + "mapper_output.txt";
      outfile.open(outfile_name);

      // download file from azure blob storage
      for (File file : request->files())
      {
        std::cout << "file name: " << file.file_name() << " file len: "
                  << file.file_len() << " start: " << file.start() << " end: " << file.end() << std::endl;
        auto blobClient = _azure_storage.get_blob_client(request->input_dir(), file.file_name());
        int start = file.start();
        int end = file.end();
        int file_len = file.file_len();
        // we will read from start - OFFSET to end. need to handle the case when start == 0
        long s_pos;
        // read with offset
        if (start == 0 || start - FH::OFFSET < 0)
        {
          s_pos = start;
        }
        else
        {
          s_pos = start - FH::OFFSET;
        }
        int len_remain = end - s_pos + 1;

        // download file from azure blob storage with offset
        auto result = _azure_storage.download_blob_with_range(blobClient, len_remain, s_pos);
        std::cout<<"Dowloaded file size: "<<result.length()<<std::endl;
        int b = start == 0 ? 0 : FH::find_begin(result);
        int e = end == (file_len - 1) ? (file_len - 1) : FH::find_end(result);
        // std::cout <<"b: "<< b << " e: "<< e;
        auto content = result.substr(b, e - b);
        // std::cout<<"content len: "<<content.length()<<std::endl;
        std::stringstream out;
        try
        {
          // do the map here
           out = execute_python_script(script_file_path.c_str(), content);
        }
        catch (std::exception &e)
        {
          std::cout << "Exception when executing python script: " << e.what() << std::endl;
        }

        outfile << out.rdbuf();
      }
      outfile.close();

      // std::cout << "C++ output: " << out_buffer.str() << std::endl;
      // TODO: sort the result by key
      FH::sort(outfile_name, outfile_name + "_sorted.txt");
      std::stringstream out_buffer;
      std::ifstream infile;
      infile.open(outfile_name + "_sorted.txt");
      out_buffer << infile.rdbuf();
      infile.close();
      // delete the temp file
      std::remove(outfile_name.c_str());
      std::remove((outfile_name + "_sorted.txt").c_str());

      // split to r_num equal parts and upload to azure blob storage
      int r_num = request->num_reducers();
      int file_len = out_buffer.str().length();
      int part_len = file_len / r_num;
      int start = 0;
      int end = part_len - 1;
      for (int i = 0; i < r_num; i++)
      {
        // find the end of the line
        while (out_buffer.str()[end] != '\n')
        {
          end++;
        }
        // upload the part to azure blob storage
        std::stringstream ss;
        ss << out_buffer.str().substr(start, end - start + 1);
        _azure_storage.upload_blob(
            request->output_dir(),
            request->id() + "_" + std::to_string(i) + "output" + std::to_string(i) + ".txt",
            ss.str());
        start = end + 1;
        end = start + part_len - 1;
      }
        FH::populate_mr_response(reply, request-> id(), MrStatus::SUCCESS);

      return Status::OK;
    }
    catch (std::exception &e)
    {
      std::cout << "Exception when doing mapper: " << e.what() << std::endl;
        FH::populate_mr_response(reply, request-> id(), MrStatus::FAILURE );
      return Status::CANCELLED;
    }
  }

  // Handle reduce request
  Status ReducerAssign(ServerContext *context, const ReduceRequest *request,
                       MrResponse *reply) override
  {
    _task_count++;
    // artificial failed the call if _fail setting is not 0 and this worker already process enough map task
    if (_fail> 0 && _task_count>= _fail){
        std::cout<<"Force Fail the reduce task"<<std::endl;
        FH::populate_mr_response(reply, request-> id(), MrStatus::FAILURE );

        return Status::CANCELLED;
    }
    std::cout << "Reduce Req for server " << request->id()
              << "  ;read from: " << request->input_dir()
              << "   ; write from: " << request->output_dir()
              << std::endl;
    try
    {
      // download script from azure blob storage
      auto script_file_path = download_script(SCRIPT_DIR, request->script_name());

      std::stringstream out_buffer;
      for (File file : request->files())
      {
        std::cout << "file name: " << file.file_name() << " file len: "
                  << file.file_len() << " start: " << file.start() 
                  << " end: " << file.end() << std::endl;

        auto content = _azure_storage.download_blob(request->input_dir(), file.file_name());
        // do the reduce for the file
        std::string out = FH::execute_python(request->script_name().c_str(), content);
        out_buffer << out;
      }

      // do reduce one more time on the out_buffer
      std::string result = FH::execute_python(request->script_name().c_str(), out_buffer.str());

      // upload the result to azure blob storage
      std::cout << "Uploading reducer result " << request->id() << std::endl;
      _azure_storage.upload_blob(
          request->output_dir(),
          request->id() + "result.txt",
          result);
      FH::populate_mr_response(reply, request-> id(), MrStatus::SUCCESS);
      return Status::OK;
    }
    catch (std::exception &e)
    {
      std::cout << "Exception when doing reducer: " << e.what() << std::endl;
       FH::populate_mr_response(reply, request-> id(), MrStatus::FAILURE);
      return Status::CANCELLED;
    }
  }

  Status PingWorker(ServerContext *context, const PingWorkerRequest *request,
                    PingWorkerResponse *reply) override
  { 

    std::cout << "Ping to worker " << request->id() << std::endl;
    if (_fail> 0 && _task_count>= _fail){
        std::cout<<"Force Ping Fail"<<std::endl;
        return Status::CANCELLED;
    }

    reply->set_id(request->id());
    reply->set_status(WorkerStatus::HEALTHY);

    return Status::OK;
  }

  std::string download_script(
    const std::string& container_name, const std::string& script_name)
  {
    auto content = _azure_storage.download_blob(container_name, script_name);
    // save to file
    std::ofstream outfile;
    std::cout << "dowloading script name: " << script_name << std::endl;
    outfile.open(script_name);
    outfile << content;
    outfile.close();
    return script_name;
  }

  std::stringstream execute_python_script(
    const char *script_name, std::string content)
  {
    std::stringstream out_buffer;
    // split content into parts of 32KB
    int part_size = 1024 * 64;
    int part_num = content.size() / part_size;
    // std::cout << "part_num: " << part_num << std::endl;
    int part_remain = content.size() % part_size;
    int part_start = 0;
    int part_end = part_size;
    for (int i = 0; i < part_num; i++)
    {
      auto part = content.substr(part_start, part_size);                                 // rough cut
      int e = part_end == content.size()-1 ? part.size()-1 : FH::find_end(part); // find the real end without cutting words
    
      part = part.substr(0, e);
      part_start += part.size();
      part_end += part.size();
      // do the map here
      // std::cout << "start: " << part_start << " end: " << part_end << " "
      //           << "size: " << part.size() << std::endl;
      std::string pout = FH::execute_python(script_name, part);
      // std::cout << "-----------------------" << std::endl;
      // std::cout << pout << std::endl;
      out_buffer << pout;
    }
    if (part_remain > 0)
    {
      auto part = content.substr(part_start, part_remain);
      std::string pout = FH::execute_python(script_name, part);
      // std::cout << pout << std::endl;
      out_buffer << pout;
    }
    return out_buffer;
  }

private:
  std::string _azure_blob_conn;
  AzureStorage _azure_storage;
  bool _is_mapper;
  // for artificial failure
  int _fail;
  int _task_count = 0;
};

/////////////////////////////////////////////////////////////////////////////////////
// Logic and data behind the server's behavior.

class Worker
{
public:
  /*
  This wil init the worker node, with provided RPC server add and
  azure blob connection string, and whether it is a mapper or reducer
  */
  Worker(
      const std::string &rpc_server_add,
      const std::string &azure_blob_conn, 
      int fail)
  {
    //// Register RPC server
    std::cout << "Starting server at " << rpc_server_add << std::endl;
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();

    // Listen on the given address without any authentication mechanism.
    ServerBuilder builder;
    builder.AddListeningPort(rpc_server_add, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.

    _service = std::make_shared<GreeterServiceImpl>(azure_blob_conn, fail);
    builder.RegisterService(_service.get());
    _server = builder.BuildAndStart();
    std::cout << "Server listening on " << rpc_server_add << std::endl;
  }

  /// This is a blocking wait
  void wait()
  {
    _server->Wait();
  }

private:
  std::unique_ptr<Server> _server;
  std::shared_ptr<GreeterServiceImpl> _service;
  int task_processed = 0;
};

/////////////////////////////////////////////////////////////////////////////////////

void RunServer()
{
  /////// get env variables ///////
  auto tmp = getenv("RPC_ADDRESS");
  if (tmp == NULL)
  {
    std::cout << "RPC_ADDRESS is not set"
              << std::endl;
    exit(1);
  }
  const std::string server_address(tmp);

  tmp = getenv("RPC_PORT");
  if (tmp == NULL)
  {
    std::cout << "RPC_PORT is not set" << std::endl;
    exit(1);
  }
  std::string rpc_port(tmp);

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
  const int fail = atoi(tmp);
  std::cout << "FAIL: " << fail << std::endl;

  // Start the server.
  auto w = Worker(server_address + ":" + rpc_port, connectionString, fail);
  w.wait();

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  std::cout << "Done" << std::endl;
}

/////////////////////////////////////////////////////////////////////////////////////
int main(int argc, char **argv)
{
  google::SetLogDestination(google::GLOG_INFO, "/");
  google::InitGoogleLogging(argv[0]);
  std::cout << "Starting Worker Node" << std::endl;
  RunServer();
  return 0;
}
