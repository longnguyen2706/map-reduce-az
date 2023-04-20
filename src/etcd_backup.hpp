#ifndef MAP_REDUCE__ETCD_BACKUP__HPP
#define MAP_REDUCE__ETCD_BACKUP__HPP

// include etcd client
#include <iostream>
#include <memory>
#include <string>
#include <chrono>

#include "etcd/Client.hpp"
#include "etcd/KeepAlive.hpp"
#include "etcd/Watcher.hpp"

////////////////////////////////////////////////////////////////////////////////////////

enum class BackupType
{
  TASKS,
  REQUESTS, // the first one is the current request
  PHASE,
  LEADER
};

////////////////////////////////////////////////////////////////////////////////////////

class EtcdBackup
{
public:
  EtcdBackup(
    const std::string etcd_conn_string,
    const std::string root_password = "")
  : _etcd_conn_string(etcd_conn_string),
    _root_password(root_password)
  {
    if (root_password.empty())
    {
      m_etcd = std::make_shared<etcd::Client>(etcd_conn_string);
      std::cout << "Using etcd without authentication IP:" 
                << etcd_conn_string << std::endl;
    }
    else
    {
      m_etcd = std::make_shared<etcd::Client>(
          etcd_conn_string, "root", root_password);
      std::cout << "Using etcd with authentication" << std::endl;
    } 
  }

  bool put(const std::string& value, const BackupType type)
  {
    // backup current phase (default is map)
    /// backup a list of task :  current_task + task_queue
    // 
    pplx::task<etcd::Response> response_task =
      m_etcd->put(type_convert(type), value);
    etcd::Response response = response_task.get(); // can throw
    if (response.is_ok())
      return true;
    else
      std::cout << "operation failed, details: " << response.error_message();
    return false;
  }

  std::string get(const BackupType type)
  {
    pplx::task<etcd::Response> response_task = m_etcd->get(type_convert(type));
    try
    {
      etcd::Response response = response_task.get(); // can throw
      if (response.is_ok())
      {
        std::cout << "successful read" << std::endl;
        return response.value().as_string();
      }
      else
        std::cout << "operation failed, details: " << response.error_message();
    }
    catch (std::exception const & ex)
    {
      std::cerr << "communication problem, details: " << ex.what();
    }
    return "";
  }

private:
  std::string _etcd_conn_string;
  std::string _root_password;
  std::shared_ptr<etcd::Client> m_etcd;

  std::string type_convert(const BackupType type)
  {
    switch (type)
    {
      case BackupType::TASKS:
        return "App/ETCD_BACKUP_tasks";
      case BackupType::REQUESTS:
        return "App/ETCD_BACKUP_requests";
      case BackupType::PHASE:
        return "App/ETCD_BACKUP_phase";
      case BackupType::LEADER:
        return "App/leader";
      default:
        return "unknown";
    }
  }
};

#endif // MAP_REDUCE__ETCD_BACKUP__HPP
