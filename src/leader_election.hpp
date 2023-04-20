#ifndef MAP_REDUCE__LEADER_ELECTION__HPP
#define MAP_REDUCE__LEADER_ELECTION__HPP

// include etcd client
#include <iostream>
#include <memory>
#include <string>
#include <chrono>

#include "etcd/Client.hpp"
#include "etcd/KeepAlive.hpp"
#include "etcd/Watcher.hpp"

////////////////////////////////////////////////////////////////////////////////////////

std::string ELECTIONKEY = "App/leader";

class LeaderElection
{
public:
  /*
   * This class handles the leader election process
   * @param etcdConnectionString - connection string for etcd
   * @param id - unique id for this instance
   * @param leaderChangeCallback - callback function when iam the leader
   */
  LeaderElection(
      const std::string etcdConnectionString,
      const std::string root_password,
      const std::string id,
      std::function<void(std::string)> leaderChangeCallback)
      : m_id(std::move(id)), m_leaderId("")
  {
    m_leaderChangeCallback = std::move(leaderChangeCallback);

    if (root_password.empty())
    {
      m_etcd = std::make_shared<etcd::Client>(etcdConnectionString);
      std::cout << "Using etcd without authentication" << std::endl;
    }
    else
    {
      m_etcd = std::make_shared<etcd::Client>(
          etcdConnectionString, "root", root_password);
      std::cout << "Using etcd with authentication" << std::endl;
    }

    auto callback = [&](etcd::Response response)
    { this->WatchForLeaderChangeCallback(response); };
    m_watcher = std::make_unique<etcd::Watcher>(*m_etcd, ELECTIONKEY, callback);
  }

  /*
   * This is the main function to start the leader election process
   */
  void StartElection()
  {
    int numberOfTries = 10;
    while (numberOfTries--)
    {
      pplx::task<etcd::Response> response_task =
          m_etcd->add(ELECTIONKEY, GetID(), GetKeepAlive()->Lease());
      try
      {
        etcd::Response response = response_task.get();

        if (response.is_ok())
        {
          // if able to add the key that means this is the new leader
          m_leaderChangeCallback(GetID());
        }

        // capture the current leader (stored as value of the ELECTIONKEY)
        // in case of is_ok() -> False, this returns the existing key's value
        m_leaderId = response.value().as_string();
        std::cout << "Leader is now: " << m_leaderId << std::endl;
        break;
      }
      catch (std::exception const &ex)
      {
        std::cerr << ex.what();
        if (numberOfTries == 0)
          throw ex;
      }
    }
  }

  ~LeaderElection()
  {
    // we should cancel the watcher first so that deletion which happens
    // as part of keepalive cancel does not trigger the watcher callback
    if (m_watcher.get() != nullptr)
    {
      m_watcher->Cancel();
    }

    if (m_keepalive.get() != nullptr)
    {
      m_keepalive->Cancel();
    }
  }

private:
  bool isLeader()
  {
    return m_id == m_leaderId;
  }

  std::string &GetID()
  {
    return m_id;
  }

  std::shared_ptr<etcd::KeepAlive> GetKeepAlive()
  {
    if (m_keepalive.get() == nullptr)
    {
      // 1 secs is the lease keep alive time
      m_keepalive = m_etcd->leasekeepalive(1).get();
    }
    return m_keepalive;
  }

  // This will be called when the leader changes
  void WatchForLeaderChangeCallback(etcd::Response response)
  {
    if (response.action() == "delete")
    {
      m_leaderId = "";
      StartElection();
    }
    else if (response.action() == "set")
    {
      m_leaderId = response.value().as_string();
    }
  }

  std::string m_id;
  std::string m_leaderId;
  std::shared_ptr<etcd::Client> m_etcd;
  std::shared_ptr<etcd::KeepAlive> m_keepalive;
  std::unique_ptr<etcd::Watcher> m_watcher;
  std::function<void(std::string)> m_leaderChangeCallback;
};


#endif // MAP_REDUCE__LEADER_ELECTION__HPP
