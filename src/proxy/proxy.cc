// include etcd client
#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include "../etcd_backup.hpp"
#include "../http_server.hpp"

#include "../httplib.h"

////////////////////////////////////////////////////////////////////////////////////////
class ProxyServer
{

public:
  ProxyServer(const bool use_etcd = false,
              const std::string etcd_conn_string = "TODO",
              const std::string root_password = "")
  {
    if (use_etcd)
    {
      _etcd_backup =
          std::make_shared<EtcdBackup>(etcd_conn_string + ":2379", root_password);
      std::cout << "etcd done" << std::endl;
    }

    _http_server = std::make_shared<HttpServer>("0.0.0.0", "8080");
    _http_server->register_req_callback(
        [this](const std::string &body)
        {
          if (_etcd_backup)
            _target_server_add = _etcd_backup->get(BackupType::LEADER);

          httplib::Client cli(_target_server_add, 8080);
          std::cout << "Sending request to " << _target_server_add << std::endl;
          // make this fault tolerant
          return manage_requests(cli.Post("/request", body, "application/json"));
        });

    _http_server->register_tasks_callback(
        [this](const std::string &body)
        {
          if (_etcd_backup)
            _target_server_add = _etcd_backup->get(BackupType::LEADER);
          std::cout << "sending request to " << _target_server_add << std::endl;
          httplib::Client cli(_target_server_add, 8080);
          return manage_requests(cli.Get("/tasks"));
        });

    _http_server->register_kill_callback(
        [this]()
        {
          if (_etcd_backup)
            _target_server_add = _etcd_backup->get(BackupType::LEADER);

          std::cout << "Sending request to " << _target_server_add << std::endl;
          httplib::Client cli(_target_server_add, 8080);
          try
          {
            manage_requests(cli.Get("/kill"));
          }
          catch(const std::exception& e)
          {
            std::cerr << e.what() << '\n';
          }
          return;
        });
  }

  void start()
  {
    _http_server->start();
  }

private:
  std::shared_ptr<HttpServer> _http_server;
  std::shared_ptr<EtcdBackup> _etcd_backup;
  std::string _target_server_add = "TODO";

  std::string manage_requests(httplib::Result res)
  {
    if (res)
    {
      if (res->status == 200)
      {
        std::cout << res->body << std::endl;
        return res->body;
      }
      return "HTTP error, status code: " + std::to_string(res->status);
    }
    else
    {
      auto err = res.error();
      std::cout << "HTTP error: " << httplib::to_string(err) << std::endl;
      return "HTTP error: " + httplib::to_string(err);
    }
  }
};

////////////////////////////////////////////////////////////////////////////////////////
int main(int argc, char **argv)
{
  auto tmp = getenv("ETCD_ADDRESS");
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

  ProxyServer proxy(
      true,
      etcd_address,
      etcd_password);
  std::cout << "Starting proxy server" << std::endl;
  proxy.start();
}
