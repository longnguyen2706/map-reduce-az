
#ifndef MAP_REDUCE__HTTP_SERVER_HPP
#define MAP_REDUCE__HTTP_SERVER_HPP

#include <iostream>
#include <memory>
#include <string>
#include <chrono>

// header only http lib https://github.com/yhirose/cpp-httplib
#include "httplib.h"

////////////////////////////////////////////////////////////////////////////////////////

class HttpServer
{
public:
  // Create a http server instance
  HttpServer(const std::string &address, const std::string &port);

  // Register a callback for /request
  void register_req_callback(
    const std::function<std::string(const std::string &)> &req_callback)
  {
    _req_callback = std::move(req_callback);
  }

  // Register a callback for /tasks
  void register_tasks_callback(
    const std::function<std::string(const std::string &)> &tasks_callback)
  {
    _tasks_callback = std::move(tasks_callback);
  }

  // Register a callback for /kill
  void register_kill_callback(const std::function<void()> &kill_callback)
  {
    _kill_callback = std::move(kill_callback);
  }

  void start()
  {
    _server.listen(_address.c_str(), std::stoi(_port));
  }

private:
  std::string _address;
  std::string _port;
  httplib::Server _server;
  std::function<std::string(const std::string &)> _req_callback;
  std::function<std::string(const std::string &)> _tasks_callback;
  std::function<void()> _kill_callback;
};

////////////////////////////////////////////////////////////////////////////////////////

HttpServer::HttpServer(
  const std::string &address,
  const std::string &port
) : _address(address), _port(port)
{
  _server.Post(
      "/request",
      [&](const httplib::Request &req, httplib::Response &res)
      {
        std::cout << "Got a request" << std::endl;
        if (_req_callback)
        {
          const auto res_s = _req_callback(req.body);
          res.set_content(res_s, "text/plain");
        }
        else
          res.set_content("Impl not avail, pls check", "text/plain");
      });

  /// THIS IS FOR TESTING
  _server.Get(
      "/test",
      [&](const httplib::Request &req, httplib::Response &res)
      {
        std::cout << "Got a status request" << std::endl;
        res.set_content("OK bastard! 😈😈 ", "text/plain");
      });

  /// THIS IS FOR TESTING
  _server.Get(
      "/kill",
      [&](const httplib::Request &req, httplib::Response &res)
      {
        std::cout << "Got a kill msg! DIE!!" << std::endl;
        if (_kill_callback)
          _kill_callback();
      });

  _server.Get(
    "/tasks",
      [&](const httplib::Request &req, httplib::Response &res)
      {
        if (_tasks_callback)
        {
          const auto res_s = _tasks_callback(req.body);
          res.set_content(res_s, "text/plain");
        }
        else
          res.set_content("Task callback not avail, pls check", "text/plain");
      }
  );
}

#endif // MAP_REDUCE__HTTP_SERVER_HPP
