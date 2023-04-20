#include <string>
#include <iostream>
#include <nlohmann/json.hpp>

using namespace std;

namespace DTO
{

/////////////////////////////////////////////////////////////////////////////////////////
class Request
{
public:
    std::string id;
    std::string input_dir;  // input directory
    std::string output_dir; // output directory

    std::string mapper_script;  // script name
    std::string reducer_script; // script name
    int num_mappers;
    int num_reducers;

    Request() = default;

    Request(const std::string &id, const std::string &input_dir,
            const std::string &output_dir, const std::string &mapper_script,
            const std::string &reducer_script, int num_mappers, int num_reducers)
        : id(id), input_dir(input_dir), output_dir(output_dir), mapper_script(mapper_script),
          reducer_script(reducer_script), num_mappers(num_mappers), num_reducers(num_reducers)
    {
    }

    /// from json
    Request(const nlohmann::json &j) {
        id = j.at("id").get<std::string>();
        input_dir = j.at("input_dir").get<std::string>();
        output_dir = j.at("output_dir").get<std::string>();
        mapper_script = j.at("mapper_script").get<std::string>();
        reducer_script = j.at("reducer_script").get<std::string>();
        num_mappers = j.at("num_mappers").get<int>();
        num_reducers = j.at("num_reducers").get<int>();
    }

    nlohmann::json to_json() {
        return nlohmann::json{{"id", id}, {"input_dir", input_dir}, {"output_dir", output_dir},
             {"mapper_script", mapper_script}, {"reducer_script", reducer_script},
                 {"num_mappers", num_mappers}, {"num_reducers", num_reducers}};
    }
};

/////////////////////////////////////////////////////////////////////////////////////////
class Task
{
public :
    int idx;
    std::string worker_id = "";
    int end_time = 0;

    Task() = default;

    Task(int idx) : idx(idx)
    {
    }

    /// from json
    Task(const nlohmann::json &j) {
        idx = j.at("idx").get<int>();
        worker_id = j.at("worker_id").get<std::string>();
        end_time = j.at("end_time").get<int>();
    }

    nlohmann::json to_json() {
        return nlohmann::json{{"idx", idx}, {"worker_id", worker_id}, {"end_time", end_time}};
    }
};

} // namespace DTO
