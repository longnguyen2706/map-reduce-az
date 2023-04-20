#ifndef MAP_REDUCE_FUNCTION_HELPER_HPP
#define MAP_REDUCE_FUNCTION_HELPER_HPP

#include <iostream>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sstream>
#include <azure/storage/blobs.hpp>
#include "mrmsg.grpc.pb.h"
#include <fstream>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <iterator>
#include <chrono>
#include <boost/algorithm/string.hpp>

using namespace std::chrono;

using mrmsg::File;
using mrmsg::MapRequest;
using mrmsg::MrResponse;
using mrmsg::MrStatus;
using mrmsg::MrStatus_descriptor;
using mrmsg::ReduceRequest;
using mrmsg::TaskStatus;
using mrmsg::TaskStatus_descriptor;
using mrmsg::WorkerInfo;
using mrmsg::WorkerStatus;
using mrmsg::WorkerStatus_descriptor;

namespace FH
{

////////////////////////////////////////////////////////////////////////////////////////
int TASK_TIMEOUT = 240000; // 4 mins
int PING_TIMEOUT = 5000;   // 5 seconds
int CHECK_INTERVAL = 1000; // 1 second
int PING_INTERVAL = 10000; // 10 second
int OFFSET = 20;

////////////////////////////////////////////////////////////////////////////////////////
std::array<std::string, 3> special_strings = {
    " ",  // space
    "\n", // newline
    "\t"  // tab
};

std::string execute_python(const char *script_path, const std::string& input)
{
    // std::cout << "Executing python script: " << script_path 
    // << " input size: " << input.size() << std::endl;
    int pipe_in[2];
    int pipe_out[2];

    if (pipe(pipe_in) == -1 || pipe(pipe_out) == -1)
    {
        std::cerr << "Failed to create pipe" << std::endl;
        exit(1);
    }
    // pipe in: parent write, child read
    // pipe out: parent read, child write
    // idx 1 to write, idx 0 to read
    pid_t pid = fork();
    if (pid == -1)
    {
        std::cerr << "Failed to fork" << std::endl;
        exit(1);
    }
    else if (pid == 0) // Child process
    {
        close(pipe_out[0]);
        close(pipe_in[1]);

        dup2(pipe_out[1], STDOUT_FILENO); // Redirect stdout to the pipe
        close(pipe_out[1]);

        dup2(pipe_in[0], STDIN_FILENO); // Redirect stdin from the pipe
        close(pipe_in[0]);

        // std::cout << "About to execute python script" << std::endl;
        execl("/usr/bin/python3", "python3", script_path, nullptr); // Execute the Python script
        // std::cout << "After execute python script" << std::endl;
        std::cerr << "Failed to execute the Python script" << std::endl;
        exit(1);
    }
    else // Parent process
    {
        close(pipe_in[0]);
        close(pipe_out[1]);

        write(pipe_in[1], input.c_str(), input.size());
        close(pipe_in[1]);

        std::stringstream output;
        char buf[1024 * 64];
        ssize_t n;
        while ((n = read(pipe_out[0], buf, sizeof(buf))) > 0)
        { // Read output from the child process
            output.write(buf, n);
        }

        close(pipe_out[0]); // Close the read end of the pipe

        int status;
        // std::cout << "Waiting child to finish : "<< std::endl;
        waitpid(pid, &status, 0); // Wait for the child process to finish

        if (WIFEXITED(status))
        {
            // std::cout << "Child process exited with status: " << WEXITSTATUS(status) << std::endl;
        }
        else
        {
            std::cerr << "Child process did not exit properly" << std::endl;
            exit(1);
        }
        return output.str();
    }
}

// seek last occurence of sub in str
int seek_last(std::string &str, std::string &sub)
{
    int pos = str.rfind(sub);
    if (pos != std::string::npos)
    {
        return pos;
    }
    return -1;
}

// find first index of special character in string by loading extra characters before string
// and find last special character in prefix
int find_begin(std::string &result)
{
    int b;

    // searching for the last space in the prefix

    auto prefix = result.substr(0, OFFSET);
    for (auto special_string : special_strings)
    {
        b = seek_last(prefix, special_string);
        if (b != -1)
        {
            break;
        }
    }
    b == -1 ? prefix.size() - 1 : b;

    return b;
}

// find last index of special character in string
int find_end(std::string &result)
{
    int e;
    // searching for the last special character in string

    for (auto special_string : special_strings)
    {
        e = seek_last(result, special_string);
        if (e != -1)
        {
            break;
        }
    }
    e == -1 ? result.size() - 1 : e;

    return e;
}

// seek first occurence of sub in str
int seek_first(std::string &str, std::string &sub)
{
    int pos = str.find(sub);
    if (pos != std::string::npos)
    {
        return pos;
    }
    return -1;
}

std::vector<Models::BlobItem> getBlobsFromFolder(
    AzureStorage azure_storage, std::string input_folder)
{
    std::cout << "Listing blobs..." << std::endl;
    auto containerClient = azure_storage.get_container_client(input_folder);
    auto listBlobsResponse = containerClient.ListBlobs();
    return listBlobsResponse.Blobs;
}

File create_file_info(
    const std::string &name, int start, int end, int file_size)
{
    File file;
    file.set_file_name(name);
    file.set_start(start);
    file.set_end(end);
    file.set_file_len(file_size);
    return file;
}

// shard files into m_num shards
// this code needs refactoring, but it works
std::map<int, std::vector<File>> shard_for_mapper(
    const std::vector<Models::BlobItem> &blobItems, int m_num)
{
    // map of idx to files
    std::map<int, std::vector<File>> shard_map;

    // get file sizes and file names inside the folder
    std::vector<int> file_sizes;
    std::vector<std::string> file_names;
    int total_size = 0;
    for (const auto item : blobItems)
    {
        const int blobSize = item.BlobSize;
        file_sizes.push_back(blobSize);
        file_names.push_back(item.Name);
        total_size += blobSize;
    }

    // calculate the shard size
    int avg_shard_size = std::floor((float)total_size / m_num);
    std::cout << "Total size: " << total_size << " Shard size: " << avg_shard_size << std::endl;

    // calculate the shard map
    int f_s = 0;
    int i = 0;
    int f_idx = 0;
    int cur_size = 0;
    int total_left = total_size;

    while (i < m_num)
    {
        int shard_size = std::min(avg_shard_size, total_left);
        std::cout << "Creating shard " << i << std::endl;
        cur_size = 0;
        std::vector<File> files;
        // left over from previous shard
        if (f_s > 0)
        {
            int size = std::min(file_sizes[f_idx] - f_s, shard_size);
            int end = size + f_s - 1;
            File file = create_file_info(file_names[f_idx], f_s, end, file_sizes[f_idx]);
            files.push_back(file);
            cur_size += size;
            f_s = end + 1; // start from the next byte
            if (end == file_sizes[f_idx] - 1 && f_idx < file_sizes.size() - 1)
            {
                f_idx++;
                f_s = 0; // start from the beginning of the next file
            }
        }

        while (cur_size + file_sizes[f_idx] <= shard_size)
        {
            cur_size += file_sizes[f_idx]; // append whole file
            File file = create_file_info(
                file_names[f_idx], f_s, file_sizes[f_idx] - 1, file_sizes[f_idx]);
            files.push_back(file);
            if (f_idx < file_sizes.size() - 1)
            {
                f_idx++;
                f_s = 0; // start from the beginning of the next file
            }
        }
        int left = shard_size - cur_size;
        if (left > 0 && f_s <= left)
        {
            File file = create_file_info(file_names[f_idx], f_s, left, file_sizes[f_idx]);
            files.push_back(file);
            f_s = left + 1;
        }

        shard_map[i] = files;
        i++;
        total_left -= shard_size;
    }

    return shard_map;
}

// since in map phase, we generate M*R files, so we just need to give every shard M files
std::map<int, std::vector<File>> shard_for_reducer(
    const std::vector<Models::BlobItem> &blobItems, int r_num)
{
    std::map<int, std::vector<File>> shard_map;
    for (int i = 0; i < r_num; i++)
    {
        std::vector<File> files;
        for (int j = 0; j < blobItems.size(); j++)
        {
            if (j % r_num == i)
            {
                File file = create_file_info(
                    blobItems[j].Name, 0, blobItems[j].BlobSize - 1, blobItems[j].BlobSize);
                files.push_back(file);
            }
        }
        shard_map[i] = files;
    }
    return shard_map;
}

// print the shard map
void print_shard_map(const std::map<int, std::vector<File>> &shard_map)
{
    for (auto it = shard_map.begin(); it != shard_map.end(); ++it)
    {

        int size = 0;
        for (auto file : it->second)
        {
            std::cout << "File: " << file.file_name() << " Start: " 
                    << file.start() << " End: " << file.end() << std::endl;
            size += file.end() - file.start() + 1;
        }
        std::cout << "Shard " << it->first << " Shard size: " << size << std::endl;
        std::cout << "---------------------" << std::endl;
    }
}

void print_blob_info(const std::vector<Models::BlobItem> &blobItems)
{
    for (auto item : blobItems)
    {
        std::cout << "Blob name: " << item.Name 
                    << " Blob size: " << item.BlobSize << std::endl;
    }
    std::cout << "---------------------" << std::endl;
}

// reference: https://bikulov.org/blog/2013/11/19/sort-strings-from-file-in-c-/
void sort(const std::string &input_file, const std::string &output_file)
{
    std::ifstream fin(input_file);
    std::vector<std::string> array;

    while (true)
    {
        std::string s;
        getline(fin, s);
        if (fin.eof())
        {
            break;
        }
        array.push_back(s);
        // std::cout << s << std::endl;
    }
    fin.close();

    std::sort(array.begin(), array.end());

    std::ofstream fout(output_file);
    std::copy(array.begin(), array.end(), std::ostream_iterator<std::string>(fout, "\n"));
    fout.close();
}

MapRequest create_map_request(const std::string &id,
                                const std::string &input_folder,
                                const std::string &output_folder,
                                const std::string &script_name,
                                int num_reducers,
                                std::vector<File> files)
{
    MapRequest request;
    request.set_id(id); // server id
    request.set_input_dir(input_folder);
    request.set_output_dir(output_folder);
    request.set_script_name(script_name);
    request.set_num_reducers(num_reducers);
    for (auto file : files)
    {
        request.add_files()->CopyFrom(file);
    }

    return request;
}

ReduceRequest create_reduce_request(const std::string &id,
                                    const std::string &input_folder,
                                    const std::string &output_folder,
                                    const std::string &script_name,
                                    int num_reducers,
                                    std::vector<File> files)
{
    // Data we are sending to the server.
    ReduceRequest request;
    request.set_id(id); // server id
    request.set_input_dir(input_folder);
    request.set_output_dir(output_folder);
    request.set_script_name(script_name);
    for (auto file : files)
    {
        request.add_files()->CopyFrom(file);
    }

    return request;
}

int get_current_time_s()
{
    uint64_t sec = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    // std::cout << "Current time: " << sec << std::endl;
    return sec;
}

std::string generate_task_id(std::string input_dir, int task_idx, bool is_mapper)
{
    if (is_mapper)
    {
        return "m_" + input_dir + "_" + std::to_string(task_idx);
    }
    else
    {
        return "r_" + input_dir + "_" + std::to_string(task_idx);
    }
}

int task_id_to_idx(std::string task_id)
{
    std::vector<std::string> strs;
    boost::split(strs, task_id, boost::is_any_of("_"));
    return std::stoi(strs[2]);
}

void populate_mr_response(MrResponse *response, std::string id, MrStatus status)
{
    response->set_status(status);
    response->set_id(id);
}

TaskStatus convert_mr_response_status(MrStatus status)
{
    if (status == MrStatus::SUCCESS)
    {
        return TaskStatus::COMPLETED;
    }
    else
    {
        return TaskStatus::FAILED;
    }
}

// print protobuf enum name from value
// https://stackoverflow.com/questions/1420426/how-to-print-enum-names-instead-of-integers
std::string print_enum_name(
    int value, const google::protobuf::EnumDescriptor *descriptor)
{
    const google::protobuf::EnumValueDescriptor *enum_descriptor =
        descriptor->FindValueByNumber(value);
    if (enum_descriptor != nullptr)
    {
        return enum_descriptor->name();
    }
    else
    {
        return "Unknown";
    }
}

std::string print_task_status(TaskStatus status)
{
    return print_enum_name(status, TaskStatus_descriptor());
}

std::string print_mr_status(MrStatus status)
{
    return print_enum_name(status, MrStatus_descriptor());
}

std::string print_worker_status(WorkerStatus status)
{
    return print_enum_name(status, WorkerStatus_descriptor());
}

} // namespace FH

#endif // MAP_REDUCE_FUNCTION_HELPER_HPP
