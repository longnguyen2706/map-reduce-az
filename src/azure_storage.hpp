#ifndef MAP_REDUCE_AZURE_STORAGE_HPP
#define MAP_REDUCE_AZURE_STORAGE_HPP
#endif
// Example: https://github.com/Azure/azure-sdk-for-cpp/blob/azure-storage-files-shares_12.4.0/sdk/storage/azure-storage-blobs/samples/blob_query.cpp
#include <azure/storage/blobs.hpp>
#include <iostream>
#include <string>
#include <cmath>

using namespace Azure::Storage::Blobs;

// Tutorial is here: https://learn.microsoft.com/en-us/azure/storage/blobs/quickstart-blobs-c-plus-plus?tabs=environment-variable-windows
class AzureStorage
{
public:
  AzureStorage(const std::string &connection_string)
  {
    _connection_string = connection_string;
    // TODO: check if connection is valid
  }

  /// This is to upload a content to azure blob storage
  void upload_blob(
      const std::string &container_name,
      const std::string &blob_name,
      const std::string &blob_content)
  {
    using namespace Azure::Storage::Blobs;
    auto containerClient = BlobContainerClient::CreateFromConnectionString(
        _connection_string, container_name);
    containerClient.CreateIfNotExists();
    auto blobClient = containerClient.GetBlockBlobClient(blob_name);

    std::vector<uint8_t> buffer(blob_content.begin(), blob_content.end());
    blobClient.UploadFrom(buffer.data(), buffer.size());
    std::cout << "Uploaded blob " << blob_name << " to container " << container_name << std::endl;
  }

  /// This is to download a content from azure blob storage
  std::string download_blob(
      const std::string &container_name,
      const std::string &blob_name)
  {
    auto blobClient = get_blob_client(container_name, blob_name);

    auto properties = blobClient.GetProperties().Value;
    std::vector<uint8_t> downloadedBlob(properties.BlobSize);

    blobClient.DownloadTo(downloadedBlob.data(), downloadedBlob.size());
    std::string val = std::string(downloadedBlob.begin(), downloadedBlob.end());
    // std::cout << "Downloaded blob contents " <<  blob_name << std::endl;
    return val;
  }

  // get blob information
  uint64_t get_blob_size(
      const std::string &container_name,
      const std::string &blob_name)
  {
    auto containerClient = BlobContainerClient::CreateFromConnectionString(
        _connection_string, container_name);
    auto blockBlobClient = containerClient.GetBlockBlobClient(blob_name);
    auto properties = blockBlobClient.GetProperties().Value;
    std::cout << "The size of the blob is: " << properties.BlobSize << " bytes" << std::endl;
    return properties.BlobSize;
  }

  std::string download_blob_with_offset(const std::string &container_name,
                                        const std::string &blob_name, int start, int end)
  {
    auto blobClient = get_blob_client(container_name, blob_name);
    int segment_size = 1 * 1024; // 1 MB chunk
    int blob_length_remaining = end - start + 1;
    long start_position = start;

    // download file from azure blob storage with offset
    do
    {
      long block_size = std::min(segment_size, blob_length_remaining);
      auto result = download_blob_with_range(blobClient, block_size, start_position);
      start_position += block_size;
      blob_length_remaining -= block_size;

    } while (blob_length_remaining > 0);
    return "";
  }

  BlobContainerClient get_container_client(const std::string &container_name)
  {
    auto containerClient = BlobContainerClient::CreateFromConnectionString(
        _connection_string, container_name);
    return containerClient;
  }

  BlobClient get_blob_client(const std::string &container_name,
                             const std::string &blob_name)
  {
    auto containerClient = get_container_client(container_name);
    auto blobClient = containerClient.GetBlobClient(blob_name);
    return blobClient;
  }

  std::string download_blob_with_range(BlobClient blobClient, int block_size, int start){
    auto range = Azure::Core::Http::HttpRange();
    range.Offset = start;
    range.Length = block_size;
    auto Options = DownloadBlobToOptions();
    Options.Range = range;

    std::vector<uint8_t> downloadedBlob(block_size);

    blobClient.DownloadTo(downloadedBlob.data(), downloadedBlob.size(), Options);
    auto result =  std::string(downloadedBlob.begin(), downloadedBlob.end());
  
    return result;
  }


private:
  std::string _connection_string;
};
