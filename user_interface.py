
# Reference : https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
import os
from time import sleep
from azure.storage.blob import BlobServiceClient

class UserInterface(object):

    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # --Begin Blob Samples-----------------------------------------------------------------

    def create_container(self, container_name):

        # Instantiate a new BlobServiceClient using a connection string

        blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string)

        # Instantiate a new ContainerClient
        container_client = blob_service_client.get_container_client(
            container_name)

        try:
            # # Create new container in the service
            container_client.create_container()

            # List containers in the storage account
            list_response = blob_service_client.list_containers()

            # Print the container names
            print("List containers in the storage account...")
            for container in list_response:
                print(container.name)
        except Exception as e:
            print("Error creating container", e)

    def delete_container(self, container_name):

        # Instantiate a new BlobServiceClient using a connection string
        blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string)

        # Instantiate a new ContainerClient
        container_client = blob_service_client.get_container_client(
            container_name)

        # Delete the container
        container_client.delete_container()

    def upload_files(self, container_name, folder_name):

        # read file name in the folder
        file_list = os.listdir(folder_name)

        # Instantiate a new BlobServiceClient using a connection string
        blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string)

        # Instantiate a new ContainerClient
        container_client = blob_service_client.get_container_client(
            container_name)
        try:
            for file_name in file_list:
                # Instantiate a new BlobClient
                blob_client = container_client.get_blob_client(file_name)

                # [START upload_a_blob]
                # Upload content to block blob
                file_path = os.path.join(folder_name, file_name)
                print("\tUploading to Azure Storage as blob:\t" + file_name)
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, blob_type="BlockBlob")
                # [END upload_a_blob]

        except Exception as e:
            print("Error creating container", e)

    def reset_result_folders(self):
        try:
            # print("Deleting dfs-intermediate folders")
            # self.delete_container("dfs-intermediate")
            # sleep(5)
            print("Creating dfs-intermediate folders")
            self.create_container("dfs-intermediate")
            sleep(5)
      
        except Exception as e:
            print(e)
            
        
        try:
            # print("Deleting dfs-dest folders")
            # self.delete_container("dfs-dest")
            # sleep(5)
            print("Creating dfs-dest folders")
            self.create_container("dfs-dest")
            sleep(5)
      
        except Exception as e:
            print(e)
            

    def reset_source_folders(self):
        try:
            print("Deleting dfs-source-small folders")
            self.delete_container("dfs-source-small")
            sleep(5)
            print("Deleting dfs-source-large folders")
            self.delete_container("dfs-source-large")
            sleep(5)
        except: 
            pass

        try:
            print("Creating dfs-source-small folders")
            self.create_container("dfs-source-small")
            sleep(1)
            print("Creating dfs-source-large folders")
            self.create_container("dfs-source-large")
            sleep(1)
        except:
            pass
        print("Uploading data to dfs-source-small and dfs-source-large")
        self.upload_files("dfs-source-small", "../data/small")
        self.upload_files("dfs-source-large", "../data/large")

    def reset_script_folders(self):
        # try:
        #     self.delete_container("dfs-scripts")
        #     sleep(2)
        # except:
        #     pass
        self.create_container("dfs-scripts")
        sleep(1)
        self.upload_files("dfs-scripts", "py_scripts")

    def clean_all(self):
        self.reset_result_folders()
        self.reset_source_folders()
        self.reset_script_folders()


if __name__ == '__main__':
    ui = UserInterface()
    ui.reset_result_folders() # clean result
    # ui.reset_source_folders() # clean source
    # ui.reset_script_folders() # clean script
