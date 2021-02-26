import sys, traceback

from enum import Enum
from typing import List, Tuple, Union
import asyncio

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.storage.blob.aio import BlobServiceClient as BlobServiceClientAsync
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

def error_msg(err, show_details=True):
    error_class = err.__class__.__name__
    if len(err.args) > 0: detail = err.args[0]
    else: detail = ''
    cl, exc, tb = sys.exc_info()
    details = '\n'.join([f"File \"{s[0]}\", line {s[1]} in {s[2]}" for s in traceback.extract_tb(tb)])
    errMsg = f"\n[{error_class}] {detail}"
    if show_details: print(details, errMsg)
    else: print (errMsg)
    
class Protocal(Enum):
    http = 0
    https = 1

class BlobBase:
    def __init__(self, protocal:Protocal, host:str, blob_port:Union[str,int], queue_port:Union[str,int],
                 account_name:str, account_key:str,**kwargs) -> None:
        self.protocol = protocal
        self.protocol_string = f"DefaultEndpointsProtocol={self.protocol.name}"
        
        self.host = host
        self.blob_port = blob_port
        self.queue_port = queue_port
        
        self.account_name = account_name
        self.account_name_string = f"AccountName={self.account_name}"
        
        self.account_key = account_key
        self.account_key_string = f"AccountKey={self.account_key}"
        
        self.blob_endpoint = f"{self.protocol.name}://{self.host}:{self.blob_port}/{self.account_name}"
        self.blob_endpoint_string = f"BlobEndpoint={self.blob_endpoint}"
        
        self.queue_endpoint = f"{self.protocol.name}://{self.host}:{self.queue_port}/devstoreaccount1"
        self.queue_endpoint_string = f"QueueEndpoint={self.queue_endpoint}"
        
        connect_string = [self.protocol_string,
                          self.account_name_string,
                          self.account_key_string,
                          self.blob_endpoint_string,
                          self.queue_endpoint_string]
        
        self.connect_string = ";".join(connect_string)

        self.container_client = None
        self.container_name = ""
        self.container_exist = False
        
        self.service_client = None
        
    def init_container_service_client(self, container_name:str):
        print (f"Connect to Blob Service... {container_name}")
        if self.service_client:
            self.blob_service_client = self.service_client.from_connection_string(self.connect_string)
        else:
            raise ValueError("'service_client' is not defined. Please Check it is Sync or Async and use BlobServiceClient[Async]")
        self.container_name = container_name
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    def __get_blob_client(self, blob_name:str) -> BlobClient:
        return self.container_client.get_blob_client(blob_name)
    
class BlobAsync(BlobBase):
    def __init__(self, **kwargs) -> None:
        super(BlobAsync, self).__init__(**kwargs)
        self.service_client = BlobServiceClientAsync
        
    async def list_container_async(self) -> list:
        containers_list = []
        async with self.blob_service_client:
            async for container in self.blob_service_client.list_containers():
                containers_list.append(container)
            return containers_list
        
    async def create_container_async(self) -> ContainerClient:
        try:
            await self.container_client.create_container()
            print (f"Create Container Success!... {self.container_name}")
        except ResourceExistsError:
            print (f"Container Exists... {self.container_name}")
            self.container_exist = True
        except Exception as err:
            error_msg(err)
            
    async def list_blobs_async(self):
        blobs_list = []
        async for blob in self.container_client.list_blobs():
            blobs_list.append(blob)
        return blobs_list
    
    async def upload_blob_async(self, data, blob_name:str)->None:
        try:
            await self.__get_blob_client(blob_name).upload_blob(data)
        except ResourceNotFoundError:
            print (f"Blob not exists... {self.container_name}")
        except Exception as err:
            error_msg(err)
            
    async def download_blob_async(self, blob_name:str, download_file_path:str):
        try:
            with open(download_file_path, "w") as download_file:
                stream = await self.__get_blob_client(blob_name).download_blob()
                data = await stream.readall()
                download_file.write(data)
            return download_file_path
        except Exception as err:
            error_msg(err)
            
    async def delete_container_async(self):
        try:
            await self.container_client.delete_container()
            print(f"Deleting blob container... {self.container_name}")
        except ResourceNotFoundError:
            print (f"Container Not Found... {self.container_name}")

class BlobSync(BlobBase):
    def __init__(self, **kwargs) -> None:
        super(BlobSync, self).__init__(**kwargs)
        self.service_client = BlobServiceClient
                
    def list_container(self) -> list:
        containers_list = []
        for container in self.blob_service_client.list_containers():
            containers_list.append(container)
        return containers_list
    
    def create_container(self):
        try:
            self.container_client.create_container()
            print (f"Create Container Success!... {self.container_name}")
        except ResourceExistsError as ExitsErr:
            print (f"Container Exists... {self.container_name}")
            self.container_exist = True
        except Exception as err:
            error_msg(err)
            
    def list_blobs(self) -> Tuple[List[str],List[str]]:
        blobs_list = []
        for blob in self.container_client.list_blobs():
            blobs_list.append(blob)
        return blobs_list
    
    def upload_blob(self, data, blob_name:str)->None:
        self.__get_blob_client(blob_name).upload_blob(data)

    def read_blob(self, blob_name:str) -> Union[str,bytes]:
        try:
            stream = self.__get_blob_client(blob_name).download_blob()
            return stream.readall()
        except Exception as err:
            error_msg(err)
            
    def download_blob(self, blob_name:str, download_file_path:str):
        try:
            with open(download_file_path, "w") as download_file:
                stream = self.__get_blob_client(blob_name).download_blob()
                data = stream.readall()
                download_file.write(data)
            return download_file_path
        except Exception as err:
            error_msg(err)
            
    def delete_container(self):
        try:
            self.container_client.delete_container()
            print(f"Deleting blob container... {self.container_name}")
        except ResourceNotFoundError:
            print (f"Container Not Found... {self.container_name}")

class Blob:
    def __init__(self, container_name:str, isasync:bool=False, **kwargs) -> None:
        if isasync:
            self.blob = BlobAsync(**kwargs)
        else:
            self.blob = BlobSync(**kwargs)        
        self.blob.init_container_service_client(container_name)
        self.__dict__.update(self.blob.__dict__)
    
    def run_async(self, func, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(func(*args,**kwargs))
        return 
    
if __name__ == "__main__":
    pass