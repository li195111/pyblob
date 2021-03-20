import asyncio
from typing import List, Tuple, Union

import azure.storage.blob as StorageSync
import azure.storage.blob.aio as StorageAsync
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from pyblob.protocal import Protocal
from pyblob.utils import error_msg


class BlobBase:
    def __init__(self, connect_string: str = None, protocal: Protocal = Protocal.https, host: str = None, account_name: str = None, account_key: str = None,
                 endpoint_suffix: str = "core.windows.net",
                 blob_port: Union[str, int] = None, queue_port: Union[str, int] = None, table_port: Union[str, int] = None) -> None:
        if connect_string:
            self.connect_string = connect_string
        else:
            if protocal and account_name and account_key:
                self.protocol = protocal
                self.protocol_string = f"DefaultEndpointsProtocol={self.protocol.name}"

                self.account_name = account_name
                self.account_name_string = f"AccountName={self.account_name}"

                self.account_key = account_key
                self.account_key_string = f"AccountKey={self.account_key}"

                if host and (blob_port or queue_port or table_port):
                    # local
                    self.host = host
                    self.blob_port = blob_port
                    self.queue_port = queue_port
                    self.table_port = table_port
                elif endpoint_suffix:
                    # azure
                    self.endpoint_suffix = endpoint_suffix
                else:
                    raise TypeError(
                        f"At least required argument: ['host', ('blob_port', 'queue_port', 'table_port')] or ['endpoint_suffix']")
            else:
                raise TypeError(
                    f"At least required argument: 'connect_string' or ['protocal', 'account_name', 'account_key']")

            self.account_url = f"https://{self.account_name}.blob.{self.endpoint_suffix}"
            self.account_file_url = f"https://{self.account_name}.file.{self.endpoint_suffix}"
            self.account_queue_url = f"https://{self.account_name}.queue.{self.endpoint_suffix}"
            self.account_table_url = f"https://{self.account_name}.table.{self.endpoint_suffix}"
            self.account_web_url = f"https://{self.account_name}.z31.web.{self.endpoint_suffix}"

            connect_string = [self.protocol_string,
                              self.account_name_string,
                              self.account_key_string]

            if blob_port:
                self.blob_endpoint = f"{self.protocol.name}://{self.host}:{self.blob_port}/{self.account_name}"
                self.blob_endpoint_string = f"BlobEndpoint={self.blob_endpoint}"
                connect_string.append(self.blob_endpoint_string)
            if queue_port:
                self.queue_endpoint = f"{self.protocol.name}://{self.host}:{self.queue_port}/{self.account_name}"
                self.queue_endpoint_string = f"QueueEndpoint={self.queue_endpoint}"
                connect_string.append(self.queue_endpoint_string)
            if table_port:
                self.table_endpoint = f"{self.protocol.name}://{self.host}:{self.table_port}/{self.account_name}"
                self.table_endpoint_string = f"TableEndpoint={self.table_endpoint}"
                connect_string.append(self.table_endpoint_string)
            if endpoint_suffix:
                self.endpoint_suffix_string = f"EndpointSuffix={endpoint_suffix}"
                connect_string.append(self.endpoint_suffix_string)

            self.connect_string = ";".join(connect_string)

        self.container_client = None
        self.__container_name = None

    @property
    def container_name(self):
        return self.__container_name

    @container_name.setter
    def container_name(self, container_name):
        self.__container_name = container_name

    def _blob_service_client(self):
        return StorageSync.BlobServiceClient.from_connection_string(conn_str=self.connect_string)


class BlobAsync(BlobBase):
    def __init__(self, connect_string: str = None, protocal: Protocal = Protocal.https, host: str = None, account_name: str = None, account_key: str = None,
                 blob_port: Union[str, int] = None, queue_port: Union[str, int] = None, table_port: Union[str, int] = None, **kwargs) -> None:
        super(BlobAsync, self).__init__(connect_string=connect_string, protocal=protocal, host=host, blob_port=blob_port,
                                        queue_port=queue_port, table_port=table_port, account_name=account_name, account_key=account_key, **kwargs)

    async def list_container(self) -> list:
        containers_list = []
        async with StorageAsync.BlobServiceClient.from_connection_string(conn_str=self.connect_string) as service_client:
            async for container in service_client.list_containers():
                containers_list.append(container.name)
        return containers_list

    async def create_container(self):
        try:
            print(f"Create Container ... {self.container_name}")
            async with StorageAsync.ContainerClient.from_connection_string(conn_str=self.connect_string,
                                                                           container_name=self.container_name) as container_client:
                await container_client.create_container()
            print(f"Create Container ... {self.container_name} ... Success")
        except ResourceExistsError:
            print(f"Container Exists ... {self.container_name}")
        except Exception as err:
            print(f"Create Container ... {self.container_name} ... Failed")
            error_msg(err)

    async def delete_container(self):
        try:
            print(f"Deleting blob container... {self.container_name}")
            async with StorageAsync.ContainerClient.from_connection_string(conn_str=self.connect_string,
                                                                           container_name=self.container_name) as container_client:
                await container_client.delete_container()
        except ResourceNotFoundError:
            print(f"Container Not Found... {self.container_name}")

    async def list_blobs(self):
        blobs_list = []
        async with StorageAsync.ContainerClient.from_connection_string(conn_str=self.connect_string,
                                                                       container_name=self.container_name) as container_client:
            async for blob in container_client.list_blobs():
                blobs_list.append(blob)
        return blobs_list

    async def upload_blob(self, blob_name: str, data: Union[str, bytes]) -> None:
        try:
            print(f"Upload Blob ... {blob_name}")
            async with StorageAsync.BlobClient.from_connection_string(conn_str=self.connect_string,
                                                                      container_name=self.container_name,
                                                                      blob_name=blob_name) as blob_client:
                await blob_client.upload_blob(data)
        except ResourceNotFoundError:
            print(f"Blob not exists... {self.container_name}")
            async with StorageAsync.ContainerClient.from_connection_string(conn_str=self.connect_string,
                                                                           container_name=self.container_name) as container_client:
                await container_client.create_container()
            async with StorageAsync.BlobClient.from_connection_string(conn_str=self.connect_string,
                                                                      container_name=self.container_name,
                                                                      blob_name=blob_name) as blob_client:
                await blob_client.upload_blob(data)
        except Exception as err:
            error_msg(err)

    async def get_blob(self, blob_name: str):
        try:
            async with StorageAsync.BlobClient.from_connection_string(conn_str=self.connect_string,
                                                                      container_name=self.container_name,
                                                                      blob_name=blob_name) as blob_client:
                return await blob_client.download_blob().readall()
        except Exception as err:
            error_msg(err)

    async def delete_blob(self, blob_name: str):
        try:
            async with StorageAsync.BlobClient.from_connection_string(conn_str=self.connect_string,
                                                                      container_name=self.container_name,
                                                                      blob_name=blob_name) as blob_client:
                return await blob_client.delete_blob()
        except Exception as err:
            error_msg(err)


class BlobSync(BlobBase):
    def __init__(self, connect_string: str = None, protocal: Protocal = Protocal.https, host: str = None, account_name: str = None, account_key: str = None,
                 blob_port: Union[str, int] = None, queue_port: Union[str, int] = None, table_port: Union[str, int] = None, **kwargs) -> None:
        super(BlobSync, self).__init__(connect_string=connect_string, protocal=protocal, host=host, blob_port=blob_port,
                                       queue_port=queue_port, table_port=table_port, account_name=account_name, account_key=account_key, **kwargs)

    def list_container(self) -> list:
        containers_list = []
        with StorageSync.BlobServiceClient.from_connection_string(conn_str=self.connect_string) as service_client:
            for container in service_client.list_containers():
                containers_list.append(container.name)
        return containers_list

    def create_container(self):
        try:
            with StorageSync.ContainerClient.from_connection_string(conn_str=self.connect_string,
                                                                    container_name=self.container_name) as container_client:
                container_client.create_container()
            print(f"Create Container Success!... {self.container_name}")
        except ResourceExistsError:
            print(f"Container Exists... {self.container_name}")
        except Exception as err:
            error_msg(err)

    def delete_container(self):
        try:
            with StorageSync.ContainerClient.from_connection_string(conn_str=self.connect_string,
                                                                    container_name=self.container_name) as container_client:
                container_client.delete_container()
            print(f"Deleting blob container... {self.container_name}")
        except ResourceNotFoundError:
            print(f"Container Not Found... {self.container_name}")

    def list_blobs(self) -> Tuple[List[str], List[str]]:
        blobs_list = []
        with StorageSync.ContainerClient.from_connection_string(conn_str=self.connect_string,
                                                                container_name=self.container_name) as container_client:
            for blob in container_client.list_blobs():
                blobs_list.append(blob)
        return blobs_list

    def upload_blob(self, blob_name: str, data: Union[str, bytes]) -> None:
        with StorageSync.BlobClient.from_connection_string(conn_str=self.connect_string,
                                                           container_name=self.container_name,
                                                           blob_name=blob_name) as blob_client:
            blob_client.upload_blob(data)

    def get_blob(self, blob_name: str) -> Union[str, bytes]:
        try:
            with StorageSync.BlobClient.from_connection_string(conn_str=self.connect_string,
                                                               container_name=self.container_name,
                                                               blob_name=blob_name) as blob_client:
                return blob_client.download_blob().readall()
        except Exception as err:
            error_msg(err)

    def delete_blob(self, blob_name: str):
        try:
            with StorageSync.BlobClient.from_connection_string(conn_str=self.connect_string,
                                                               container_name=self.container_name,
                                                               blob_name=blob_name) as blob_client:
                return blob_client.delete_blob()
        except Exception as err:
            error_msg(err)


class Blob:
    def __init__(self, connect_string: str = None, protocal: Protocal = Protocal.https, host: str = None, account_name: str = None, account_key: str = None,
                 blob_port: Union[str, int] = None, queue_port: Union[str, int] = None, table_port: Union[str, int] = None, container_name: str = None, isasync: bool = False, *args, **kwargs) -> None:
        self.isasync = isasync
        if self.isasync:
            self.blob = BlobAsync(connect_string=connect_string, protocal=protocal, host=host, blob_port=blob_port,
                                  queue_port=queue_port, table_port=table_port, account_name=account_name, account_key=account_key, *args, **kwargs)
        else:
            self.blob = BlobSync(connect_string=connect_string, protocal=protocal, host=host, blob_port=blob_port,
                                 queue_port=queue_port, table_port=table_port, account_name=account_name, account_key=account_key, *args, **kwargs)

    @property
    def container_name(self):
        return self.blob.container_name

    @property
    def container_exists(self):
        if self.isasync:
            return self.blob.list_container()
        return self.container_name in self.blob.list_container()

    @container_name.setter
    def container_name(self, container_name):
        self.blob.container_name = container_name

    def create_container(self):
        if self.container_name:
            self.blob.create_container()
        else:
            ValueError("Pleas assign a container name")

    def delete_container(self):
        if self.container_name:
            self.blob.delete_container()
        else:
            ValueError("Pleas assign a container name")

    def upload_blob(self, blob_name: str, data: Union[str, bytes]):
        return self.blob.upload_blob(blob_name=blob_name, data=data)

    def download_blob(self, blob_name: str):
        return self.blob.get_blob(blob_name=blob_name)

    def delete_blob(self, blob_name: str):
        return self.blob.delete_blob(blob_name=blob_name)

    def run_async(self, func, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(func(*args, **kwargs))
        return
