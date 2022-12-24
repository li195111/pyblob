import asyncio
import datetime
import logging
from typing import Any, List, Union

import azure.storage.blob as StorageSync
import azure.storage.blob.aio as StorageAsync
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.common import CloudStorageAccount

from .protocal import Protocal
from .utils import error_msg

logger = logging.getLogger('info-simple')


class BlobBase:

  def __init__(self,
               container_name: str,
               blob_name: str = None,
               account_name: str = None,
               account_key: str = None,
               connect_string: str = None,
               protocal: Protocal = Protocal.https,
               endpoint_suffix: str = "core.windows.net",
               host: str = None,
               blob_port: Union[str, int] = None,
               queue_port: Union[str, int] = None,
               table_port: Union[str, int] = None) -> None:
    if connect_string:
      self._connect_string = connect_string
    else:
      if protocal and account_name and account_key:
        self._protocol = protocal
        self._protocol_string = f"DefaultEndpointsProtocol={self._protocol.name}"

        self._account_name = account_name
        self._account_name_string = f"AccountName={self._account_name}"

        self._account_key = account_key
        self._account_key_string = f"AccountKey={self._account_key}"

        if host and (blob_port or queue_port or table_port):
          # local
          self._host = host
          self._blob_port = blob_port
          self._queue_port = queue_port
          self._table_port = table_port
        elif endpoint_suffix:
          # azure
          self._endpoint_suffix = endpoint_suffix
        else:
          raise TypeError(
              f"At least required argument: ['host', ('blob_port', 'queue_port', 'table_port')] or ['endpoint_suffix']"
          )
      else:
        raise TypeError(
            f"At least required argument: 'connect_string' or ['protocal', 'account_name', 'account_key']"
        )

      self.account_url = f"https://{self._account_name}.blob.{self._endpoint_suffix}"
      self.account_file_url = f"https://{self._account_name}.file.{self._endpoint_suffix}"
      self.account_queue_url = f"https://{self._account_name}.queue.{self._endpoint_suffix}"
      self.account_table_url = f"https://{self._account_name}.table.{self._endpoint_suffix}"
      self.account_web_url = f"https://{self._account_name}.z31.web.{self._endpoint_suffix}"

      _connect_string = [
          self._protocol_string, self._account_name_string,
          self._account_key_string
      ]

      if blob_port:
        self._blob_endpoint = f"{self._protocol.name}://{self._host}:{self._blob_port}/{self._account_name}"
        self._blob_endpoint_string = f"BlobEndpoint={self._blob_endpoint}"
        _connect_string.append(self._blob_endpoint_string)
      if queue_port:
        self._queue_endpoint = f"{self._protocol.name}://{self._host}:{self._queue_port}/{self._account_name}"
        self._queue_endpoint_string = f"QueueEndpoint={self._queue_endpoint}"
        _connect_string.append(self._queue_endpoint_string)
      if table_port:
        self._table_endpoint = f"{self._protocol.name}://{self._host}:{self._table_port}/{self._account_name}"
        self._table_endpoint_string = f"TableEndpoint={self._table_endpoint}"
        _connect_string.append(self._table_endpoint_string)
      if endpoint_suffix:
        self._endpoint_suffix_string = f"EndpointSuffix={endpoint_suffix}"
        _connect_string.append(self._endpoint_suffix_string)

      self._connect_string = ";".join(_connect_string)

    self._container_name = container_name
    self._blob_name = blob_name

  @property
  def conn_str(self):
    return self._connect_string

  @property
  def container_name(self):
    return self._container_name

  @container_name.setter
  def container_name(self, container_name):
    self._container_name = container_name

  @property
  def blob_name(self):
    return self._blob_name

  @blob_name.setter
  def blob_name(self, blob_name):
    self._blob_name = blob_name

  def get_sas_token(self,
                    blob_name,
                    expiry=datetime.datetime.utcnow() +
                    datetime.timedelta(hours=1)):
    return StorageSync.generate_blob_sas(
        account_name=self._account_name,
        account_key=self._account_key,
        container_name=self.container_name,
        blob_name=blob_name,
        permission=StorageSync.BlobSasPermissions(read=True),
        expiry=expiry)


class BlobAsync(BlobBase):

  def __init__(self,
               container_name: str,
               blob_name: str = None,
               connect_string: str = None,
               protocal: Protocal = Protocal.https,
               host: str = None,
               account_name: str = None,
               account_key: str = None,
               blob_port: Union[str, int] = None,
               queue_port: Union[str, int] = None,
               table_port: Union[str, int] = None,
               **kwargs) -> None:
    super().__init__(container_name=container_name,
                     blob_name=blob_name,
                     account_name=account_name,
                     account_key=account_key,
                     connect_string=connect_string,
                     protocal=protocal,
                     host=host,
                     blob_port=blob_port,
                     queue_port=queue_port,
                     table_port=table_port,
                     **kwargs)

  @property
  def _service_client(self) -> StorageAsync.BlobServiceClient:
    return StorageAsync.BlobServiceClient(account_url=self.account_url,
                                          credential=self._account_key)

  @property
  def _container_client(self) -> StorageAsync.ContainerClient:
    return StorageAsync.ContainerClient(account_url=self.account_url,
                                        container_name=self.container_name,
                                        credential=self._account_key)

  @property
  def _blob_client(self) -> StorageAsync.BlobClient:
    return StorageAsync.BlobClient(account_url=self.account_url,
                                   container_name=self.container_name,
                                   blob_name=self.blob_name,
                                   credential=self._account_key)

  async def list_container(self, *args, **kwargs) -> list:
    containers_list = []
    async with self._service_client as service_client:
      async for container in service_client.list_containers(*args, **kwargs):
        containers_list.append(container.name)
    return containers_list

  async def create_container(self, *args, **kwargs):
    async with self._container_client as container_client:
      await container_client.create_container(*args, **kwargs)

  async def delete_container(self, *args, **kwargs):
    async with self._container_client as container_client:
      return await container_client.delete_container(*args, **kwargs)

  async def list_blobs(self, *args, **kwargs) -> list:
    blobs_list = []
    async with self._container_client as container_client:
      async for blob in container_client.list_blobs(*args, **kwargs):
        blobs_list.append(blob)
    return blobs_list

  async def walk_blobs(self, *args, **kwargs) -> List:
    blobs_list = []
    async with self._container_client as container_client:
      async for blob in container_client.walk_blobs(*args, **kwargs):
        blobs_list.append(blob)
    return blobs_list

  @property
  async def blob_url(self):
    async with self._blob_client as blob_client:
      return blob_client.url

  async def blob_properties(self, blob_name, *args, **kwargs):
    self.blob_name = blob_name
    async with self._blob_client as blob_client:
      return await blob_client.get_blob_properties(*args, **kwargs)

  async def upload_blob(self, blob_name: str, data: Union[str, bytes], *args,
                        **kwargs) -> None:
    self.blob_name = blob_name
    async with self._blob_client as blob_client:
      return await blob_client.upload_blob(data, *args, **kwargs)

  async def get_blob(self, blob_name: str, *args,
                     **kwargs) -> StorageAsync.StorageStreamDownloader:
    self.blob_name = blob_name
    async with self._blob_client as blob_client:
      return await blob_client.download_blob(*args, **kwargs)

  async def read_blob(self, blob_name: str, *args, **kwargs):
    self.blob_name = blob_name
    async with self._blob_client as blob_client:
      storageStream: StorageAsync.StorageStreamDownloader = await blob_client.download_blob(
          *args, **kwargs)
      return await storageStream.readall()

  async def delete_blob(self, blob_name: str, *args, **kwargs) -> None:
    self.blob_name = blob_name
    async with self._blob_client as blob_client:
      return await blob_client.delete_blob(*args, **kwargs)


class BlobSync(BlobBase):

  def __init__(self,
               container_name: str,
               blob_name: str = None,
               connect_string: str = None,
               protocal: Protocal = Protocal.https,
               host: str = None,
               account_name: str = None,
               account_key: str = None,
               blob_port: Union[str, int] = None,
               queue_port: Union[str, int] = None,
               table_port: Union[str, int] = None,
               **kwargs) -> None:
    super(BlobSync, self).__init__(container_name=container_name,
                                   blob_name=blob_name,
                                   account_name=account_name,
                                   account_key=account_key,
                                   connect_string=connect_string,
                                   protocal=protocal,
                                   host=host,
                                   blob_port=blob_port,
                                   queue_port=queue_port,
                                   table_port=table_port,
                                   **kwargs)

  @property
  def _service_client(self) -> StorageSync.BlobServiceClient:
    return StorageSync.BlobServiceClient(account_url=self.account_url,
                                         credential=self._account_key)

  @property
  def _container_client(self) -> StorageSync.ContainerClient:
    return StorageSync.ContainerClient(account_url=self.account_url,
                                       container_name=self.container_name,
                                       credential=self._account_key)

  @property
  def _blob_client(self) -> StorageSync.BlobClient:
    return StorageSync.BlobClient(account_url=self.account_url,
                                  container_name=self.container_name,
                                  blob_name=self.blob_name,
                                  credential=self._account_key)

  def list_container(self, *args, **kwargs) -> list:
    containers_list = []
    with self._service_client as service_client:
      for container in service_client.list_containers(*args, **kwargs):
        containers_list.append(container.name)
    return containers_list

  def create_container(self, *args, **kwargs) -> Union[dict, None]:
    with self._container_client as container_client:
      container_client.create_container(*args, **kwargs)

  def delete_container(self, *args, **kwargs) -> None:
    with self._container_client as container_client:
      return container_client.delete_container(*args, **kwargs)

  def list_blobs(self, *args, **kwargs) -> List[str]:
    blobs_list = []
    with self._container_client as container_client:
      for blob in container_client.list_blobs(*args, **kwargs):
        blobs_list.append(blob)
    return blobs_list

  def walk_blobs(self, *args, **kwargs) -> List:
    blobs_list = []
    with self._container_client as container_client:
      for blob in container_client.walk_blobs(*args, **kwargs):
        blobs_list.append(blob)
    return blobs_list

  @property
  def blob_url(self):
    with self._blob_client as blob_client:
      return blob_client.url

  def blob_properties(self, blob_name, *args,
                      **kwargs) -> StorageSync.BlobProperties:
    self.blob_name = blob_name
    with self._blob_client as blob_client:
      return blob_client.get_blob_properties(*args, **kwargs)

  def upload_blob(self, blob_name: str, data: Union[str, bytes], *args,
                  **kwargs) -> Any:
    self.blob_name = blob_name
    with self._blob_client as blob_client:
      return blob_client.upload_blob(data, *args, **kwargs)

  def get_blob(self, blob_name: str, *args,
               **kwargs) -> StorageSync.StorageStreamDownloader:
    self.blob_name = blob_name
    with self._blob_client as blob_client:
      return blob_client.download_blob(*args, **kwargs)

  def read_blob(self, blob_name: str, *args, **kwargs):
    self.blob_name = blob_name
    with self._blob_client as blob_client:
      storageStream: StorageSync.StorageStreamDownloader = blob_client.download_blob(
          *args, **kwargs)
      return storageStream.readall()

  def delete_blob(self, blob_name: str) -> None:
    self.blob_name = blob_name
    with self._blob_client as blob_client:
      return blob_client.delete_blob()


class Blob:

  def __init__(self,
               container_name: str,
               blob_name: str = None,
               isasync: bool = False,
               account_name: str = None,
               account_key: str = None,
               connect_string: str = None,
               protocal: Protocal = Protocal.https,
               host: str = None,
               blob_port: Union[str, int] = None,
               queue_port: Union[str, int] = None,
               table_port: Union[str, int] = None,
               timeout: int = None,
               *args,
               **kwargs) -> None:
    if isasync:
      _blob = BlobAsync
    else:
      _blob = BlobSync
    self.blob = _blob(container_name=container_name,
                      blob_name=blob_name,
                      account_name=account_name,
                      account_key=account_key,
                      connect_string=connect_string,
                      protocal=protocal,
                      host=host,
                      blob_port=blob_port,
                      queue_port=queue_port,
                      table_port=table_port,
                      *args,
                      **kwargs)
    self.isasync = isasync
    self.blob.container_name = container_name
    self.timeout = timeout

  @property
  def container_name(self):
    return self.blob.container_name

  @container_name.setter
  def container_name(self, container_name):
    self.blob.container_name = container_name

  @property
  def container_url(self):
    return self.blob._container_client.url

  @property
  def blob_name(self):
    return self.blob.blob_name

  @blob_name.setter
  def blob_name(self, blob_name):
    self.blob.blob_name = blob_name

  @property
  def container_exists(self):
    logger.info(f"Check Container Exists ... {self.container_name}")
    if self.isasync:
      container_list = self.run_async(self.blob.list_container,
                                      **{'timeout': self.timeout})
    else:
      container_list = self.blob.list_container(**{'timeout': self.timeout})
    return self.container_name in container_list

  @property
  def container_url(self):
    return f"{self.blob.account_url}/{self.container_name}"

  @property
  def blob_exists(self):
    logger.info(f"Check Blob Exists ... {self.blob_name}")
    if self.isasync:
      return self.blob_name in self.run_async(self.blob.list_blobs,
                                              **{'timeout': self.timeout})
    return self.blob_name in self.blob.list_blobs(**{'timeout': self.timeout})

  @property
  def blobs(self):
    logger.info(f"Get Blobs ...")
    if self.isasync:
      return self.run_async(self.blob.list_blobs, **{'timeout': self.timeout})
    return self.blob.list_blobs(**{'timeout': self.timeout})

  @property
  def blob_url(self):
    try:
      return f"{self.blob.account_url}/{self.container_name}/{self.blob_name}"
    except Exception as err:
      error_msg(err)

  def get_sas_token(self,
                    blob_name,
                    expiry=datetime.datetime.utcnow() +
                    datetime.timedelta(hours=1)):
    return self.blob.get_sas_token(blob_name, expiry=expiry)

  def walk_blobs(self, *args, **kwargs):
    try:
      logger.info(f"Walk Blob ... {args[0]}")
      kwargs.update({'timeout': self.timeout})
      raw = kwargs.get('raw')
      if self.isasync and not raw:
        if raw:
          kwargs.pop('raw')
        return self.run_async(self.blob.walk_blobs, *args, **kwargs)
      kwargs.pop('raw')
      return self.blob.walk_blobs(*args, **kwargs)
    except Exception as err:
      error_msg(err)

  def blob_properties(self, blob_name: str, *args, **kwargs):
    try:
      logger.info(f"Properties ... {blob_name}")
      kwargs.update({'timeout': self.timeout})
      if self.isasync and not kwargs.get('raw'):
        kwargs.pop('raw')
        return self.run_async(self.blob.blob_properties, blob_name, *args,
                              **kwargs)
      kwargs.pop('raw')
      return self.blob.blob_properties(blob_name, *args, **kwargs)
    except Exception as err:
      error_msg(err)

  def create_container(self, *args, **kwargs):
    try:
      logger.info(f"Create Container ... {self.container_name}")
      kwargs.update({'timeout': self.timeout})
      if self.isasync and not kwargs.get('raw'):
        kwargs.pop('raw')
        return self.run_async(self.blob.create_container, *args, **kwargs)
      kwargs.pop('raw')
      return self.blob.create_container(*args, **kwargs)
    except ResourceExistsError:
      logger.info(f"Container Exists ... {self.container_name}")
    except Exception as err:
      error_msg(err)

  def delete_container(self, *args, **kwargs):
    try:
      logger.info(f"Deleting container ... {self.container_name}")
      kwargs.update({'timeout': self.timeout})
      if self.isasync and not kwargs.get('raw'):
        kwargs.pop('raw')
        return self.run_async(self.blob.delete_container, *args, **kwargs)
      kwargs.pop('raw')
      return self.blob.delete_container(*args, **kwargs)
    except ResourceNotFoundError:
      logger.info(f"Container Not Found ... {self.container_name}")
    except Exception as err:
      error_msg(err)

  def upload_blob(self, blob_name: str, data: Union[str, bytes], *args,
                  **kwargs):
    try:
      logger.info(f"Upload ... {blob_name}")
      kwargs.update({
          'timeout': self.timeout,
      })
      if self.isasync and not kwargs.get('raw'):
        kwargs.pop('raw')
        return self.run_async(self.blob.upload_blob, blob_name, data, *args,
                              **kwargs)
      kwargs.pop('raw')
      return self.blob.upload_blob(blob_name=blob_name,
                                   data=data,
                                   *args,
                                   **kwargs)
    except ResourceNotFoundError:
      logger.info(f"Container Not Found ... {self.container_name}")
      try:
        self.create_container()
        kwargs.update({
            'timeout': self.timeout,
        })
        return self.upload_blob(blob_name=blob_name,
                                data=data,
                                *args,
                                **kwargs)
      except ResourceExistsError:
        logger.info(
            "The specified container is being deleted. Try operation later.")
    except ResourceExistsError:
      logger.info("Blob Exists. to overwrite please use 'overwrite=True'")
    except Exception as err:
      error_msg(err)

  def download_blob(self, blob_name: str, *args, **kwargs):
    try:
      logger.info(f"Download ... {blob_name}")
      kwargs.update({'timeout': self.timeout})
      if self.isasync and not kwargs.get('raw'):
        kwargs.pop('raw')
        return self.run_async(self.blob.get_blob,
                              blob_name=blob_name,
                              *args,
                              **kwargs)
      kwargs.pop('raw')
      return self.blob.get_blob(blob_name=blob_name, *args, **kwargs)
    except Exception as err:
      error_msg(err)

  def read_blob(self, blob_name: str, *args, **kwargs):
    try:
      logger.info(f"Read ... {blob_name}")
      kwargs.update({'timeout': self.timeout})
      if self.isasync and not kwargs.get('raw'):
        kwargs.pop('raw')
        return self.run_async(self.blob.read_blob,
                              blob_name=blob_name,
                              *args,
                              **kwargs)
      kwargs.pop('raw')
      return self.blob.read_blob(blob_name=blob_name, *args, **kwargs)
    except Exception as err:
      error_msg(err)

  def delete_blob(self, blob_name: str, *args, **kwargs):
    try:
      logger.info(f"Delete ... {blob_name}")
      kwargs.update({'timeout': self.timeout})
      if self.isasync and not kwargs.get('raw'):
        kwargs.pop('raw')
        return self.run_async(self.blob.delete_blob, blob_name, *args,
                              **kwargs)
      kwargs.pop('raw')
      return self.blob.delete_blob(blob_name=blob_name, *args, **kwargs)
    except ResourceNotFoundError:
      logger.info(f"Blob not exists... {self.container_name}")
    except Exception as err:
      error_msg(err)

  def run_async(self, func, *args, **kwargs):
    return asyncio.new_event_loop().run_until_complete(func(*args, **kwargs))
