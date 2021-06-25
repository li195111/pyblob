import mimetypes
from tempfile import SpooledTemporaryFile
from typing import Any, List, Optional, Tuple

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import ContentSettings
from django.conf import settings
from django.contrib.staticfiles.storage import StaticFilesStorage
from django.core.exceptions import SuspiciousOperation
from django.core.files.base import File

from .azure_blob import Blob
from .utils import clean_name, safe_join

_AZURE_NAME_MAX_LEN = 1024


def setting(name: str, default: Optional[Any] = None):
    return getattr(settings, name, default)


def _get_valid_path(s):
    # A blob name:
    #   * must not end with dot or slash
    #   * can contain any character
    #   * must escape URL reserved characters
    #     (not needed here since the azure client will do that)
    s = s.strip('./')
    if len(s) > _AZURE_NAME_MAX_LEN:
        raise ValueError(
            "File name max len is %d" % _AZURE_NAME_MAX_LEN)
    if not len(s):
        raise ValueError(
            "File name must contain one or more "
            "printable characters")
    if s.count('/') > 256:
        raise ValueError(
            "File name must not contain "
            "more than 256 slashes")
    return s


def _content_type(content):
    try:
        return content.file.content_type
    except AttributeError:
        pass
    try:
        return content.content_type
    except AttributeError:
        pass
    return None


class AzureStorage(StaticFilesStorage):

    account_name = setting("AZURE_ACCOUNT_NAME")
    account_key = setting("AZURE_ACCOUNT_KEY")
    container_name = setting("AZURE_CONTAINER_NAME")
    expiration_secs = setting("EXPIRATION_SECS")
    account_domain = setting("AZURE_CUSTOM_DOMAIN")

    timeout = setting("AZURE_CONNECT_TIMEOUT_SEC", 20)
    max_memory_size = setting("AZURE_BLOB_MAX_MEMORY_SIZE", 20*1024*1024)
    overwrite_files = setting('AZURE_OVERWRITE_FILES', False)
    upload_max_conn = setting("AZURE_UPLOAD_MAX_CONN", 2)
    object_parameters = setting("AZURE_OBJECT_PARAMETERS", {})

    file_upload_temp_dir = setting("FILE_UPLOAD_TEMP_DIR", "./")
    cache_control = setting("AZURE_CACHE_CONTROL")
    run_async = setting("AZURE_RUN_ASYNC", False)

    def __init__(self):
        super().__init__()
        self.blob = Blob(container_name=self.container_name,
                         isasync=self.run_async,
                         account_name=self.account_name,
                         account_key=self.account_key,
                         timeout=self.timeout)

    def _normalize_name(self, name):
        try:
            return safe_join(self.location, name)
        except ValueError:
            raise SuspiciousOperation(
                "Attempted access to '%s' denied." % name)

    def _get_valid_path(self, name):
        # Must be idempotent
        return _get_valid_path(
            self._normalize_name(
                clean_name(name)))

    def _get_content_settings_parameters(self, name, content=None):
        guessed_type, content_encoding = mimetypes.guess_type(name)
        content_type = (guessed_type)

        params = {'cache_control': self.cache_control,
                  'content_type': content_type,
                  'content_encoding': content_encoding}
        params.update(self.get_object_parameters(name))
        return ContentSettings(**params)

    def get_object_parameters(self, name):
        """
        Returns a dictionary that is passed to content settings. Override this
        method to adjust this on a per-object basis to set e.g ContentDisposition.
        By default, returns the value of AZURE_OBJECT_PARAMETERS.
        """
        return self.object_parameters.copy()

    def _open(self, name, mode="rb"):
        return AzureStorageFile(name, mode, self)

    def _save(self, name, content):
        cleaned_name = clean_name(name)
        name = self._get_valid_path(name)
        params = self._get_content_settings_parameters(name, content)

        # Unwrap django file (wrapped by parent's save call)
        if isinstance(content, File):
            content = content.file

        content.seek(0)
        try:
            self.blob.blob_name = cleaned_name
            if not self.blob.blob_exists:
                self.blob.upload_blob(blob_name=cleaned_name,
                                      data=content,
                                      content_settings=params,
                                      max_concurrency=self.upload_max_conn,
                                      timeout=self.timeout)
            return cleaned_name
        except ResourceExistsError:
            pass

    def delete(self, name: str) -> None:
        return self.blob.delete_blob(name, timeout=self.timeout)

    def exists(self, name: str) -> bool:
        self.blob.blob_name = name
        return self.blob.blob_exists

    def listdir(self, path: str) -> Tuple[List[str], List[str]]:
        return self.blob.blobs

    def size(self, name: str) -> int:
        return self.blob.blob_properties(name).size

    def url(self, name: Optional[str]) -> str:
        return super().url(name)

    def get_available_name(self, name: str, max_length: Optional[int] = _AZURE_NAME_MAX_LEN) -> str:
        name = clean_name(name)
        return super().get_available_name(name, max_length=max_length)


class AzureStorageFile(File):
    def __init__(self, name: str, mode: str, storage: AzureStorage) -> None:

        self.name = name
        self.mode = mode
        self._storage = storage
        self._file = None
        self._path = storage

    def _get_file(self):
        if self._file:
            return self._file
        file = SpooledTemporaryFile(max_size=self._storage.max_memory_size,
                                    suffix=".AzureStorageBlobFile",
                                    dir=self._storage.file_upload_temp_dir)
        if 'r' in self.mode or 'a' in self.mode:
            self._storage.blob.download_blob(blob_name=self.name,
                                             timeout=self.timeout).readinto(file)
        if 'r' in self.mode:
            # 將讀取指針到開頭位置
            file.seek(0)
        self._file = file
        return self._file

    def _set_file(self, value):
        self.file = value

    file = property(_get_file, _set_file)
