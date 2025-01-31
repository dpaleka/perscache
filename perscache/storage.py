import datetime as dt
from abc import ABC, abstractmethod
from pathlib import Path

import redis
from beartype import beartype
from beartype.typing import Iterator, Optional, Union


class CacheExpired(Exception):
    ...


class Storage(ABC):
    @abstractmethod
    def read(self, path: Union[str, Path], deadline: dt.datetime) -> bytes:
        """Read the file at the given path and return its contents as bytes.
        If the file does not exist, raise FileNotFoundError. If the file is
        older than the given deadline, raise CacheExpired.
        """
        ...

    @abstractmethod
    def write(self, path: Union[str, Path], data: bytes) -> None:
        """Write the file at the given path."""
        ...


class FileStorage(Storage):
    @beartype
    def __init__(
        self,
        location: Optional[Union[str, Path]] = ".cache",
        max_size: Optional[int] = None,
    ):
        self.location = Path(location)
        self.max_size = max_size

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(location={self.location}, max_size={self.max_size})>"

    def read(self, path: Union[str, Path], deadline: dt.datetime) -> bytes:
        final_path = self.location / path

        if deadline is not None and self.mtime(final_path) < deadline:
            raise CacheExpired

        return self.read_file(self.location / path)

    def write(self, path: Union[str, Path], data: bytes) -> None:
        final_path = self.location / path

        self.ensure_path(final_path.parent)

        if self.max_size and self.size(self.location) + len(data) > self.max_size:
            self.delete_least_recently_used(target_size=self.max_size)

        self.write_file(final_path, data)

    def delete_least_recently_used(self, target_size: int) -> None:
        """Removes the least recently used file from the cache.
        The least recently used file is the one with the smallest last access time.

        Args:
            target_size: The target size of the cache.
        """
        files = sorted(self.iterdir(self.location), key=self.atime, reverse=True)

        # find the set of most recently accessed files whose total size
        # is smaller than the target size
        i, size = 0, 0
        while size < target_size and i < len(files):
            size += self.size(files[i])
            i += 1

        # remove remaining files
        for f in files[i - 1 :]:
            self.delete(f)

    def clear(self) -> None:
        """Remove the directory with the cache along with all of its contents
        if it exists, otherwise just silently passes with no exceptions.
        """

        for f in self.iterdir(self.location):
            self.delete(f)
        self.rmdir(self.location)

    @abstractmethod
    def read_file(self, path: Union[str, Path]) -> bytes:
        """Read a file at a relative path inside the cache
        or raise FileNotFoundError if not found.
        """
        ...

    @abstractmethod
    def write_file(self, path: Union[str, Path]) -> bytes:
        """Write a file at a relative path inside the cache
        or raise FileNotFoundError if the cache directory doesn't exist.
        """
        ...

    @abstractmethod
    def ensure_path(self, path: Union[str, Path]) -> None:
        """Create an absolute path if it doesn't exist."""
        ...

    @abstractmethod
    def iterdir(self, path: Union[str, Path]) -> Union[Iterator[Path], list]:
        """Return an iterator through files within a directory indicated by path
        or an empty list if the path doesn't exist.
        """
        ...

    @abstractmethod
    def rmdir(self, path: Union[str, Path]) -> None:
        """Remove a directory. Silently pass if it doesn't exist."""
        ...

    @abstractmethod
    def mtime(self, path: Union[str, Path]) -> dt.datetime:
        """Get file last modified time."""
        ...

    @abstractmethod
    def atime(self, path: Union[str, Path]) -> dt.datetime:
        """Get file last accessed time."""
        ...

    @abstractmethod
    def size(self, path: Union[str, Path]) -> int:
        """Get the size in bytes for a file or a directory indicated by path.
        Zero if the path doesn't exist.
        """
        ...

    @abstractmethod
    def delete(self, path: Union[str, Path]) -> None:
        """Remove file or raise FileNotFoundError if not found."""
        ...


class LocalFileStorage(FileStorage):
    def read_file(self, path: Union[str, Path]) -> bytes:
        return path.read_bytes()

    def write_file(self, path: Union[str, Path], data: bytes) -> None:
        path.write_bytes(data)

    def ensure_path(self, path: Union[str, Path]) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def iterdir(self, path: Union[str, Path]) -> Union[Iterator[Path], list]:
        return path.iterdir() if path.exists() else []

    def rmdir(self, path: Union[str, Path]) -> None:
        if path.exists():
            path.rmdir()

    def mtime(self, path: Union[str, Path]) -> dt.datetime:
        return dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)

    def atime(self, path: Union[str, Path]) -> dt.datetime:
        return dt.datetime.fromtimestamp(path.stat().st_atime, tz=dt.timezone.utc)

    def size(self, path: Union[str, Path]) -> int:
        return path.stat().st_size if path.exists() else 0

    def delete(self, path: Union[str, Path]) -> None:
        path.unlink()


class GoogleCloudStorage(FileStorage):
    @beartype
    def __init__(
        self,
        location: Optional[Union[str, Path]] = ".cache",
        max_size: Optional[int] = None,
        storage_options: Optional[dict] = None,
    ) -> None:
        super().__init__(location, max_size)

        import gcsfs

        self.fs = (
            gcsfs.GCSFileSystem(**storage_options)
            if storage_options
            else gcsfs.GCSFileSystem()
        )

    def read_file(self, path: Union[str, Path]) -> bytes:
        with self.fs.open(str(path), "rb") as f:
            return f.read()

    def write_file(self, path: Union[str, Path], data: bytes) -> None:
        with self.fs.open(str(path), "wb") as f:
            f.write(data)

    def ensure_path(self, path: Union[str, Path]) -> None:
        if not self.fs.exists(str(path)):
            self.fs.makedirs(str(path), exist_ok=True)

    def iterdir(self, path: Union[str, Path]) -> Iterator[Path]:
        return self.fs.ls(str(path)) if self.fs.exists(str(path)) else []

    def rmdir(self, path: Union[str, Path]) -> None:
        self.fs.rm(str(path))

    def mtime(self, path: Union[str, Path]) -> dt.datetime:
        mtime = self.fs.info(str(path))["updated"]
        ts = dt.datetime.strptime(mtime.split(".")[0], "%Y-%m-%dT%H:%M:%S")
        ts = ts.replace(tzinfo=dt.timezone.utc, microsecond=int(mtime[-4:-1]))
        return ts

    def atime(self, path: Union[str, Path]) -> dt.datetime:
        # fall back to mtime because last accessed (atime) is not available for GCS
        return self.mtime(str(path))

    def size(self, path: Union[str, Path]) -> int:
        return self.fs.info(str(path))["size"] if self.fs.exists(str(path)) else 0

    def delete(self, path: Union[str, Path]) -> None:
        self.fs.rm(str(path))

REDIS_CONFIG_DEFAULT = {
    'host': 'localhost',
    'port': 6379,
    'db': 7,
}

class DbStorage(Storage):
    """A generic database storage class."""

    @abstractmethod
    def read(self, path: Union[str, Path], deadline: dt.datetime) -> bytes:
        """Read data from the database."""

    @abstractmethod
    def write(self, path: Union[str, Path], data: bytes) -> None:
        """Write data to the database."""


class RedisStorage(Storage):
    """A Redis-specific storage class optimized for storing arbitrary JSON objects."""

    @beartype
    def __init__(
        self,
        namespace: Optional[Union[str, Path]] = "cache",
        redis_config: Optional[dict] = None,
    ):
        """
        Initialize the RedisStorage instance.

        :param location: A namespace prefix for keys in Redis to avoid collisions.
        :param redis_config: Configuration dictionary for Redis connection.
        """
        # Convert location to string to ensure compatibility with Redis key naming
        self.namespace = str(namespace)
        assert ":" not in self.namespace, "Namespace cannot contain ':'"
        self.redis_config = redis_config if redis_config else REDIS_CONFIG_DEFAULT
        self.db = self._connect()

    def _connect(self):
        """Establish a connection to the Redis server."""
        return redis.Redis(**self.redis_config)

    def read(self, path: str | Path, deadline: dt.datetime) -> bytes:
        """
        Read a JSON object from Redis.

        :param path: The key associated with the JSON object.
#        :param deadline: Not used in this implementation, provided for interface compatibility.
        :return: The JSON object as bytes.
        :raises FileNotFoundError: If the key does not exist in Redis.
        """
        key = self._make_key(path)
        data = self.db.get(key)
        if data is None:
            raise FileNotFoundError(f"No data found for key: {key}")
        
        return data

    def write(self, path: Union[str, Path], data: bytes) -> None:
        """
        Write a JSON object to Redis.

        :param path: The key under which to store the JSON object.
        :param data: The JSON object, serialized into bytes.
        """
        key = self._make_key(path)
        self.db.set(key, data)

    def _make_key(self, path: Union[str, Path]) -> str:
        """
        Generate a Redis key based on the given path and the location prefix.

        :param path: The original key for the JSON object.
        :return: A namespaced key for use in Redis.
        """
        return f"{self.namespace}:{str(path)}"

    def __decode_key(self, key: str) -> str:
        """
        Decode a Redis key into its original form.

        :param key: The Redis key.
        :return: The original key for the JSON object.
        """
        return key.split(":", 1)[1]
    
    def get_all(self, namespace: str):
        """
        Get all keys and values from a given namespace.

        :param namespace: The namespace to query.
        :return: A dictionary of keys and values.
        """
        keys = self.db.keys(f"{namespace}:*")
        print("len(keys):", len(keys))
        return {self.__decode_key(key.decode('utf-8')): self.db.get(key) for key in keys}
