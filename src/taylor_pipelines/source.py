import abc
import os
import re
import asyncio
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Literal, Optional
from typing import AsyncIterator

# import boto3
# import aiobotocore
from aiobotocore.session import get_session, AioSession
import xxhash
import lz4.frame as lz4
import zstandard as zstd

# use logger with timestamp
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



@dataclass
class File:
    """
    A file is a file-like object that can be read.
    """

    filename: str
    content: Any
    # should we include metadata?


class Source(abc.ABC):
    """
    A data source defines a set of data that can be streamed.
    It should be able to return an async iterator (this allows other work
    to be done while waiting for I/O).
    """

    @property
    def batched(self) -> bool:
        """
        Returns True if the source is batched.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def __aiter__(self) -> AsyncIterator[File]:
        """
        Returns an async iterator over files.
        """
        raise NotImplementedError


def normalized_hash(item: str):
    # Convert the item to a string and hash it
    hash_value = xxhash.xxh64(item).intdigest()
    return hash_value / float(2**64)

@dataclass
class S3(Source):
    bucket: str
    prefix: Optional[str]
    access_key_id: str
    secret_access_key: str
    compression: Literal["lz4", "zstd", None] = None
    sample_rate: float = 1.0
    session: AioSession = field(init=False)
    prefixes: list[str] = field(init=False, default_factory=list)
    queue: Optional[asyncio.Queue] = None
    semaphore: Optional[asyncio.Semaphore] = None

    def __post_init__(self):
        self.session = get_session()
        if self.prefix is None:
            print("Prefix is None, so we're setting it to an empty string.")
            self.prefix = ""
            self.prefixes = [""]
        
        # search for any glob characters: [], *, **, ?
        elif re.search(r"[\*\?\[\]]", self.prefix):
            print("Prefix contains glob characters, using s3fs to expand.")
            import s3fs
            fs = s3fs.S3FileSystem(
                key=self.access_key_id,
                secret=self.secret_access_key
            )
            glob_str = self.bucket.rstrip("/") + "/" + self.prefix.lstrip("/")
            prefixes = fs.glob(glob_str)
            # if glob includes trailing /, add it to each prefix so they also only match directories
            if glob_str.endswith("/"):
                prefixes = [p + "/" for p in prefixes]
            # split out the bucket name
            self.prefixes = [p.split("/", 1)[1] for p in prefixes]
            print("Found", len(self.prefixes), "prefixes.")

        else:
            print("Prefix does not contain glob characters, so we're not expanding.")
            self.prefixes = [self.prefix]

    def __str__(self):
        return f"🪣 [S3 Source]: s3://{self.bucket}{('/' + self.prefix) if self.prefix else ''}"

    def decompress(self, obj: Any) -> Any:
        """
        Decompresses an object.
        """
        if self.compression == "lz4":
            return lz4.decompress(obj).decode("utf-8")
        elif self.compression == "zstd":
            return zstd.decompress(obj)
        elif self.compression is None:
            return obj
        else:
            raise ValueError(f"Unknown compression {self.compression}")
        
    async def fetch_object(self, client, key):
        async with self.semaphore:
            response = await client.get_object(Bucket=self.bucket, Key=key)
            async with response['Body'] as stream:
                data = await stream.read()
                decompressed_data = self.decompress(data)
                await self.queue.put(File(filename=key, content=decompressed_data))
            
    async def __aiter__(self) -> AsyncIterator[File]:
        """
        Returns an iterator over S3 objects.
        """
        self.queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(100) # limit to 100 concurrent requests
        async with self.session.create_client(
            "s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        ) as client:
            tasks = []
            logger.info("Initializing tasks to fetch objects from S3.")
            paginator = client.get_paginator("list_objects_v2")
            for prefix in self.prefixes:
                async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    if not page["Contents"]:
                        print("No objects found.")
                        continue
                    for obj in page["Contents"]:
                        # skip directories
                        if obj["Key"].endswith("/"):
                            continue
                        if self.sample_rate < 1.0:
                            if normalized_hash(obj["Key"]) > self.sample_rate:
                                continue
                        tasks.append(
                            asyncio.create_task(self.fetch_object(client, obj["Key"]))
                        )
            logger.info("Running tasks.")
            producer = asyncio.gather(*tasks)

            def done_callback(future):
                logger.info("Done streaming files from S3.")
                self.queue.put_nowait(None)
            producer.add_done_callback(done_callback)

            while True:
                file = await self.queue.get()
                if file is None:
                    self.queue.task_done()
                    break
                yield file
                self.queue.task_done()
            
            await producer
            await self.queue.join()



## NEED TO MAKE THIS ASYNC TO FIT WITH THE API NOW
# class LocalDirectory(Source):
#     directory: str
#     compression: Literal["lz4", "zstd", None] = None

#     def __init__(self, directory: str):
#         self.directory = directory

#     def decompress(self, obj: Any) -> Any:
#         """
#         Decompresses an object.
#         """
#         if self.compression == "lz4":
#             return lz4.decompress(obj).decode("utf-8")
#         elif self.compression == "zstd":
#             return zstd.decompress(obj)
#         elif self.compression is None:
#             return obj
#         else:
#             raise ValueError(f"Unknown compression {self.compression}")

#     def iterator(self) -> Iterator[Any]:
#         """
#         Returns an iterator over files in a directory.
#         """
#         for filename in os.listdir(self.directory):
#             with open(os.path.join(self.directory, filename), "rb") as f:
#                 data = f.read()
#                 decompressed_data = self.decompress(data)
#                 yield File(filename=filename, content=decompressed_data)
