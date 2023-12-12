import abc
import os
from io import BytesIO
import json
import pandas as pd
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

class Parser(abc.ABC):
    """
    A parser defines a way to parse a data source, and returns an iterator over its
    items as dictionaries.
    """

    def __init__(self):
        self.metrics = {"files_in": 0, "items_out": 0}

    @abc.abstractmethod
    def parse(self, file: File) -> Iterator[dict]:
        """
        Parses a data source.
        """
        raise NotImplementedError


class JSONLParser(Parser):
    """
    A parser that parses JSON files.
    """

    def parse(self, file: File) -> Iterator[dict]:
        """
        Parses a JSON file.
        """
        self.metrics["files_in"] += 1
        if isinstance(file.content, bytes):
            file.content = file.content.decode()
        for line in file.content.split("\n"):
            if line.strip():
                self.metrics["items_out"] += 1
                yield json.loads(line)


class ParquetParser(Parser):
    """
    A parser that parses Parquet files.
    """

    def parse(self, file: File) -> Iterator[dict]:
        """
        Parses a Parquet file.
        """
        self.metrics["files_in"] += 1
        df = pd.read_parquet(BytesIO(file.content))
        for _, row in df.iterrows():
            self.metrics["items_out"] += 1
            yield row.to_dict()


class Source(abc.ABC):
    """
    A data source defines a set of data that can be streamed.
    It should be able to return an async iterator (this allows other work
    to be done while waiting for I/O).
    """
    parser: Parser

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
    # configuration things
    bucket: str
    prefix: Optional[str]
    access_key_id: str
    secret_access_key: str
    parser: Parser
    compression: Literal["lz4", "zstd", None] = None
    sample_rate: float = 1.0
    sample_level: Literal["file", "instance"] = "file"
    max_concurrent_downloads = 500
    
    # internal things
    prefixes: list[str] = field(init=False, default_factory=list)
    session: AioSession = field(init=False)
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
            if len(self.prefixes) < 10:
                print("Prefixes:", self.prefixes)

        else:
            print("Prefix does not contain glob characters, so we're not expanding.")
            self.prefixes = [self.prefix]

    def __str__(self):
        result = f"🪣 [S3 Source]: s3://{self.bucket}{('/' + self.prefix) if self.prefix else ''}"
        result += f"\n ↳ 📦 [Parser]: {self.parser}"
        result += f"\n ↳ 📈 [Sampling]: {self.sample_rate * 100}% of {self.sample_level}s"
        # compression
        result += f"\n ↳ 🗜 [Compression]: {self.compression}"
        return result

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

    async def paginate_and_fetch(self, client, prefix):
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if not page.get("Contents", None):
                print("No objects found.")
                continue
            for obj in page["Contents"]:
                # skip directories
                if obj["Key"].endswith("/"):
                    continue
                if self.sample_rate < 1.0 and self.sample_level == "file":
                    if normalized_hash(obj["Key"]) > self.sample_rate:
                        continue
                await self.fetch_object(client, obj["Key"])

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
        self.semaphore = asyncio.Semaphore(self.max_concurrent_downloads) # limit to 100 concurrent requests
        async with self.session.create_client(
            "s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        ) as client:
            tasks = [asyncio.create_task(self.paginate_and_fetch(client, prefix)) for prefix in self.prefixes]
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
                for item in self.parser.parse(file):
                    if self.sample_rate < 1.0 and self.sample_level == "instance":
                        if normalized_hash(str(item)) > self.sample_rate:
                            continue
                    yield item
                self.queue.task_done()
            
            await producer
            await self.queue.join()