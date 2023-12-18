import abc
import os
import requests
from io import BytesIO
import random
import json
import numpy as np
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
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
                key=self.access_key_id, secret=self.secret_access_key
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
        result += (
            f"\n ↳ 📈 [Sampling]: {self.sample_rate * 100}% of {self.sample_level}s"
        )
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
                object_size = obj["Size"]
                await self.fetch_object(client, obj["Key"], object_size)

    async def fetch_chunk(self, client, key, index, byte_range):
        # fetch without decompressing (you can only decompress the entire file)
        async with self.semaphore:
            response = await client.get_object(
                Bucket=self.bucket, Key=key, Range=byte_range
            )
            async with response["Body"] as stream:
                data = await stream.read()
            print(f"Fetched chunk {index} of {key}.")
            return data

    async def fetch_object(self, client, key, size):
        # if size is under 10MB, just fetch it directly
        if size < 10_000_000:
            async with self.semaphore:
                response = await client.get_object(Bucket=self.bucket, Key=key)
                async with response["Body"] as stream:
                    data = await stream.read()
                    decompressed_data = self.decompress(data)
                    await self.queue.put(File(filename=key, content=decompressed_data))
        # otherwise use Range requests to fetch it in chunks concurrently
        else:
            print("Fetching large file in chunks.")
            chunk_size = 3_000_000
            byte_ranges = [
                f"bytes={i}-{i+chunk_size-1}" for i in range(0, size, chunk_size)
            ]
            tasks = [
                asyncio.create_task(self.fetch_chunk(client, key, idx, byte_range=br))
                for idx, br in enumerate(byte_ranges)
            ]
            chunks = await asyncio.gather(*tasks)
            decompressed_data = self.decompress(b"".join(chunks))
            await self.queue.put(File(filename=key, content=decompressed_data))

    async def __aiter__(self) -> AsyncIterator[File]:
        """
        Returns an iterator over S3 objects.
        """
        self.queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(
            self.max_concurrent_downloads
        )  # limit to 100 concurrent requests
        async with self.session.create_client(
            "s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        ) as client:
            tasks = [
                asyncio.create_task(self.paginate_and_fetch(client, prefix))
                for prefix in self.prefixes
            ]
            producer = asyncio.gather(*tasks)

            def done_callback(future):
                print("Done streaming files from S3.")
                self.queue.put_nowait(None)

            producer.add_done_callback(done_callback)

            while True:
                file = await self.queue.get()
                if file is None:
                    self.queue.task_done()
                    break
                for item in self.parser.parse(file):
                    if self.sample_rate < 1.0 and self.sample_level == "instance":
                        # if normalized_hash(str(item)) > self.sample_rate:
                        if random.random() > self.sample_rate:
                            continue
                    yield item
                self.queue.task_done()

            await producer
            await self.queue.join()


@dataclass
class HuggingFace(Source):
    """
    A data source that uses the HuggingFace Datasets library.
    """

    dataset_name: str
    split: str
    config_name: Optional[str] = None
    streaming: bool = False
    sample_rate: float = 1.0
    hf_api_key: Optional[str] = None
    parser: Parser = None

    def __str__(self):
        result = f"🤗 [HuggingFace Source]: {self.dataset_name} ("
        if self.config_name:
            result += f"{self.config_name}/ "
        result += f"{self.split})"
        result += f"\n ↳ 📦 [Parser]: {self.parser}"
        result += f"\n ↳ 📈 [Sampling]: {self.sample_rate * 100}% of examples"
        return result

    def __iter__(self) -> Iterator[dict]:
        """
        Returns an iterator over parsed data.
        """
        import datasets

        if self.hf_api_key:
            import huggingface_hub
            print(f"Using provided token hf_xxxxxxxxxxxxxxxxxxxxxx{self.hf_api_key[-4:]} to log into HuggingFace Hub.")

            huggingface_hub.login(token=self.hf_api_key)

        handle = datasets.load_dataset(
            self.dataset_name,
            name=self.config_name,
            split=self.split,
            streaming=self.streaming,
            token=self.hf_api_key,
        )

        if self.sample_rate < 1.0:
            if not self.streaming:
                dataset_length = len(handle)
            else:
                try:
                    res = requests.get(
                        f"https://datasets-server.huggingface.co/info?dataset={self.dataset_name}"
                    )
                    data = res.json()
                    cn = self.config_name if self.config_name else "default"
                    dataset_length = data["dataset_info"][cn]["splits"][self.split][
                        "num_examples"
                    ]
                except KeyError as e:
                    print("Can't sample by example because dataset length is unknown.")
                    raise e
            idxs_to_keep = np.random.choice(
                dataset_length, int(self.sample_rate * dataset_length), replace=False
            )
            handle = handle.select(idxs_to_keep)

        for item in handle:
            yield item

    async def __aiter__(self) -> AsyncIterator[dict]:
        """
        Returns an async iterator over files.
        """
        for item in self:
            yield item
            await asyncio.sleep(0.01)

    def to_dict(self):
        """
        Returns a dict/serializable representation of the source.
        """
        return {
            "type": "HuggingFace",
            "dataset_name": self.dataset_name,
            "split": self.split,
            "config_name": self.config_name,
            "streaming": self.streaming,
            "sample_rate": self.sample_rate,
            "hf_api_key": self.hf_api_key,
        }
    
    def to_json_string(self):
        """
        Returns a JSON string representation of the source.
        """
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, d):
        """
        Returns a HuggingFace source from a dict.
        """
        return cls(**d)
    
    @classmethod
    def from_json_string(cls, json_string):
        """
        Returns a HuggingFace source from a JSON string.
        """
        return cls.from_dict(json.loads(json_string))

