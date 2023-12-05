import abc
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

import boto3
import xxhash
import lz4.frame as lz4
import zstandard as zstd


@dataclass
class File:
    """
    A file is a file-like object that can be read.
    """

    filename: str
    content: Any


class Source(abc.ABC):
    """
    A data source defines a set of data that can be streamed.
    It should be able to return an iterator.
    """

    @property
    def batched(self) -> bool:
        """
        Returns True if the source is batched.
        """
        raise NotImplementedError

    def iterator(self) -> Iterator[Any]:
        """
        Returns an iterator.
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
    s3_client: boto3.client = field(init=False, default=None)

    def __post_init__(self):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )

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

    def iterator(self) -> Iterator[Any]:
        """
        Returns an iterator over S3 objects.
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in pages:
            if not page["Contents"]:
                print("No objects found.")
                continue
            for obj in page["Contents"]:
                if self.sample_rate < 1.0:
                    if normalized_hash(obj["Key"]) > self.sample_rate:
                        continue
                response = self.s3_client.get_object(Bucket=self.bucket, Key=obj["Key"])
                data = response["Body"].read()
                decompressed_data = self.decompress(data)
                yield File(filename=obj["Key"], content=decompressed_data)


class LocalDirectory(Source):
    directory: str
    compression: Literal["lz4", "zstd", None] = None

    def __init__(self, directory: str):
        self.directory = directory

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

    def iterator(self) -> Iterator[Any]:
        """
        Returns an iterator over files in a directory.
        """
        for filename in os.listdir(self.directory):
            with open(os.path.join(self.directory, filename), "rb") as f:
                data = f.read()
                decompressed_data = self.decompress(data)
                yield File(filename=filename, content=decompressed_data)
