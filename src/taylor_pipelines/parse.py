import abc
import json
import pandas as pd
from collections.abc import Iterator

from .source import File


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
        df = pd.read_parquet(file.content)
        for _, row in df.iterrows():
            yield row.to_dict()