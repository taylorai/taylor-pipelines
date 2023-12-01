import abc
import functools
import json
from collections.abc import Callable
from dataclasses import dataclass


class Filter(abc.ABC):
    """
    A filter defines a way to filter a stream of data.
    """

    @abc.abstractmethod
    def filter(self, batch: list[dict]) -> list[dict]:
        """
        Filters a batch of data.
        """
        raise NotImplementedError
    
    def __call__(self, batch: list[dict]) -> list[dict]:
        return self.filter(batch)
    

class Map(abc.ABC):
    """
    A map defines a way to map a stream of data.
    """
    @abc.abstractmethod
    def map(self, batch: list[dict]) -> list[dict]:
        """
        Maps a batch of data.
        """
        raise NotImplementedError
    
    def __call__(self, batch: list[dict]) -> list[dict]:
        return self.map(batch)
    

class Sink(abc.ABC):
    """
    A sink defines a way to write a stream of data.
    It also returns the data so it can be an intermediate
    step in a pipeline.
    """
    @abc.abstractmethod
    def write(self, batch: list[dict]):
        """
        Writes a batch of data.
        """
        raise NotImplementedError
    
    def __call__(self, batch: list[dict]):
        self.write(batch)
        return batch

@dataclass
class FunctionFilter(Filter):
    """
    A simple filter that filters a batch of data by applying a function
    to each item in the batch.
    """
    predicate: Callable[[dict], bool]

    def __init__(self, predicate: Callable, **kwargs):
        self.predicate = functools.partial(predicate, **kwargs)

    def filter(self, batch: list[dict]) -> list[dict]:
        """
        Filters a batch of data.
        """
        return list(filter(self.predicate, batch))

class FunctionMap(Map):
    """
    A simple map that maps a batch of data by applying a function
    to each item in the batch.
    """
    function: Callable[[dict], dict]

    def __init__(self, function: Callable, **kwargs):
        self.function = functools.partial(function, **kwargs)

    def map(self, batch: list[dict]) -> list[dict]:
        """
        Maps a batch of data.
        """
        return list(map(self.function, batch))
    
class JSONLSink(Sink):
    """
    A sink that writes a batch of data to a JSONL file.
    """
    output_file: str

    def __init__(self, output_file: str):
        self.output_file = output_file

    def write(self, batch: list[dict]):
        """
        Writes a batch of data.
        """
        with open(self.output_file, "a") as f:
            for item in batch:
                f.write(json.dumps(item) + "\n")
