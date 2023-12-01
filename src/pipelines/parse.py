import abc
import json
from .source import File
from collections.abc import Iterator

class Parser(abc.ABC):
    """
    A parser defines a way to parse a data source, and returns an iterator over its
    items as dictionaries.
    """
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
        for line in file.content.split("\n"):
            if line.strip():
                yield json.loads(line)
