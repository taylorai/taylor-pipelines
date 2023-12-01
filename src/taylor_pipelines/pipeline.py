from dataclasses import dataclass, field
from typing import Union

from .argument import Argument
from .parse import Parser
from .process import Filter, Map, Sink
from .source import Source


@dataclass
class Pipeline:
    """
    A pipeline defines a data source and a parser, and then a list of
    transforms to apply to each batch of data.
    """
    source: Source
    parser: Parser
    transforms: list[Union[Map, Filter, Sink]]
    batch_size: int = 2
    arguments: list[Argument] = field(default_factory=list)
    files_read: int = 0
    items_returned: int = 0

    def apply_transforms(self, batch: list[dict]) -> list[dict]:
        """
        Applies the transforms to a batch of data.
        """
        for transform in self.transforms:
            batch = transform(batch)
        return batch
    
    def iterator(self):
        """
        Returns an iterator over the parsed data.
        """
        batch = []
        for file in self.source.iterator():
            self.files_read += 1
            for item in self.parser.parse(file):
                self.items_returned += 1
                batch.append(item)
                if len(batch) == self.batch_size:
                    yield self.apply_transforms(batch)
                    batch = []
        if batch:
            yield self.apply_transforms(batch)
