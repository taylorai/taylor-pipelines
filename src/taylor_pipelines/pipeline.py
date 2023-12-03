from dataclasses import dataclass, field
from typing import Union

from .argument import Argument
from .parse import Parser
from .process import Transform, Filter, Map, Sink
from .source import Source


@dataclass
class Pipeline:
    """
    A pipeline defines a data source and a parser, and then a list of
    transforms to apply to each batch of data.
    """

    source: Source
    parser: Parser
    transforms: list[Transform] = field(default_factory=list)
    batch_size: int = 2
    arguments: list[Argument] = field(default_factory=list)
    metrics: dict[str, Union[int, float]] = field(
        default_factory=lambda: {"files_read": 0, "items_parsed": 0}
    )
    compiled: bool = False

    def compile_transforms(self, arguments: dict):
        # gotta be a cleaner way but for now arguments has a key for each transform,
        # and under that key is a dict of argument names to values for that transform
        # also want "global" arguments that apply to all transforms
        for transform in self.transforms:
            if transform.name in arguments:
                transform.compile(**arguments[transform.name])
                # print(
                #     "Compiled transform",
                #     transform.name,
                #     "with arguments",
                #     arguments[transform.name],
                # )
            else:
                transform.compile()
                # print("Compiled transform", transform.name, "with no arguments")
        self.compiled = True

    def apply_transforms(self, batch: list[dict]) -> list[dict]:
        """
        Applies the transforms to a batch of data.
        """
        for transform in self.transforms:
            batch = transform(batch)
        return batch
    
    def remove_transform(self, transform_name: str):
        """
        Removes a transform from the pipeline.
        """
        for i, transform in enumerate(self.transforms):
            if transform.name == transform_name:
                del self.transforms[i]
                return
        raise ValueError(f"Transform {transform_name} not found.")

    def iterator(self):
        """
        Returns an iterator over the parsed data.
        """
        if not self.compiled:
            raise ValueError("Pipeline not compiled.")
        batch = []
        for file in self.source.iterator():
            self.metrics["files_read"] += 1
            for item in self.parser.parse(file):
                self.metrics["items_parsed"] += 1
                batch.append(item)
                if len(batch) == self.batch_size:
                    yield self.apply_transforms(batch)
                    batch = []
        if batch:
            yield self.apply_transforms(batch)

    def __str__(self):
        result = "== Pipeline ==\n"
        result += "↳ Source: " + str(self.source) + "\n"
        result += "↳ Parser: " + str(self.parser.__class__.__name__) + "\n"
        result += f"↳ Transforms ({len(self.transforms)}):"
        for transform in self.transforms:
            result += "\n  "
            if isinstance(transform, Filter):
                result += "⛔️ "
            elif isinstance(transform, Map):
                result += "🔀 "
            elif isinstance(transform, Sink):
                result += "💾 "
            result += f"{transform}"
        return result
    
    def get_arguments(self):
        """
        Returns a list of all arguments for the pipeline.
        """
        pipeline_args = {"transforms": []}
        for transform in self.transforms:
            transform_spec = {"name": transform.name, "optional": transform.optional, "arguments": []}
            if isinstance(transform, Filter):
                transform_spec["type"] = "filter"
            elif isinstance(transform, Map):
                transform_spec["type"] = "map"
            elif isinstance(transform, Sink):
                transform_spec["type"] = "sink"
            for argument in transform.arguments.values():
                transform_spec["arguments"].append(argument.to_json())

        return pipeline_args

    def print_metrics(self):
        result = "== Metrics ==\n"
        print("Pipeline:", self.metrics)
        print("Parser:", self.parser.metrics)
        for t in self.transforms:
            if hasattr(t, "metrics"):
                print(t.name, t.metrics)
        return result
