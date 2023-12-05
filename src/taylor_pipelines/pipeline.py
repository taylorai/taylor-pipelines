import os
from dataclasses import dataclass, field
from typing import Union

import asyncio
from .argument import Argument
from .parse import Parser
from .process import Transform, Filter, Map, Sink
from .source import Source, S3


@dataclass
class Pipeline:
    """
    A pipeline defines a data source and a parser, and then a list of
    transforms to apply to each batch of data.
    """

    source: Source
    parser: Parser
    output_directory: str = None
    transforms: list[Transform] = field(default_factory=list)
    batch_size: int = 2
    arguments: list[Argument] = field(default_factory=list)
    metrics: dict[str, Union[int, float]] = field(
        default_factory=lambda: {"files_read": 0, "items_parsed": 0}
    )
    compiled: bool = False

    def set_output_directory(self, output_directory: str):
        """
        Sets the output directory for the pipeline.
        """
        self.output_directory = output_directory

    def set_s3_data_source(self, bucket: str, prefix: str, compression: str = None):
        """
        Sets the data source to an S3 bucket.
        """
        self.source = S3(
            bucket=bucket,
            prefix=prefix,
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            compression=compression,
        )

    def compile_transforms(self, arguments: dict):
        # gotta be a cleaner way but for now arguments has a key for each transform,
        # and under that key is a dict of argument names to values for that transform
        # also want "global" arguments that apply to all transforms
        if "__disabled__" in arguments:
            for transform in self.transforms:
                if transform.name in arguments["__disabled__"]:
                    try:
                        self.remove_transform(transform.name)
                    except ValueError:
                        print("Couldn't disable transform", transform.name)
                
        for transform in self.transforms:
            if isinstance(transform, Sink):
                if self.output_directory:
                    transform.output_directory = self.output_directory
            if transform.name in arguments:
                transform.compile(**arguments[transform.name])
            else:
                transform.compile()
            
        self.compiled = True

    def apply_transforms(self, batch: list[dict]) -> list[dict]:
        """
        Applies the transforms to a batch of data.
        """
        for transform in self.transforms:
            batch = transform(batch)
        return batch
    
    async def apply_transforms_async(self, batch: list[dict]) -> list[dict]:
        return self.apply_transforms(batch)

    def remove_transform(self, transform_name: str):
        """
        Removes a transform from the pipeline.
        """
        for i, transform in enumerate(self.transforms):
            if transform.name == transform_name:
                if not transform.optional:
                    raise ValueError(f"Transform {transform_name} is not optional.")
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

    async def async_iterator(self):
        """
        Returns an async iterator over the parsed data.
        (Probably much faster when reading from S3).
        """
        if not self.compiled:
            raise ValueError("Pipeline not compiled.")
        batch = []
        async for file in self.source.async_iterator():
            self.metrics["files_read"] += 1
            async for item in self.parser.async_parse(file):
                self.metrics["items_parsed"] += 1
                batch.append(item)
                if len(batch) == self.batch_size:
                    yield self.apply_transforms(batch)
                    batch = []
        if batch:
            yield self.apply_transforms(batch)

    def run(self, arguments: dict = {}):
        """
        Runs the pipeline.
        """
        if not self.compiled:
            self.compile_transforms(arguments)
        print(self)
        batches_processed = 0
        for batch in self.iterator():
            batches_processed += 1
        print(f"Processed {batches_processed} batches.")

        self.print_metrics()

    async def run_async(self, arguments: dict = {}):
        """
        Runs the pipeline asynchronously.
        """
        if not self.compiled:
            self.compile_transforms(arguments)
        print(self)
        batches_processed = 0
        tasks = []
        async for batch in self.iterator():
            task = asyncio.ensure_future(self.apply_transforms_async(batch))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for _ in results:
            # Handle the processed batch
            batches_processed += 1
        print(f"Processed {batches_processed} batches.")
        self.print_metrics()

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
            transform_spec = {
                "name": transform.name,
                "description": transform.description,
                "optional": transform.optional,
                "arguments": [],
            }
            if isinstance(transform, Filter):
                transform_spec["type"] = "filter"
            elif isinstance(transform, Map):
                transform_spec["type"] = "map"
            elif isinstance(transform, Sink):
                transform_spec["type"] = "sink"
            for argument in transform.arguments.values():
                transform_spec["arguments"].append(argument.to_json())

            pipeline_args["transforms"].append(transform_spec)

        return pipeline_args

    def print_metrics(self):
        result = "== Metrics ==\n"
        print("Pipeline:", self.metrics)
        print("Parser:", self.parser.metrics)
        for t in self.transforms:
            if hasattr(t, "metrics"):
                print(t.name, t.metrics)
        return result
