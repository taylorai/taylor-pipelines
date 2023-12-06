import os
from dataclasses import dataclass, field
from typing import Union, Optional

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

    source: Optional[Source]
    parser: Parser
    output_directory: str = None
    transforms: list[Transform] = field(default_factory=list)
    batch_size: int = 25
    arguments: list[Argument] = field(default_factory=list)
    metrics: dict[str, Union[int, float]] = field(
        default_factory=lambda: {"files_read": 0, "items_parsed": 0, "batches_processed": 0}
    )
    compiled: bool = False
    queue: Optional[asyncio.Queue] = None

    def set_output_directory(self, output_directory: str):
        """
        Sets the output directory for the pipeline.
        """
        self.output_directory = output_directory

    def set_s3_data_source(
        self, 
        bucket: str, 
        prefix: str, 
        access_key_id: str,
        secret_access_key: str,
        sample_rate: float = 1.0,
        compression: str = None
    ):
        """
        Sets the data source to an S3 bucket.
        """
        self.source = S3(
            bucket=bucket,
            prefix=prefix,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            compression=compression,
            sample_rate=sample_rate
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

    def apply_transforms(self, batch: list[dict]) -> list[dict]:
        """
        Applies the transforms to a batch of data.
        """
        for transform in self.transforms:
            batch = transform(batch)
        return batch

    async def stream_batches(self):
        batch = []
        async for file in self.source:
            self.metrics["files_read"] += 1
            for item in self.parser.parse(file):
                self.metrics["items_parsed"] += 1
                batch.append(item)
                if len(batch) == self.batch_size:
                    await self.queue.put(batch)
                    # print("put batch")
                    batch = []
        if batch:
            await self.queue.put(batch)
            # print("put batch")
        await self.queue.put(None)
        # print("put None")

    async def process_batches(self):
        while True:
            batch = await self.queue.get()
            if batch is None:
                # print("got None")
                self.queue.task_done()
                break
            # print("got batch")
            self.apply_transforms(batch)
            await asyncio.sleep(0) # allow other tasks to run so the doesn't empty
            self.metrics["batches_processed"] += 1
            self.queue.task_done()

    async def run(self, arguments: dict = {}):
        """
        Returns an async iterator over the parsed data.
        (Probably much faster when reading from S3).
        """
        print(self)
        if not self.compiled:
            self.compile_transforms(arguments)
        self.queue = asyncio.Queue()
        producer = asyncio.create_task(self.stream_batches())
        consumer = asyncio.create_task(self.process_batches())
        await asyncio.gather(producer, consumer)
        await self.queue.join()

        print(f"Processed {self.metrics['batches_processed']} batches.")
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