import os
import time
import json
import concurrent.futures
import multiprocessing as mp
from dataclasses import dataclass, field
from typing import Union, Optional, Literal

from rich.status import Status
import asyncio
from .argument import Argument
from .process import Transform, Filter, Map, Sink
from .classification import TrainClassifier
from .source import Source, S3, HuggingFace, LocalFile, Parser, JSONLParser, ParquetParser


PARSERS = {"jsonl": JSONLParser, "parquet": ParquetParser, "csv": None}


@dataclass
class Pipeline:
    """
    A pipeline defines a data source and a parser, and then a list of
    transforms to apply to each batch of data.
    """

    source: Optional[Source]
    parser: Optional[Parser] = None
    output_directory: str = None
    transforms: list[Transform] = field(default_factory=list)
    batch_size: int = 25
    arguments: list[Argument] = field(default_factory=list)
    max_concurrent_batches: int = 100
    status: Optional[Status] = field(init=False)

    # internal
    metrics: dict[str, Union[int, float]] = field(
        default_factory=lambda: {
            "batches_streamed": 0,
            "batches_processed": 0,
            "items_processed": 0,
        }
    )
    compiled: bool = False
    queue: Optional[asyncio.Queue] = None
    semaphore: Optional[asyncio.Semaphore] = None

    ## TODO: Add way to specify total maximum number of examples to process.

    def set_output_directory(self, output_directory: str):
        """
        Sets the output directory for the pipeline.
        """
        self.output_directory = output_directory

    def set_data_source(self, config_file: str):
        """
        Sets the data source using a local config file.
        """
        config = json.load(open(config_file))
        if config["type"] == "S3":
            # assume that secret access key and access key id are in environment variables
            parser = PARSERS[config["file_type"]]()
            self.source = S3(
                bucket=config["bucket"],
                prefix=config["prefix"],
                access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                parser=parser,
                compression=config["compression"],
                sample_rate=config["sample_rate"],
                sample_level=config["sample_level"],
            )
        elif config["type"] == "HuggingFace":
            self.source = HuggingFace(
                dataset_name=config["dataset_name"],
                split=config["split"],
                config_name=config.get("config_name", None),
                sample_rate=config["sample_rate"],
                hf_api_key=os.getenv("HUGGING_FACE_HUB_TOKEN", None),
                streaming=config.get("streaming", False),
            )
        elif config["type"] == "Local":
            self.source = LocalFile(
                filename=config["filename"],
                file_type=config["file_type"],
                sample_rate=config["sample_rate"],
            )
        else:
            raise ValueError(f"Unknown data source type {config['type']}")


    def set_s3_data_source(
        self,
        bucket: str,
        prefix: str,
        access_key_id: str,
        secret_access_key: str,
        file_type: Literal["jsonl", "parquet", "csv"],
        sample_rate: float = 1.0,
        sample_level: Literal["file", "instance"] = "file",
        compression: Literal["lz4", "zstd", None] = None,
    ):
        """
        Sets the data source to an S3 bucket.
        """
        if self.parser:
            parser = self.parser
        elif file_type:
            parser = PARSERS[file_type]()
        else:
            parser = JSONLParser()
        self.source = S3(
            bucket=bucket,
            prefix=prefix,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            parser=parser,
            compression=compression,
            sample_rate=sample_rate,
            sample_level=sample_level,
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
            if isinstance(transform, Sink) or isinstance(transform, TrainClassifier):
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

    def update_status(self):
        """
        Updates the status bar.
        """
        message = f"Streamed {self.metrics['batches_streamed']} batches, processed {self.metrics['batches_processed']} batches."
        self.status.update(message)

    async def apply_transforms(
        self, batch: list[dict], executor: concurrent.futures.Executor = None
    ) -> list[dict]:
        """
        Applies the transforms to a batch of data.
        """
        async with self.semaphore:
            for transform in self.transforms:
                batch = await transform(batch, executor=executor)
            self.metrics["batches_processed"] += 1
            self.metrics["items_processed"] += len(batch)
            self.update_status()
            return batch

    async def stream_batches(self):
        batch = []
        async for item in self.source:
            batch.append(item)
            if len(batch) == self.batch_size:
                await self.queue.put(batch)
                self.metrics["batches_streamed"] += 1
                self.update_status()
                # print("put batch")
                batch = []
        if batch:
            await self.queue.put(batch)
            self.metrics["batches_streamed"] += 1
            self.update_status()
        await self.queue.put(None)

    async def process_batches(self):
        # print("Processing batches with max_workers", mp.cpu_count())
        # with concurrent.futures.ProcessPoolExecutor(max_workers=mp.cpu_count()) as pool:
        tasks = []
        while True:
            batch = await self.queue.get()
            if batch is None:
                # print("got None")
                self.queue.task_done()
                break
            # print("got batch")
            task = asyncio.create_task(self.apply_transforms(batch))
            tasks.append(task)
            self.queue.task_done()

        await asyncio.gather(*tasks)

    async def flush_sinks(self):
        """
        Flushes all sinks.
        """
        for transform in self.transforms:
            if isinstance(transform, Sink):
                await transform.flush()

    async def run(self, arguments: dict = {}):
        """
        Run the pipeline.
        """
        print()
        print(self)
        if not self.compiled:
                print("Compiling transforms...")
                self.compile_transforms(arguments)
        start_time = time.time()
        self.queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(self.max_concurrent_batches)
        self.status = Status("Beginning to process data...")
        with self.status:
            producer = asyncio.create_task(self.stream_batches())
            consumer = asyncio.create_task(self.process_batches())
            await asyncio.gather(producer, consumer)
            await self.queue.join()
            await self.flush_sinks()
        self.status.update("Finishing classifier training...")
        with self.status:
            self.finish_training()
        end_time = time.time()
        print(
            (
                "\n* ===== RESULTS ===== *\n"
                f"Processed {self.metrics['batches_processed']} batches "
                f"of {self.batch_size} items (total {self.metrics['items_processed']}) "
                f"in {end_time - start_time:.2f} seconds."
            )
        )
        for transform in self.transforms:
            if hasattr(transform, "print_metrics"):
                print(f"{transform}")
                transform.print_metrics()
            elif hasattr(transform, "metrics"):
                print(transform.name, transform.metrics)

    def finish_training(self):
        """
        Finishes training for all classifiers.
        """
        for transform in self.transforms:
            if isinstance(transform, TrainClassifier):
                transform.complete_remaining_epochs()

    def __str__(self):
        result = "* === PIPELINE === *\n"
        result += "↳ Source: " + str(self.source) + "\n"
        result += f"↳ Transforms ({len(self.transforms)}):"
        for transform in self.transforms:
            result += f"\n  {transform}"
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
                "source_code": transform.source_code,
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