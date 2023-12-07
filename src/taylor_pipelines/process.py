import os
import asyncio
import aiofiles
import concurrent.futures

import abc
import functools
import json
from collections.abc import Callable
from typing import Any, Optional

from .argument import Argument


class Transform(abc.ABC):
    """
    A Transform is a filter, map, or sink.
    Has shared functionality between the three.
    A Transform should only be optional if it can be left out of the pipeline
    entirely without breaking it (no matter what other transforms are/aren't there).
    """

    name: str
    arguments: dict[str, Argument]
    optional: bool
    compiled: bool
    description: Optional[str]

    def __init__(
        self,
        name: str,
        description: str = "",
        arguments: list[Argument] = [],
        optional: bool = False,
    ):
        self.name = name
        self.description = description
        self.arguments = {argument.name: argument for argument in arguments}
        self.optional = optional
        self.compiled = False

    def args_to_kwargs(self):
        """
        Converts the arguments to keyword arguments for compilation with 'partial'.
        """
        return {name: argument.value for name, argument in self.arguments.items()}

    def set_arguments(self, **kwargs):
        """
        Accept values for Arguments (preparing for compilation).
        1. Make sure all required arguments are provided.
        2. Make sure no extra arguments are provided.
        3. Make sure all provided arguments are valid.
        4. Return merged kwargs for compilation, falling back on defaults.
        """
        provided_arguments = set(kwargs.keys())
        for name, arg in self.arguments.items():
            # make sure that required arguments are provided
            if arg.required and arg.name not in provided_arguments:
                raise ValueError(
                    f"Required Argument {arg.name} missing for filter '{self.name}'"
                )
            # if not required and not provided, use default
            elif not arg.required and arg.name not in provided_arguments:
                self.arguments[name].set_value(arg.default)

            # otherwise use the provided value
            else:
                self.arguments[name].set_value(kwargs[name])
                provided_arguments.remove(name)

        # make sure no extra arguments are provided
        if len(provided_arguments) > 0:
            raise ValueError(
                f"Invalid Arguments {provided_arguments} for filter '{self.name}'"
            )

    @abc.abstractmethod
    def compile(self, **kwargs):
        """
        Compiles the transform with provided public Arguments.
        Should set self.compiled to True in this method.
        After compilation, all arguments but those passed to
        [map, filter, write] should be fixed with 'partial'.
        """
        raise NotImplementedError

    def __str__(self):
        result = self.name + f" ({self.__class__.__name__})"
        for name, argument in self.arguments.items():
            result += f"\n    ↳ {argument.__class__.__name__}: {name}"
            if argument.value is not None:
                result += f" = {argument.value}"
        return result


class Filter(Transform):
    """
    A filter defines a way to filter a stream of data.
    :param arguments: A list of public Arguments that parameterize the filter,
    and can be configured by the person running the pipeline.
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        arguments: list[Argument] = [],
        optional: bool = False,
    ):
        super().__init__(name, description, arguments, optional)
        self.metrics = {"items_in": 0, "items_out": 0}

    @abc.abstractmethod
    def filter(self, batch: list[dict]) -> list[dict]:
        """
        Filters a batch of data.
        """
        raise NotImplementedError

    def __call__(self, batch: list[dict]) -> list[dict]:
        self.metrics["items_in"] += len(batch)
        filtered = self.filter(batch)
        self.metrics["items_out"] += len(filtered)
        return filtered


class FunctionFilter(Filter):
    """
    A simple filter that filters a batch of data by applying a function
    to each item in the batch.
    :param predicate: A function that takes a dict and returns a bool.
    :param arguments: A list of public Arguments that parameterize the filter.
    :param kwargs: Keyword arguments to pass to the predicate that are NOT
    public Arguments.
    """

    name: str
    predicate: Callable[[dict], bool]
    description: Optional[str]
    arguments: dict[str, Argument]

    def __init__(
        self,
        name: str,
        predicate: Callable,
        description: str = "",
        arguments: list[Argument] = [],
        optional: bool = False,
        **kwargs,
    ):
        self.predicate = functools.partial(predicate, **kwargs)
        super().__init__(name, description, arguments, optional)

    def compile(self, **kwargs):
        """
        Compiles the filter with provided public Arguments.
        """
        # merge provided values with defaults for Arguments and validate
        self.set_arguments(**kwargs)
        # set the predicate with the compiled arguments
        self.predicate = functools.partial(self.predicate, **self.args_to_kwargs())
        self.compiled = True

    def filter(self, batch: list[dict]) -> list[dict]:
        """
        Filters a batch of data.
        """
        if not self.compiled:
            raise ValueError("Filter not compiled.")
        return list(filter(self.predicate, batch))


class Map(Transform):
    """
    A map defines a way to map a stream of data.
    """

    name: str
    description: Optional[str]
    arguments: dict[str, Argument]
    compiled: bool

    def __init__(
        self, name: str, description: str = "", arguments: list[Argument] = [], optional: bool = False
    ):
        super().__init__(name, description, arguments, optional)

    @abc.abstractmethod
    def map(self, batch: list[dict]) -> list[dict]:
        """
        Maps a batch of data.
        """
        raise NotImplementedError

    def __call__(self, batch: list[dict]) -> list[dict]:
        return self.map(batch)


class FunctionMap(Map):
    """
    A simple map that maps a batch of data by applying a function
    to each item in the batch.
    """

    name: str
    function: Callable[[dict], dict]
    description: str = ""
    arguments: dict[str, Argument]

    def __init__(
        self,
        name: str,
        function: Callable,
        description: str = "",
        arguments: list[Argument] = [],
        optional: bool = False,
        **kwargs,
    ):
        self.function = functools.partial(function, **kwargs)
        super().__init__(name, description, arguments, optional)

    def compile(self, **kwargs):
        """
        Compiles the filter with provided public Arguments.
        """
        self.set_arguments(**kwargs)
        self.function = functools.partial(self.function, **self.args_to_kwargs())
        self.compiled = True

    async def map(self, batch: list[dict], executor: concurrent.futures.Executor = None) -> list[dict]:
        """
        Maps a batch of data.
        """
        if not self.compiled:
            raise ValueError("Map not compiled.")
        # if executor is not None:
        #     loop = asyncio.get_event_loop()
        #     tasks = [
        #         loop.run_in_executor(executor, self.function, item) for item in batch
        #     ]
        return list(map(self.function, batch))


class Sink(Transform):
    """
    A sink defines a way to write a stream of data.
    It also returns the data so it can be an intermediate
    step in a pipeline. output_directory can be used if the
    sink writes to a file, in which case paths will be relative to
    the output directory.
    """
    name: str
    description: Optional[str]
    output_directory: Optional[str]
    arguments: dict[str, Argument]
    optional: bool

    def __init__(
        self,
        name: str,
        description: str = "",
        arguments: list[Argument] = [],
        optional: bool = False,
    ):
        super().__init__(name, description, arguments, optional)
        self.output_directory = None

    @abc.abstractmethod
    async def write(self, batch: list[dict]):
        """
        Writes a batch of data.
        """
        raise NotImplementedError

    async def __call__(self, batch: list[dict]):
        await self.write(batch)
        return batch


class JSONLSink(Sink):
    """
    A sink that writes a batch of data to a JSONL file.
    """
    output_file: str

    def __init__(
        self, 
        name: str, 
        output_file: str,
        description: str = "",
        arguments: list[Argument] = [],
        optional: bool = False,
    ):
        super().__init__(name, description, arguments, optional)
        self.output_file = output_file

    def compile(self, **kwargs):
        """
        Compiles the filter with provided public Arguments.
        """
        self.set_arguments(**kwargs)  # have to do this to check no extras provided
        # if output_directory is not None, prepend to output file and make sure the directories exist
        if self.output_directory is not None:
            self.output_file = os.path.join(self.output_directory, self.output_file)
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        self.compiled = True

    async def write(self, batch: list[dict]):
        """
        Writes a batch of data.
        """
        async with aiofiles.open(self.output_file, "a") as f:
            for item in batch:
                await f.write(json.dumps(item) + "\n")
