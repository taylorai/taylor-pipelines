import os
import inspect
import pickle
import asyncio
import aiohttp
import aiofiles
import concurrent.futures

import abc
import functools
import json
import json_tricks
import jinja2
from jinja2.sandbox import ImmutableSandboxedEnvironment
from jinja2 import meta

from collections.abc import Callable
from typing import Any, Optional

from .argument import Argument
from .embeddings import ONNXEmbeddingModel


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
        source_code: Optional[str] = None
    ):
        self.name = name
        self.description = description
        self.arguments = {argument.name: argument for argument in arguments}
        self.optional = optional
        self.compiled = False
        self.source_code = source_code

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
    async def filter(self, batch: list[dict]) -> list[dict]:
        """
        Filters a batch of data.
        """
        raise NotImplementedError

    async def __call__(self, batch: list[dict], executor: concurrent.futures.Executor = None) -> list[dict]:
        self.metrics["items_in"] += len(batch)
        filtered = await self.filter(batch, executor=executor)
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
    can_run_in_pool: bool

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

        try:
            self.source_code = inspect.getsource(predicate)
        except Exception as e:
            print(f"Unable to get source code for {name} predicate. Error: {e}")

    def compile(self, **kwargs):
        """
        Compiles the filter with provided public Arguments.
        """
        # merge provided values with defaults for Arguments and validate
        self.set_arguments(**kwargs)
        # set the predicate with the compiled arguments
        self.predicate = functools.partial(self.predicate, **self.args_to_kwargs())
        # check once at compilation time that the predicate can be pickled
        try:
            pickle.dumps(self.predicate)
            self.can_run_in_pool = True
        except pickle.PicklingError:
            print(f"Warning: transform {self.name} cannot be run in a process pool.")
            self.can_run_in_pool = False
        self.compiled = True

    async def filter(self, batch: list[dict], executor: concurrent.futures.Executor = None) -> list[dict]:
        """
        Filters a batch of data.
        """
        if not self.compiled:
            raise ValueError("Filter not compiled.")
        if executor is not None and self.can_run_in_pool:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(executor, self.predicate, item) for item in batch
            ]
            return [item for item, keep in zip(batch, await asyncio.gather(*tasks)) if keep]
        else:
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
    async def map(self, batch: list[dict], executor: concurrent.futures.Executor = None) -> list[dict]:
        """
        Maps a batch of data.
        """
        raise NotImplementedError

    async def __call__(self, batch: list[dict], executor: concurrent.futures.Executor = None) -> list[dict]:
        return await self.map(batch, executor=executor)


class FunctionMap(Map):
    """
    A simple map that maps a batch of data by applying a function
    to each item in the batch.
    """

    name: str
    function: Callable[[dict], dict]
    description: str = ""
    arguments: dict[str, Argument]
    can_run_in_pool: bool

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

        try:
            self.source_code = inspect.getsource(function)
        except Exception as e:
            print(f"Unable to get source code for {name} function. Error: {e}")

    def compile(self, **kwargs):
        """
        Compiles the filter with provided public Arguments.
        """
        self.set_arguments(**kwargs)
        self.function = functools.partial(self.function, **self.args_to_kwargs())
        # check once at compilation time that the function can be pickled
        try:
            pickle.dumps(self.function)
            self.can_run_in_pool = True
        except pickle.PicklingError:
            print(f"Warning: transform {self.name} cannot be run in a process pool.")
            self.can_run_in_pool = False
        self.compiled = True

    async def map(self, batch: list[dict], executor: concurrent.futures.Executor = None) -> list[dict]:
        """
        Maps a batch of data.
        """
        if not self.compiled:
            raise ValueError("Map not compiled.")
        if executor is not None and self.can_run_in_pool:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(executor, self.function, item) for item in batch
            ]
            return await asyncio.gather(*tasks)
        else:
            return list(map(self.function, batch))


class LLMMap(Map):
    """
    Apply LLM completion to a batch of data and put the output in a new field.
    Works with any OpenAI-compatible API. The prompt and system prompt will be
    compiled ONCE with Jinja2 using provided Arguments (if applicable). Fields from the
    input dataset that vary per data sample should be doubly-nested: {{ "{{ field_name }}" }}.
    At compilation time, these will unnest, and then will be filled in per-example.
    """
    def __init__(
        self,
        name: str,
        prompt_template: str,
        model_name: str,
        output_field: str,
        api_key: str,
        api_base: str = "https://api.openai.com/v1",
        description: str = "",
        system_prompt_template: str = "",
        temperature: float = 0.9,
        arguments: list[Argument] = [],
        optional: bool = False,
    ):
        super().__init__(name, description, arguments, optional)
        self.prompt_template = prompt_template
        self.system_prompt_template = system_prompt_template
        self.api_key = api_key
        self.api_base = api_base
        self.model_name = model_name
        self.temperature = temperature
        self.output_field = output_field
        self.prompt = None
        self.system_prompt = None

        # TODO: Add asyncio.Queue to limit concurrent requests, handle retries, etc.
        # TODO: Add a way to specify labels + apply logit bias
        # TODO: Other options like max_length, JSON mode, etc.

    def compile(self, **kwargs):
        """
        Compiles the filter with provided public Arguments.
        """
        # validate and prepare user-provided arguments
        self.set_arguments(**kwargs)
        all_args = self.args_to_kwargs()

        # compile the prompt and system prompt with these
        env = ImmutableSandboxedEnvironment()
        try:
            prompt_env = env.from_string(self.prompt_template)
            prompt_template_args = meta.find_undeclared_variables(env.parse(self.prompt_template))
            self.prompt = prompt_env.render(**{
                k: all_args[k] for k in prompt_template_args
            })
        except jinja2.exceptions.TemplateSyntaxError as e:
            raise ValueError(f"Invalid prompt template: {e}")
        
        try:
            system_prompt_env = env.from_string(self.system_prompt_template)
            system_prompt_template_args = meta.find_undeclared_variables(env.parse(self.system_prompt_template))
            self.system_prompt = system_prompt_env.render(**{
                k: all_args[k] for k in system_prompt_template_args
            })
        except jinja2.exceptions.TemplateSyntaxError as e:
            raise ValueError(f"Invalid system prompt template: {e}")

        self.compiled = True

    async def _get_completion(self, prompt: str, system_prompt: str) -> str:
        """
        Gets a completion from the API.
        """
        messages = []
        if system_prompt and len(system_prompt) > 0:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        self.request_header = {
            "Authorization": f"Bearer {self.api_key}",
        }
        self.request_json = {
            "model": self.model_name,
            "messages": messages,
            "temperature": self.temperature,
        }
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(
                    url=self.api_base + "/chat/completions",
                    headers=self.request_header,
                    json=self.request_json,
                ) as response:
                    if response.status != 200:
                        text = await response.text()
                        print("Got non-200 response: ", text, "Prompt that led to this: ", prompt[:150] + "[...]", flush=True)
                        return None
                    
                    response = await response.json()
                    return response["choices"][0]["message"]["content"]
                    # except Exception as e:
                    #     raise e
            except asyncio.TimeoutError as e:
                print("Timeout occurred. Returning None.")
                return None
        
    async def map(self, batch: list[dict], executor: concurrent.futures.Executor = None) -> list[dict]:
        """
        Gets completions for a batch of data. We don't use an executor, as LLM completions are network-bound
        and asyncio is more than sufficient.
        """
        if not self.compiled:
            raise ValueError("LLMMap not compiled.")
        if self.prompt is None:
            raise ValueError("LLMMap prompt is None.")
        if self.system_prompt is None:
            raise ValueError("LLMMap system prompt is None.")
        
        env = ImmutableSandboxedEnvironment()
        prompt_args = meta.find_undeclared_variables(env.parse(self.prompt)) # only has holes left for per-item fields
        prompts = [env.from_string(self.prompt).render(**{k: item[k] for k in prompt_args}) for item in batch]
        for prompt in prompts:
            assert len(prompt) > 0, "Prompt is empty."
        tasks = []
        for prompt in prompts:
            tasks.append(asyncio.create_task(self._get_completion(prompt, self.system_prompt)))

        completions = await asyncio.gather(*tasks)
        
        return [
            {**item, self.output_field: completion} for item, completion in zip(batch, completions)
        ]
    
class APIEmbeddingMap(Map):
    """
    Compute text embedding on a field in a batch of data and put the output in a new field.
    This one is for an OpenAI-compatible server. Use LocalEmbeddingMap to compute locally.
    """
    pass

class LocalEmbeddingMap(Map):
    """
    Compute text embedding on a field in a batch of data and put the output in a new field.
    This one is for a local server. Use APIEmbeddingMap to compute remotely.
    """
    def __init__(
        self,
        name: str,
        input_field: str,
        output_field: str,
        local_onnx_path: str,
        huggingface_repo: str = None, # used for tokenizer, and model not found at local_onnx_path 
        huggingface_path_in_repo: Optional[str] = None, # used if model not found at local_onnx_path
        max_length=512,
        normalize=True,
        optional: bool = False,
    ):
        super().__init__(name, description=f"Compute embeddings on {input_field} and put them in {output_field}.", optional=optional)
        self.input_field = input_field
        self.output_field = output_field
        self.normalize = normalize
        self.model = ONNXEmbeddingModel(
            local_onnx_path=local_onnx_path,
            huggingface_repo=huggingface_repo,
            huggingface_path_in_repo=huggingface_path_in_repo,
            max_length=max_length
        )

    def compile(self, **kwargs):
        self.compiled = True

    async def map(self, batch: list[dict], executor: concurrent.futures.Executor = None) -> list[dict]:
        """
        Gets embeddings for a batch of data.
        """
        if not self.compiled:
            raise ValueError("LocalEmbeddingMap not compiled.")
        if self.input_field is None:
            raise ValueError("LocalEmbeddingMap input field is None.")
        if self.output_field is None:
            raise ValueError("LocalEmbeddingMap output field is None.")
        
        texts = [item[self.input_field] for item in batch]
        embeddings = await self.model.embed_batch(texts, normalize=self.normalize)
        return [
            {**item, self.output_field: embedding} for item, embedding in zip(batch, embeddings)
        ]

    


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
    
    @abc.abstractmethod
    async def flush(self):
        """
        Flushes any remaining data to disk.
        """
        pass

    async def __call__(self, batch: list[dict], executor: concurrent.futures.Executor = None):
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
        max_buffer_size: int = 100000
    ):
        super().__init__(name, description, arguments, optional)
        self.output_file = output_file
        self.buffer = []
        self.max_buffer_size = max_buffer_size

    def compile(self, **kwargs):
        """
        Compiles the filter with provided public Arguments.
        """
        self.set_arguments(**kwargs)  # have to do this to check no extras provided
        # if output_directory is not None, prepend to output file and make sure the directories exist
        if self.output_directory is not None:
            self.output_file = os.path.join(self.output_directory, self.output_file)
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        print("Output file for JSONL Sink:", self.output_file)
        self.compiled = True

    async def _flush_buffer(self):
        async with aiofiles.open(self.output_file, "a") as f:
            for item in self.buffer:
                await f.write(json_tricks.dumps(item, primitives=True) + "\n")
        self.buffer = []
        

    async def write(self, batch: list[dict]):
        """
        Writes a batch of data.
        """
        if len(self.buffer) + len(batch) > self.max_buffer_size:
            await self._flush_buffer()
        self.buffer.extend(batch)

    async def flush(self):
        """
        Flushes any remaining data to disk.
        """
        await self._flush_buffer()
        
