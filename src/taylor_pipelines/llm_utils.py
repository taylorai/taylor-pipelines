import time
import asyncio
from dataclasses import dataclass
from typing import Callable

@dataclass
class Model:
    """
    Definition of a model that an AsyncLLMEngine can route requests to.
    """
    model_name: str
    api_base: str
    api_key: str
    max_length_in_tokens: int
    max_requests_per_minute: int
    max_tokens_per_minute: int
    tokenizer: Callable

@dataclass
class APIRequest:
    """
    Definition of an API request.
    """
    messages: list[dict]
    temperature: float
    retries_left: int
    


class AsyncModelEngine:
    """
    This class is responsible for handling global state for one API model.
    """
    def __init__(self, model_spec: Model):
        self.model_spec = model_spec
        self.tokenizer = model_spec.tokenizer
        self.queue = asyncio.Queue()
        self.available_request_capacity = model_spec.max_requests_per_minute
        self.available_token_capacity = model_spec.max_tokens_per_minute
        self.last_update_time = time.time()
        self.shutdown_requested = False
        # self.loop = asyncio.get_running_loop()
        # self.loop.create_task(self.run())

    async def add_request(self, request: APIRequest):
        if self.shutdown_requested:
            raise RuntimeError("Engine is shutting down, no more requests accepted")
        future = asyncio.get_running_loop().create_future()
        await self.queue.put((request, future))
        return future
    
    async def process_requests(self):
        while True:
            request, future = await self.queue.get()
            if request is None:  # Shutdown signal
                break
            try:
                result = await self.process_request(request)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)

    async def process_request(self, request):
        # Implement your API request processing logic here
        pass

class AsyncLLMGateway:
    """
    This class is responsible for handling global state for all API models, and
    routing requests to the best model with capacity.
    """