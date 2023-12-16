import os
import asyncio
from abc import ABC, abstractmethod
from typing import Literal, Union
import json
import numpy as np
import onnxruntime as ort
from huggingface_hub import hf_hub_download
from transformers import AutoTokenizer
import contextlib
from typing import Optional
import shutil

class EmbeddingModelBase(ABC):
    def __init__(self):
        super().__init__()
        self.tokenizer = None
        self.max_length = 512

    def __call__(
        self,
        input: Union[str, list[str]],
    ) -> Union[list[list[float]], list[float], np.ndarray]:
        if isinstance(input, str):
            return self.embed(input)
        elif isinstance(input, list):
            return self.embed_batch(input)
        else:
            raise ValueError(f"Input must be str or list[str], not {type(input)}")

    def split_and_tokenize_single(
        self,
        text: str,
        pad: bool = False,
        split_strategy: Literal["truncate", "greedy", "even"] = "even",
    ) -> dict[str, list[list[int]]]:
        """
        Split and tokenize a single text to prepare it for the embedding model.
        Padding is only necessary if running more than 1 sequence thru the model at once.
        Splitting happens when the model exceeds the max_length (usually 512).
        You can either truncate the text, or split into chunks. Chunking can be "greedy"
        (as many 512 chunks as possible), or "even" (split into even-ish chunks with np.array_split).
        """

        # first make into tokens
        tokenized = self.tokenizer(text)  # (seq_len, )

        # if don't have to pad and don't have to split into chunks, we're done
        if not pad and len(tokenized["input_ids"]) <= self.max_length:
            return {k: [tokenized[k]] for k in tokenized}

        # handle splitting
        if split_strategy == "truncate":
            for k in tokenized:
                tokenized[k] = [tokenized[k][: self.max_length]]

        elif split_strategy == "greedy":
            for k in tokenized:
                tokenized[k] = [
                    tokenized[k][idx : idx + self.max_length]
                    for idx in range(0, len(tokenized[k]), self.max_length)
                ]

        elif split_strategy == "even":
            for k in tokenized:
                tokenized[k] = [
                    arr.tolist()
                    for arr in np.array_split(
                        tokenized[k],
                        int(np.ceil(len(tokenized[k]) / self.max_length)),
                    )
                ]

        else:
            raise ValueError(
                f"split_strategy must be 'truncate', 'greedy', or 'even', not {split_strategy}"
            )

        # pad if applicable
        if pad:
            # first make sure list is nested
            if not isinstance(tokenized["input_ids"][0], list):
                for k in tokenized:
                    tokenized[k] = [tokenized[k]]

            # get pad token
            pad_token_id = self.tokenizer.pad_token_id
            if pad_token_id is None:
                pad_token_id = 0

            pad_len = max(
                [
                    len(tokenized["input_ids"][i])
                    for i in range(len(tokenized["input_ids"]))
                ]
            )
            for k in tokenized:
                tokenized[k] = [
                    np.pad(
                        tokenized[k][i],
                        (0, pad_len - len(tokenized[k][i])),
                        constant_values=pad_token_id,
                    ).tolist()
                    for i in range(len(tokenized[k]))
                ]

        return tokenized

    def split_and_tokenize_batch(
        self,
        texts: str,
        pad: bool = False,
        split_strategy: Literal["truncate", "greedy", "even"] = "even",
    ) -> dict:
        """
        Tokenize the text and pad if applicable.

        :param text: The input text to be tokenized.
        :type text: str
        :return: Returns a tuple. dictionary containing tokenized and padded 'input_ids',
        'attention_mask' and 'token_type_ids', along with a list of offsets.
        :rtype: Tuple[Dict[str, numpy.ndarray], list[int]]

        Example:

        .. code-block:: python

            tokenized_text = model.split_and_tokenize('sample text')
        """
        result = {}
        offsets = [0]
        if self.tokenizer is None:
            raise ValueError("Tokenizer is not initialized.")
        if self.max_length is None:
            raise ValueError("max_length is not initialized.")

        # first tokenize without padding
        for text in texts:
            tokenized = self.split_and_tokenize_single(
                text, pad=False, split_strategy=split_strategy
            )
            for k in tokenized:
                if k not in result:
                    result[k] = tokenized[k]
                else:
                    result[k].extend(tokenized[k])

            offsets.append(len(result["input_ids"]))

        # then, if padding, use longest length in batch
        if pad:
            pad_token_id = self.tokenizer.pad_token_id
            if pad_token_id is None:
                pad_token_id = 0

            pad_len = max([len(tokenized[k][0]) for k in result])
            for k in result:
                result[k] = [
                    np.pad(
                        result[k][i],
                        (0, pad_len - len(result[k][i])),
                        constant_values=pad_token_id,
                    ).tolist()
                    for i in range(len(result[k]))
                ]

        return {
            "tokens": result,
            "offsets": offsets,
        }

    @abstractmethod
    def embed(
        self, text: str, normalize: bool = False
    ) -> Union[list[float], np.ndarray]:
        pass

    @abstractmethod
    def embed_batch(
        self, texts: list[str], normalize: bool = False
    ) -> Union[list[list[float]], np.ndarray]:
        pass


class ONNXEmbeddingModel(EmbeddingModelBase):
    def __init__(
        self,
        local_onnx_path: str,
        huggingface_repo: str = None, # used for tokenizer, and model not found at local_onnx_path 
        huggingface_path_in_repo: Optional[str] = None, # used if model not found at local_onnx_path
        max_length = 512
    ):
        super().__init__()
        self.tokenizer = AutoTokenizer.from_pretrained(huggingface_repo)
        self.tokenizer.model_max_length = 1_000_000
        self.max_length = max_length
        self.providers = ["CPUExecutionProvider"]
        if not os.path.exists(local_onnx_path):
            # download
            local_path = hf_hub_download(
                repo_id=huggingface_repo,
                filename=huggingface_path_in_repo
            )
            shutil.copyfile(local_path, local_onnx_path, follow_symlinks=True)
            os.remove(local_path)
        self.session = ort.InferenceSession(local_onnx_path, providers=self.providers)

    def embed(
        self,
        text: str,
        normalize: bool = False,
        pad: bool = False,
        split_strategy: Literal["truncate", "greedy", "even"] = "even",
    ):
        input = self.split_and_tokenize_single(
            text, pad=pad, split_strategy=split_strategy
        )
        outs = []
        for seq in range(len(input["input_ids"])):
            if not pad:
                assert (
                    np.mean(input["attention_mask"][seq]) == 1
                ), "pad=False but attention_mask has 0s"
            out = self.session.run(
                None,
                {
                    "input_ids": input["input_ids"][seq : seq + 1],
                    "attention_mask": input["attention_mask"][seq : seq + 1],
                    "token_type_ids": input["token_type_ids"][seq : seq + 1],
                },
            )[0]  # 1, seq_len, hidden_size
            trimmed = out[
                0, np.array(input["attention_mask"][seq]) == 1, :
            ]  # chunk_seq_len, hidden_size
            outs.append(trimmed)
        outs = np.concatenate(outs, axis=0)  # full_seq_len, hidden_size
        avg = np.mean(outs, axis=0)  # hidden_size
        if normalize:
            avg = avg / np.linalg.norm(avg)
        return avg

    async def embed_batch(
        self,
        texts: list[str],
        normalize: bool = False,
        pad: bool = False,
        split_strategy: Literal["truncate", "greedy", "even"] = "even",
    ):
        result = []
        for text in texts:
            result.append(
                self.embed(
                    text,
                    normalize=normalize,
                    pad=pad,
                    split_strategy=split_strategy,
                )
            )
            asyncio.sleep(0) # allow other tasks to run

        return np.array(result)