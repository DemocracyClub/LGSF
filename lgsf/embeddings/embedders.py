"""
Embedding models: turn chunk text, and later search queries, into vectors.

Each embedder has a short `name`. It's what
`manage.py embeddings embed --model` takes, and it names the directory the
embeddings are written to and the database table they're loaded into.

boto3 and sentence-transformers are imported inside the constructors, so
this module imports without the `embeddings` dependency group installed.
"""

import abc
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Sequence


@dataclass
class Embedding:
    vector: list[float]
    #: How many tokens the model read, by its own tokenizer.
    token_count: int


class EmbedderBase(abc.ABC):
    """
    One embedding model.

    Some models embed a search query differently from the documents it's
    searched against (nomic and Cohere take a prefix or input type,
    Qwen3-Embedding adds an instruction to queries). Neither model here
    does, but both methods exist so that one that does only has to
    change embed_query(), and search code can always call embed_query().
    """

    #: Short name, used for the output directory and the database table.
    name: str
    #: The provider's model ID, written on every record.
    model: str
    dimensions: int

    @abc.abstractmethod
    def embed_documents(self, texts: Sequence[str]) -> list[Embedding]:
        """Embeddings for chunks, in the same order as `texts`."""

    @abc.abstractmethod
    def embed_query(self, text: str) -> list[float]:
        """The embedding for a search query."""


class TitanV2Embedder(EmbedderBase):
    """
    Amazon Titan Text Embeddings V2, on Bedrock.

    The region and credentials come from the environment, for example
    `AWS_PROFILE=dev-lgsf-dc`. The role needs bedrock:InvokeModel on this
    model.
    """

    name = "titan-v2"
    model = "amazon.titan-embed-text-v2:0"
    dimensions = 1024

    #: Bedrock takes one text per InvokeModel call for this model, so
    #: texts are sent from several threads. Its default quota is 2,000
    #: requests a minute per account and region; when that's reached,
    #: the adaptive retry mode below slows the client down.
    max_workers = 8

    def __init__(self, client=None):
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "bedrock-runtime",
                config=Config(retries={"mode": "adaptive", "max_attempts": 10}),
            )
        self.client = client

    def embed_documents(self, texts: Sequence[str]) -> list[Embedding]:
        # map() returns results in input order, whichever finishes first.
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            return list(pool.map(self._embed, texts))

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text).vector

    def _embed(self, text: str) -> Embedding:
        response = self.client.invoke_model(
            modelId=self.model,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(
                {"inputText": text, "dimensions": self.dimensions, "normalize": True}
            ),
        )
        body = json.loads(response["body"].read())
        return Embedding(
            vector=body["embedding"], token_count=body["inputTextTokenCount"]
        )


class BgeM3Embedder(EmbedderBase):
    """
    BAAI/bge-m3, run locally on the CPU with sentence-transformers.

    The first run downloads the model, about 2.3GB, to
    ~/.cache/huggingface. bge-m3's dense vectors don't use a query
    instruction, so queries are encoded the same way as documents.
    """

    name = "bge-m3"
    model = "BAAI/bge-m3"
    dimensions = 1024
    batch_size = 16

    def __init__(self):
        from sentence_transformers import SentenceTransformer

        self.encoder = SentenceTransformer(self.model, device="cpu")

    def embed_documents(self, texts: Sequence[str]) -> list[Embedding]:
        texts = list(texts)
        if not texts:
            return []
        # All texts in one call: sentence-transformers sorts them by
        # length before batching, so each batch is padded less.
        vectors = self.encoder.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        # Counted without truncation, so a text longer than the model's
        # limit shows up as a count over it.
        token_counts = [len(ids) for ids in self.encoder.tokenizer(texts)["input_ids"]]
        return [
            Embedding(vector=vector.tolist(), token_count=count)
            for vector, count in zip(vectors, token_counts, strict=True)
        ]

    def embed_query(self, text: str) -> list[float]:
        return self.encoder.encode(text, normalize_embeddings=True).tolist()


EMBEDDERS = {cls.name: cls for cls in (TitanV2Embedder, BgeM3Embedder)}
