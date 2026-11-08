"""
Embedding models: turn chunk text, and later search queries, into vectors.

Each embedder has a short ``name``. It names the file a document's
embeddings are written to, ``<storage key>.<name>.jsonl``, and its entry
in the document's status.json.

boto3's Bedrock client is made inside the constructor, so this module
imports without connecting to anything.
"""

import abc
import hashlib
import json
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass


@dataclass
class Embedding:
    vector: list[float]
    #: How many tokens the model read, by its own tokenizer.
    token_count: int


@dataclass
class Failed:
    """
    One text the model didn't embed, in place of its Embedding. The
    others in the batch still come back, so one throttled request
    doesn't lose a document's work.
    """

    error: str


class EmbedderBase(abc.ABC):
    """
    One embedding model.

    Some models embed a search query differently from the documents it's
    searched against (nomic and Cohere take a prefix or input type,
    Qwen3-Embedding adds an instruction to queries). Titan doesn't, but
    both methods exist so that one that does only has to change
    embed_query(), and search code can always call embed_query().
    """

    #: Short name, used for the embeddings file and the status entry.
    name: str
    #: The provider's model ID, written on every record.
    model: str
    dimensions: int

    @abc.abstractmethod
    def embed_documents(self, texts: Sequence[str]) -> list[Embedding | Failed]:
        """
        Embeddings for chunks, in the same order as ``texts``, with a
        Failed for each one the model didn't embed.
        """

    @abc.abstractmethod
    def embed_query(self, text: str) -> list[float]:
        """The embedding for a search query."""


class TitanV2Embedder(EmbedderBase):
    """
    Amazon Titan Text Embeddings V2, on Bedrock.

    The region and credentials come from the environment, for example
    ``AWS_PROFILE`` and ``AWS_DEFAULT_REGION``. The role needs
    bedrock:InvokeModel on this model.
    """

    name = "titan-v2"
    model = "amazon.titan-embed-text-v2:0"
    dimensions = 1024

    #: Bedrock takes one text per InvokeModel call for this model, so
    #: texts are sent from several threads. Its quotas in eu-west-2 are
    #: 6,000 requests and 300,000 tokens a minute for the account; when
    #: either is reached, the adaptive retry mode below slows the client
    #: down.
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

    def embed_documents(self, texts: Sequence[str]) -> list[Embedding | Failed]:
        # map() returns results in input order, whichever finishes first.
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            return list(pool.map(self._embed_or_fail, texts))

    def _embed_or_fail(self, text: str) -> Embedding | Failed:
        try:
            return self._embed(text)
        except Exception as exc:  # noqa: BLE001
            # Such as a ThrottlingException that outlasts the retries, or
            # a ValidationException for a text that's too long.
            return Failed(f"{type(exc).__name__}: {exc}")

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text).vector

    def _embed(self, text: str) -> Embedding:
        response = self.client.invoke_model(
            modelId=self.model,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(
                {
                    "inputText": text,
                    "dimensions": self.dimensions,
                    "normalize": True,
                }
            ),
        )
        body = json.loads(response["body"].read())
        return Embedding(
            vector=body["embedding"], token_count=body["inputTextTokenCount"]
        )


class FakeEmbedder(EmbedderBase):
    """
    Vectors made from each text's SHA-256, so the same text always gets
    the same vector and nothing is sent anywhere. For running the
    pipeline without AWS, and for tests, which also read the texts it
    was sent.
    """

    name = "fake"
    model = "fake-sha256"
    dimensions = 4

    def __init__(self):
        self.texts = []

    def vector(self, text: str) -> list[float]:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        return [byte / 255 for byte in digest[: self.dimensions]]

    def embed_documents(self, texts: Sequence[str]) -> list[Embedding | Failed]:
        self.texts.extend(texts)
        return [
            Embedding(vector=self.vector(text), token_count=len(text.split()))
            for text in texts
        ]

    def embed_query(self, text: str) -> list[float]:
        return self.vector(text)


EMBEDDERS = {cls.name: cls for cls in (FakeEmbedder, TitanV2Embedder)}
