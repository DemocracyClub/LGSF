"""TitanV2Embedder against a fake Bedrock client, and the fake embedder."""

import io
import json
import threading
import time

from lgsf.embeddings.embedders import (
    EMBEDDERS,
    FakeEmbedder,
    Failed,
    TitanV2Embedder,
)


class FakeBedrockClient:
    """
    Answers InvokeModel like Bedrock does for Titan, recording requests.

    Shorter texts take longer to answer, so with several threads the
    responses finish out of order.
    """

    def __init__(self, fail_on=None):
        self.requests = []
        self.lock = threading.Lock()
        self.fail_on = fail_on

    def invoke_model(self, **kwargs):
        body = json.loads(kwargs["body"])
        with self.lock:
            self.requests.append({**kwargs, "body": body})
        text = body["inputText"]
        if self.fail_on and self.fail_on in text:
            raise RuntimeError("ThrottlingException: Too many requests")
        time.sleep(max(0, 20 - len(text)) / 1000)
        response = {
            "embedding": [float(len(text)), 0.5],
            "inputTextTokenCount": len(text.split()),
        }
        return {"body": io.BytesIO(json.dumps(response).encode())}


def test_request():
    client = FakeBedrockClient()
    TitanV2Embedder(client=client).embed_documents(["Cabinet resolved."])
    [request] = client.requests
    assert request["modelId"] == "amazon.titan-embed-text-v2:0"
    assert request["body"] == {
        "inputText": "Cabinet resolved.",
        "dimensions": 1024,
        "normalize": True,
    }


def test_reads_vector_and_token_count():
    embedder = TitanV2Embedder(client=FakeBedrockClient())
    [embedding] = embedder.embed_documents(["Cabinet resolved to approve."])
    assert embedding.vector == [28.0, 0.5]
    assert embedding.token_count == 4


def test_results_are_in_input_order():
    embedder = TitanV2Embedder(client=FakeBedrockClient())
    texts = ["x" * length for length in range(1, 21)]
    embeddings = embedder.embed_documents(texts)
    assert [e.vector[0] for e in embeddings] == [float(len(t)) for t in texts]


def test_one_failed_request_keeps_the_others():
    embedder = TitanV2Embedder(client=FakeBedrockClient(fail_on="slow"))
    results = embedder.embed_documents(["fast one", "slow one", "fast two"])
    assert results[0].vector == [8.0, 0.5]
    assert results[1] == Failed("RuntimeError: ThrottlingException: Too many requests")
    assert results[2].vector == [8.0, 0.5]


def test_embed_query():
    embedder = TitanV2Embedder(client=FakeBedrockClient())
    assert embedder.embed_query("planning") == [8.0, 0.5]


def test_fake_embedder_is_deterministic():
    one, two = FakeEmbedder(), FakeEmbedder()
    [a] = one.embed_documents(["Cabinet resolved."])
    [b] = two.embed_documents(["Cabinet resolved."])
    assert a == b
    assert len(a.vector) == 4
    assert all(0 <= value <= 1 for value in a.vector)
    assert one.vector("Cabinet resolved.") != one.vector("Cabinet refused.")
    assert one.texts == ["Cabinet resolved."]


def test_embedders_by_name():
    assert sorted(EMBEDDERS) == ["fake", "titan-v2"]
    assert EMBEDDERS["titan-v2"].dimensions == 1024
