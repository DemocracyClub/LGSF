"""
TitanV2Embedder against a fake Bedrock client.

BgeM3Embedder isn't tested here: it needs the 2.3GB model download.
"""

import io
import json
import threading
import time

from lgsf.embeddings.embedders import EMBEDDERS, TitanV2Embedder


class FakeBedrockClient:
    """
    Answers InvokeModel like Bedrock does for Titan, recording requests.

    Shorter texts take longer to answer, so with several threads the
    responses finish out of order.
    """

    def __init__(self):
        self.requests = []
        self.lock = threading.Lock()

    def invoke_model(self, **kwargs):
        body = json.loads(kwargs["body"])
        with self.lock:
            self.requests.append({**kwargs, "body": body})
        text = body["inputText"]
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


def test_embed_query():
    embedder = TitanV2Embedder(client=FakeBedrockClient())
    assert embedder.embed_query("planning") == [8.0, 0.5]


def test_embedders_by_name():
    assert sorted(EMBEDDERS) == ["bge-m3", "titan-v2"]
    for name, embedder in EMBEDDERS.items():
        assert embedder.name == name
        assert embedder.dimensions == 1024
