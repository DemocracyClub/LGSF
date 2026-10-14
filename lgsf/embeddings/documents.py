"""
Splitting the DoclingDocuments that `manage.py embeddings extract` saved
into chunks of text, with docling-core's HybridChunker.

HybridChunker splits a document at the section headings Docling found,
then splits any section that's too long, and merges neighbouring pieces
that are too short and have the same headings. Its tokenizer here counts
words, so that the limit is the same 350 words as for split_text(), until
an embedding model is chosen.

This needs docling-core, from the embeddings dependency group, so
DocumentChunker only imports it when it's run.
"""

from dataclasses import dataclass
from typing import Any

from docling_core.transforms.chunker.hierarchical_chunker import (
    ChunkingDocSerializer,
    ChunkingSerializerProvider,
)
from docling_core.transforms.chunker.hybrid_chunker import HybridChunker
from docling_core.transforms.chunker.tokenizer.base import BaseTokenizer
from docling_core.transforms.serializer.base import (
    BaseDocSerializer,
    BaseTableSerializer,
    SerializationResult,
)
from docling_core.transforms.serializer.common import create_ser_result
from docling_core.types.doc import DoclingDocument, TableItem

from lgsf.embeddings.text import count_words


class WordTokenizer(BaseTokenizer):
    """Counts words, as split_text() does by default."""

    max_tokens: int = 350

    def count_tokens(self, text: str) -> int:
        return count_words(text)

    def get_max_tokens(self) -> int:
        return self.max_tokens

    def get_tokenizer(self) -> Any:
        # HybridChunker gives this to semchunk to split text that's too
        # long, and semchunk takes a function that counts tokens.
        return count_words


class RowTableSerializer(BaseTableSerializer):
    """
    Writes a table one row to a line.

    docling-core's TripletTableSerializer writes every cell as "row label,
    column label = value", all on one line. When a table has no header
    row, Docling numbers its columns, which gives "Decision maker, 1 =
    City Treasurer". Decision notices are mostly tables like that, of
    labels and their values, so a table with no header row is written as
    "Decision maker: City Treasurer". A table with a header row is written
    as TripletTableSerializer writes it, but with a line for each row, so
    that a long table can be split between rows.
    """

    def serialize(
        self,
        *,
        item: TableItem,
        doc_serializer: BaseDocSerializer,
        doc: DoclingDocument,
        **kwargs,
    ) -> SerializationResult:
        parts = []
        captions = doc_serializer.serialize_captions(item=item, **kwargs)
        if captions.text:
            parts.append(captions)
        if item.self_ref not in doc_serializer.get_excluded_refs(**kwargs):
            lines = table_lines(item.export_to_dataframe(doc=doc))
            if lines:
                parts.append(create_ser_result(text="\n".join(lines), span_source=item))
        return create_ser_result(
            text="\n\n".join(part.text for part in parts), span_source=parts
        )


def table_lines(table) -> list[str]:
    """
    One line for each row of a table, from its pandas DataFrame.

    The first cell of a row is its label. Docling repeats the text of a
    cell spanning several columns in each of them, so a value that's the
    same as the label or the value before it is left out, as are empty
    cells.
    """
    if table.shape[1] == 0:
        return []
    # Docling numbers the columns of a table with no header row.
    has_header = not all(isinstance(column, int) for column in table.columns)
    column_labels = [_cell_text(column) for column in table.columns]
    if table.shape[0] == 0:
        return [" | ".join(label for label in column_labels if label)]

    lines = []
    for row in table.itertuples(index=False, name=None):
        cells = [_cell_text(value) for value in row]
        label = cells[0]
        values = []
        for column_label, value in zip(column_labels[1:], cells[1:]):
            if not value or value == label or (values and value == values[-1][1]):
                continue
            values.append((column_label, value))
        if has_header:
            line = ". ".join(
                f"{label}, {column_label} = {value}"
                if label
                else f"{column_label} = {value}"
                for column_label, value in values
            )
        elif label and values:
            line = f"{label}: " + " | ".join(value for _, value in values)
        else:
            line = label or " | ".join(value for _, value in values)
        if line:
            lines.append(line)
    return lines


def _cell_text(value) -> str:
    return " ".join(str(value).split())


class _SerializerProvider(ChunkingSerializerProvider):
    def get_serializer(self, doc: DoclingDocument) -> BaseDocSerializer:
        return ChunkingDocSerializer(doc=doc, table_serializer=RowTableSerializer())


@dataclass
class DocumentPart:
    """One chunk's worth of a document."""

    text: str
    #: The headings of the section it's in, outermost first.
    headings: list[str]
    #: The pages it's on.
    pages: list[int]


def split_document(
    document: DoclingDocument, max_words: int = 350
) -> list[DocumentPart]:
    chunker = HybridChunker(
        tokenizer=WordTokenizer(max_tokens=max_words),
        serializer_provider=_SerializerProvider(),
    )
    parts = []
    for chunk in chunker.chunk(document):
        text = chunk.text.strip()
        if not text:
            continue
        pages = sorted(
            {prov.page_no for item in chunk.meta.doc_items for prov in item.prov}
        )
        parts.append(DocumentPart(text, list(chunk.meta.headings or []), pages))
    return parts
