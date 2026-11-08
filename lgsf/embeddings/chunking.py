"""
Turning a converted document into Chunks.

Each chunk's header names the document and the section the chunk is
from, and nothing else: no council, no meeting, no decision, no date.
Those belong to the records that link the file, which can change
without the file changing.
"""

import hashlib
import json
import re

from lgsf.embeddings.formats import DocumentFormat
from lgsf.embeddings import records
from lgsf.embeddings.records import Chunk

#: The most words in a chunk. Words for now, until an embedding model's
#: tokenizer is used.
MAX_WORDS = 350

#: Chunks with fewer words than this aren't embedded. They're mostly a
#: line on its own, such as "Made by an Executive Member or Chief
#: Officer".
MIN_EMBED_WORDS = 15

#: What Word puts in front of a PDF's title when it makes one.
_WORD_PREFIX = re.compile(r"^\s*Microsoft Word\s*-\s*", re.IGNORECASE)
#: A Word file's extension left on the end of a title.
_WORD_SUFFIX = re.compile(r"\.docx?\s*$", re.IGNORECASE)
#: A title that's still a file name: ``report.pdf``, ``agenda_final.rtf``.
_FILENAME = re.compile(r"\.[A-Za-z0-9]{2,5}$")
#: Titles that say nothing: Word's and Acrobat's defaults.
_MEANINGLESS = re.compile(
    r"^(untitled|document\d*|doc\d*|blank\s*page)$", re.IGNORECASE
)


def meaningful_title(title: str | None) -> str | None:
    """
    A PDF's metadata Title, cleaned up, or None if there isn't one worth
    using. "Microsoft Word - Report.docx" becomes "Report"; "untitled",
    an empty title and a file name are None.
    """
    if not title:
        return None
    title = _WORD_PREFIX.sub("", title)
    title = _WORD_SUFFIX.sub("", title)
    title = " ".join(title.split())
    if not title or _MEANINGLESS.match(title) or _FILENAME.search(title):
        return None
    return title


def document_title(document, metadata_title: str | None = None) -> str | None:
    """
    The title a document's chunks are headed with: its metadata Title if
    that's meaningful, else the first heading-like item Docling found in
    it, else None.

    ``document`` is a DoclingDocument. docling-core is only imported if
    the metadata title isn't usable.
    """
    title = meaningful_title(metadata_title)
    if title:
        return title
    if document is None:
        return None
    from lgsf.embeddings.documents import first_heading

    return first_heading(document)


def header(title: str | None, headings: list[str]) -> str:
    """
    The header put in front of a chunk's text: the document's title on
    one line, and the section's headings joined with " · " on the next.
    A line that would be empty is left out, so the header can be empty.
    A heading that repeats the title isn't written twice.
    """
    title = " ".join((title or "").split())
    heading_line = " · ".join(
        heading
        for heading in (" ".join(h.split()) for h in headings)
        if heading and heading != title
    )
    return "\n".join(line for line in (title, heading_line) if line)


def inputs_hash(
    document_format: DocumentFormat,
    docling_version: str,
    content_hash: str,
    max_words: int = MAX_WORDS,
    min_embed_words: int = MIN_EMBED_WORDS,
) -> str:
    """
    A hash of everything a document's chunks are made from, apart from
    the converted document itself: the chunking settings and code
    version, the Docling version that converted it, its format and the
    file's hash. When it changes, the document is chunked again.
    """
    inputs = {
        "chunker_version": records.CHUNKER_VERSION,
        "max_words": max_words,
        "min_embed_words": min_embed_words,
        "docling_version": docling_version,
        "format": str(document_format),
        "content_hash": content_hash,
    }
    encoded = json.dumps(inputs, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def chunk_document(
    council: str,
    storage_key: str,
    document,
    title: str | None,
    max_words: int = MAX_WORDS,
    min_embed_words: int = MIN_EMBED_WORDS,
) -> list[Chunk]:
    """
    Split a DoclingDocument and wrap each part in a Chunk. This needs
    docling-core, from the embeddings dependency group.
    """
    from lgsf.embeddings.documents import count_words, split_document

    digest = storage_key.split(".", 1)[0]
    parts = split_document(document, max_words=max_words)
    return [
        Chunk(
            chunk_id=f"{council}:documents:{digest}:{index}",
            council=council,
            content_hash=f"sha256:{digest}",
            storage_key=storage_key,
            chunk_index=index,
            chunk_count=len(parts),
            header=header(title, part.headings),
            text=part.text,
            headings=part.headings,
            pages=part.pages,
            embed=count_words(part.text) >= min_embed_words,
        )
        for index, part in enumerate(parts)
    ]
