import abc
import datetime
import json
import re
from pathlib import Path
from typing import Sequence

from bs4 import BeautifulSoup

from lgsf.commands.base import Council
from lgsf.embeddings.models import Chunk, ChunkType, SourceType
from lgsf.embeddings.text import count_words, html_to_text, split_text
from lgsf.path_utils import data_abs_path


class ChunkerBase(abc.ABC):
    """
    Reads one council's scraped data from disk and turns it into chunks.

    Subclasses decide how to find and extract the text for one kind of
    source. They all produce the same Chunk record, built with
    make_chunks(), so the embedding step doesn't need to know where a
    chunk came from.
    """

    type: ChunkType
    source_type: SourceType

    #: Passed to split_text(). Words for now: change these once the
    #: embedding model is chosen, to its token limit (less room for the
    #: context header) and a count from its tokenizer.
    max_length = 350
    overlap = 1
    length = staticmethod(count_words)

    #: Where chunks are written, under the council's data directory.
    OUTPUT_DIR_NAME = "Embeddings"

    #: What one source record is, in the plural, for the summary.
    sources_name = "sources"

    def __init__(
        self,
        council_code: str,
        data_path: Path = None,
        since: datetime.date = None,
        until: datetime.date = None,
    ):
        self.council = council_code.upper()
        self.council_name = Council(self.council).name
        self.data_path = Path(data_path) if data_path else data_abs_path(self.council)
        #: Only sources dated on or after `since` and on or before `until`
        #: are chunked. Either can be None.
        self.since = since
        self.until = until
        #: How many source records (meetings, for minutes) the last call
        #: to get_chunks() read.
        self.sources_read = 0

    @abc.abstractmethod
    def has_data(self) -> bool:
        """
        Whether this council has any of this kind of source on disk.

        A council with none is skipped rather than given an empty file.
        """

    @abc.abstractmethod
    def get_chunks(self) -> Sequence[Chunk]:
        """Every chunk for this council from this kind of source."""

    def in_range(self, date: str | None) -> bool:
        """
        Whether an ISO date is within `since` and `until`.

        A source with no date is in range only if neither is set.
        """
        if self.since is None and self.until is None:
            return True
        if not date:
            return False
        day = datetime.date.fromisoformat(date)
        if self.since is not None and day < self.since:
            return False
        if self.until is not None and day > self.until:
            return False
        return True

    def parent_id(self, *parts) -> str:
        return ":".join([self.council, str(self.type), *parts])

    def make_chunks(
        self,
        parent_id: str,
        date: str | None,
        context: str,
        text: str,
        metadata: dict = None,
        embed: bool = True,
    ) -> list[Chunk]:
        """
        Split one source's text and wrap each part in a Chunk.

        A source with no text gets one chunk with no text, so that its
        header can still be found.
        """
        parts = split_text(
            text,
            max_length=self.max_length,
            overlap=self.overlap,
            length=self.length,
        ) or [""]
        return [
            Chunk(
                chunk_id=f"{parent_id}:{index}",
                parent_id=parent_id,
                council=self.council,
                type=self.type,
                source_type=self.source_type,
                date=date,
                context=context,
                text=part,
                chunk_index=index,
                chunk_count=len(parts),
                embed=embed,
                metadata=dict(metadata or {}),
            )
            for index, part in enumerate(parts)
        ]

    @property
    def output_path(self) -> Path:
        return self.data_path / self.OUTPUT_DIR_NAME / f"{self.type}.jsonl"

    def write(self, chunks: Sequence[Chunk]) -> Path:
        """
        Write chunks as JSON lines, replacing what was there.

        Sorted by chunk ID (with chunk 10 after chunk 9, not after chunk
        1) so that running again on unchanged data writes an identical
        file.

        This is plain file I/O rather than a StorageSession: in production
        the storage session commits to the per-council GitHub repos, and
        chunks shouldn't go there yet.
        """
        path = self.output_path
        path.parent.mkdir(parents=True, exist_ok=True)
        ordered = sorted(chunks, key=lambda c: (c.parent_id, c.chunk_index))
        path.write_text(
            "".join(f"{chunk.as_json()}\n" for chunk in ordered), encoding="utf-8"
        )
        return path


class ModGovMinutesChunker(ChunkerBase):
    """
    Chunks the minutes ModernGov publishes inside its meeting XML.

    The minutes scraper stores each meeting as `Minutes/json/<name>.json`
    and the GetMeeting XML it came from as `Minutes/raw/<name>.xml`. Each
    `<agendaitem>` in the XML carries that item's minutes as escaped HTML
    in `<minutesnonemptyhtmlbody>`, and each item becomes one or more
    chunks. The scraper's `-minutes.html` file joins these up but drops
    the minute numbers, item IDs and linked documents, so it isn't used.
    """

    type = ChunkType.MINUTES
    source_type = SourceType.MODERNGOV_XML
    sources_name = "meetings"

    #: Items with fewer words than this are mostly procedural ("There were
    #: none.") and aren't embedded, unless they're declarations of interest.
    min_embed_words = 15

    @property
    def minutes_path(self) -> Path:
        return self.data_path / "Minutes"

    def has_data(self) -> bool:
        return (self.minutes_path / "json").is_dir()

    def get_chunks(self) -> list[Chunk]:
        self.sources_read = 0
        chunks = []
        for meeting, detail in self.meetings():
            self.sources_read += 1
            for item in detail.find_all("agendaitem"):
                chunks.extend(self.chunks_for_item(meeting, item))
        return chunks

    def meetings(self):
        """
        (meeting JSON, meeting XML) for each meeting in range with
        published minutes.

        The JSON has what the XML lacks: the committee name and ID, and
        the meeting's web page.
        """
        for json_path in sorted((self.minutes_path / "json").glob("*.json")):
            meeting = json.loads(json_path.read_text())
            if not meeting.get("minutes_published"):
                continue
            if not self.in_range(meeting.get("raw_date")):
                continue
            xml_path = self.minutes_path / "raw" / f"{json_path.stem}.xml"
            if not xml_path.exists():
                continue
            soup = BeautifulSoup(xml_path.read_text(), features="xml")
            detail = soup.find("meeting")
            if detail is not None:
                yield meeting, detail

    def chunks_for_item(self, meeting, item) -> list[Chunk]:
        body = _tag_text(item, "minutesnonemptyhtmlbody")
        if not body:
            return []
        text = html_to_text(_AGENDA_ITEM_MARKER.sub("", body))
        if not text:
            return []

        meeting_id = meeting["raw_identifier"]
        agenda_item_id = _tag_text(item, "agendaitemid")
        title = _tag_text(item, "agendaitemtitle")
        minute_number = _tag_text(item, "minutesitemnumber") or _tag_text(
            item, "fulldisplaynumberformat"
        ).rstrip(".")
        date = meeting.get("raw_date")

        metadata = {
            "committee": meeting.get("raw_committee"),
            "committee_id": meeting.get("raw_committee_id"),
            "meeting_id": meeting_id,
            "meeting_url": meeting.get("url"),
            "agenda_item_id": agenda_item_id,
            "agenda_item_title": title,
            "minute_number": minute_number,
            "restricted": _tag_text(item, "isrestricted") == "True",
            "linked_attachment_ids": self.linked_attachment_ids(item),
        }
        embed = (
            count_words(text) >= self.min_embed_words or "declaration" in title.lower()
        )
        return self.make_chunks(
            parent_id=self.parent_id(meeting_id, agenda_item_id),
            date=date,
            context=self.context(
                meeting.get("raw_committee"), date, minute_number, title
            ),
            text=text,
            metadata=metadata,
            embed=embed,
        )

    def context(self, committee, date, minute_number, title):
        """
        For example:

            East Devon District Council · Cabinet · 16 September 2026
            Minute 36: LGR in Devon: Government Decision and Implications

        The date is written out so that a search for "September 2026"
        matches it.
        """
        first_line = " · ".join(
            part
            for part in (self.council_name, committee, _date_in_words(date))
            if part
        )
        if minute_number and title:
            second_line = f"Minute {minute_number}: {title}"
        elif minute_number:
            second_line = f"Minute {minute_number}"
        else:
            second_line = title
        return "\n".join(line for line in (first_line, second_line) if line)

    def linked_attachment_ids(self, item):
        """
        IDs of the documents linked to this item, in the order listed.

        These are how report PDFs will later be connected to the minutes
        that discussed them.
        """
        ids = []
        linked = item.find("linkeddocuments", recursive=False)
        if linked is None:
            return ids
        for tag in linked.find_all("attachmentid"):
            attachment_id = tag.text.strip()
            if attachment_id and attachment_id not in ids:
                ids.append(attachment_id)
        return ids


class DecisionChunker(ChunkerBase):
    """
    Chunks the decisions the decisions scraper stores as
    `Decisions/json/<date>-<identifier>.json`.

    Every decisions scraper writes the same JSON, whatever the CMS, so
    this one chunker reads them all. Each decision becomes one or more
    chunks of its `purpose` followed by its `text`, which the scraper
    writes as plain text with a blank line between paragraphs.
    """

    type = ChunkType.DECISIONS
    source_type = SourceType.DECISION_JSON
    sources_name = "decisions"

    @property
    def decisions_path(self) -> Path:
        return self.data_path / "Decisions"

    def has_data(self) -> bool:
        return (self.decisions_path / "json").is_dir()

    def decisions(self):
        """Each decision taken on a date in range, as the scraper stored it."""
        for json_path in sorted((self.decisions_path / "json").glob("*.json")):
            decision = json.loads(json_path.read_text())
            if self.in_range(decision.get("raw_date")):
                yield decision

    def get_chunks(self) -> list[Chunk]:
        self.sources_read = 0
        chunks = []
        for decision in self.decisions():
            self.sources_read += 1
            chunks.extend(self.chunks_for_decision(decision))
        return chunks

    def chunks_for_decision(self, decision) -> list[Chunk]:
        """
        A decision with neither a purpose nor text gets one chunk of its
        header alone. Its title and decision maker are all it has, apart
        from the documents linked from it.
        """
        title = decision.get("raw_title") or ""
        text = "\n\n".join(
            part.strip()
            for part in (decision.get("purpose"), decision.get("text"))
            if part and part.strip()
        )
        metadata = {
            "url": decision.get("url"),
            "title": title,
            "decision_maker": decision.get("raw_decision_maker"),
            "status": decision.get("status"),
            "publication_date": decision.get("publication_date"),
            "is_key_decision": decision.get("is_key_decision"),
            "is_officer_decision": decision.get("is_officer_decision"),
            "is_subject_to_call_in": decision.get("is_subject_to_call_in"),
            "document_hashes": self.document_hashes(decision),
        }
        return self.make_chunks(
            parent_id=self.parent_id(decision["raw_identifier"]),
            date=decision.get("raw_date"),
            context=self.context(decision),
            text=text,
            metadata=metadata,
            embed=not _PROCEDURAL_TITLE.match(" ".join(title.split())),
        )

    def context(self, decision):
        """
        For example:

            Manchester City Council · Strategic Director (Corporate Services) & City Treasurer · 6 August 2026
            Decision: P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation (2026/06/09A)

        A decision taken at a meeting has the meeting's date in front of
        its decision maker, as in "12/01/2026 - Cabinet". That's left out,
        because the header already has the date.
        """
        decision_maker = _MEETING_DATE.sub("", decision.get("raw_decision_maker") or "")
        first_line = " · ".join(
            part
            for part in (
                self.council_name,
                decision_maker.strip(),
                _date_in_words(decision.get("raw_date")),
            )
            if part
        )
        title = (decision.get("raw_title") or "").strip()
        second_line = f"Decision: {title}" if title else ""
        return "\n".join(line for line in (first_line, second_line) if line)

    def document_hashes(self, decision):
        """
        The content_hash of each document linked from the decision, in the
        order listed. These connect a decision to the chunks of its
        documents. Documents that weren't downloaded have no hash.
        """
        hashes = []
        for document in decision.get("documents") or []:
            content_hash = document.get("content_hash")
            if content_hash and content_hash not in hashes:
                hashes.append(content_hash)
        return hashes


#: Titles of decisions that are part of running a meeting rather than a
#: decision about anything: apologies, declarations of interest, approving
#: the minutes and urgent business. These decisions aren't embedded. A
#: title can start with an item number, as in "3. Minutes".
_PROCEDURAL_TITLE = re.compile(
    r"""
    ^(?:[\d.()a-z]{1,4}\s*[-.:]?\s+)?
    (?:
        apologies(?:\s+for\s+absence)?
        | (?:declarations?|disclosures?)\s+of\s+
          (?:personal\s+(?:and|&)\s+prejudicial\s+)?interests?(?:\s*\(.*\))?
        | minutes(?:\s+of\s+(?:the\s+)?previous\s+meetings?)?
        | urgent\s+(?:items|matters|business)(?:\s+as\s+agreed\s+by\s+the\s+chair)?
        | any\s+items\s+that\s+the\s+chair\s+has\s+decided\s+are\s+urgent
    )
    \.?$
    """,
    re.IGNORECASE | re.VERBOSE,
)

#: The date of the meeting a decision was taken at, in front of its
#: decision maker: "12/01/2026 - Cabinet".
_MEETING_DATE = re.compile(r"^\s*\d{1,2}/\d{1,2}/\d{4}\s*-\s*")


#: ModernGov marks where each agenda item starts and ends in the document
#: it builds minutes from, as "<AI9>" and "</AI9>". Some councils' minutes
#: keep these as text, escaped in the HTML.
_AGENDA_ITEM_MARKER = re.compile(r"&lt;/?AI\d+&gt;")


def _tag_text(parent, name):
    # Only direct children: linked documents have their own <isrestricted>
    # and <meetingdate> further down.
    tag = parent.find(name, recursive=False)
    return tag.text.strip() if tag else ""


def _date_in_words(iso_date):
    if not iso_date:
        return ""
    date = datetime.date.fromisoformat(iso_date)
    return f"{date.day} {date:%B %Y}"
