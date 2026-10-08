"""
Turning council HTML into plain text, and plain text into chunks.

Paragraphs are separated by a blank line throughout. html_to_text()
produces text in that form and split_text() relies on it.
"""

import re
from typing import Callable, NamedTuple

from bs4 import BeautifulSoup
from bs4.element import NavigableString, PreformattedString

#: Elements that start a new paragraph. Anything not listed, such as the
#: spans Word wraps around every run of text, is treated as part of the
#: paragraph it's in. Tables are handled separately.
BLOCK_TAGS = {
    "address",
    "article",
    "aside",
    "blockquote",
    "body",
    "center",
    "dd",
    "div",
    "dl",
    "dt",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "html",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "ul",
}

#: Elements whose content is never text to keep.
SKIP_TAGS = {"head", "noscript", "script", "style", "template", "title"}

_WHITESPACE = re.compile(r"\s+")
_SPACES = re.compile(r" +")


def html_to_text(html: str) -> str:
    """
    Plain text with one paragraph per block element, separated by a
    blank line.

    Whitespace inside a paragraph is collapsed to single spaces, `<br>`
    becomes a line break, and paragraphs left empty (such as Word's
    `<p>&nbsp;</p>` spacers) are dropped. Each table row becomes one line
    with its cells separated by " | ".
    """
    builder = _TextBuilder()
    builder.walk(BeautifulSoup(html, "html.parser"))
    builder.flush()
    return "\n\n".join(text for _, text in builder.blocks)


class _TextBuilder:
    TEXT = "text"
    TABLE = "table"

    def __init__(self):
        #: (kind, text) pairs. The kind lets a table cell keep its own text
        #: and pass any table nested in it out to the enclosing table.
        self.blocks = []
        self._parts = []

    def flush(self):
        """End the current paragraph."""
        lines = (
            _SPACES.sub(" ", line).strip() for line in "".join(self._parts).split("\n")
        )
        text = "\n".join(line for line in lines if line)
        if text:
            self.blocks.append((self.TEXT, text))
        self._parts = []

    def walk(self, node):
        for child in node.children:
            if isinstance(child, PreformattedString):
                # Comments, doctypes and Word's <![if !supportLists]> markers
                continue
            if isinstance(child, NavigableString):
                # Source line breaks are only wrapping, so they become
                # spaces like any other whitespace, including &nbsp;.
                self._parts.append(_WHITESPACE.sub(" ", str(child)))
            elif child.name in SKIP_TAGS:
                continue
            elif child.name == "br":
                self._parts.append("\n")
            elif child.name == "table":
                self.flush()
                self.table(child)
            elif child.name in BLOCK_TAGS:
                self.flush()
                if child.name == "li" and child.parent.name == "ol":
                    # A browser draws these numbers; they aren't in the text.
                    self._parts.append(f"{_list_marker(child)} ")
                self.walk(child)
                self.flush()
            else:
                self.walk(child)

    def table(self, table):
        """
        One line per row, with cells separated by " | ".

        Word often puts a table inside a cell of another, sometimes as a
        one-cell wrapper for layout. Folding the inner table into the
        cell would run all its rows into one line, so it's written as its
        own block after the row it's in.
        """
        lines = []
        for row in table.find_all("tr"):
            if row.find_parent("table") is not table:
                continue
            cells = []
            nested = []
            for cell in row.find_all(["td", "th"], recursive=False):
                cell_builder = _TextBuilder()
                cell_builder.walk(cell)
                cell_builder.flush()
                cells.append(
                    " ".join(
                        text.replace("\n", " ")
                        for kind, text in cell_builder.blocks
                        if kind == self.TEXT
                    )
                )
                nested.extend(b for b in cell_builder.blocks if b[0] == self.TABLE)
            if any(cells):
                lines.append(" | ".join(cells).strip())
            if nested:
                self._add_table(lines)
                lines = []
                self.blocks.extend(nested)
        self._add_table(lines)

    def _add_table(self, lines):
        if lines:
            self.blocks.append((self.TABLE, "\n".join(lines)))


def _list_marker(li):
    """The number a browser would show for an `<li>` in an `<ol>`."""
    ol = li.parent
    try:
        start = int(ol.get("start", 1))
    except ValueError:
        start = 1
    number = start + len(li.find_previous_siblings("li"))
    list_type = ol.get("type", "1")
    if list_type in ("a", "A") and 1 <= number <= 26:
        letter = chr(ord("a") + number - 1)
        return f"{letter.upper() if list_type == 'A' else letter}."
    if list_type in ("i", "I") and number >= 1:
        numeral = _roman(number)
        return f"{numeral.upper() if list_type == 'I' else numeral}."
    return f"{number}."


def _roman(number):
    numerals = [
        (1000, "m"),
        (900, "cm"),
        (500, "d"),
        (400, "cd"),
        (100, "c"),
        (90, "xc"),
        (50, "l"),
        (40, "xl"),
        (10, "x"),
        (9, "ix"),
        (5, "v"),
        (4, "iv"),
        (1, "i"),
    ]
    out = ""
    for value, numeral in numerals:
        while number >= value:
            out += numeral
            number -= value
    return out


def count_words(text: str) -> int:
    return len(text.split())


#: A paragraph that records a decision. In minutes these are written in
#: capitals, usually at the start ("RESOLVED that Cabinet:") but sometimes
#: mid-sentence ("Council supported the motion and RESOLVED: That...").
_RESOLUTION = re.compile(r"\b(RESOLVED|RECOMMENDED)\b")

#: A list number: "1.", "(1)", "1)", "(a)", "a." or "iv."
_LIST_NUMBER = re.compile(r"\(?(?:\d{1,3}|[a-z]|[ivx]{1,5})[.)]")

#: A paragraph that's an item in a list: a list number or a bullet. Word's
#: bullets come through as "·" from the Symbol font.
_LIST_ITEM = re.compile(rf"^(?:{_LIST_NUMBER.pattern}|[·•\-–])(?:\s|$)")

#: A sentence ends at ., ! or ?, possibly followed by a closing quote or
#: bracket, then whitespace and something that can start a sentence.
_SENTENCE_END = re.compile(
    r"(?:(?<=[.!?])|(?<=[.!?][\"'”’)]))\s+(?=[\"'“‘(]?[A-Z0-9£])"
)


#: A paragraph that introduces what follows it: "REASON:", "The applicant
#: highlighted the following points:", "Resolved to:-".
_LEAD_IN = re.compile(r"(?::[-–]?|:?\s[-–])$")

#: A paragraph this short with no closing punctuation is usually a heading,
#: such as "Highways Services" or "Response provided by Councillor Smith".
_HEADING_MAX_WORDS = 15

#: At most this many headings are moved from the end of a chunk: a heading
#: and a lead-in, such as "Recommendation 3: Terms of Reference" followed
#: by "RESOLVED:". A longer run of short lines is more likely a list.
_MAX_HEADINGS_MOVED = 2


class _Piece(NamedTuple):
    text: str
    #: What goes between this piece and the one before it when both are
    #: in the same chunk, so that a paragraph split into sentences is put
    #: back together with spaces rather than blank lines.
    separator: str


def split_text(
    text: str,
    max_length: int = 350,
    overlap: int = 1,
    length: Callable[[str], int] = count_words,
) -> list[str]:
    """
    Split text into chunks of whole paragraphs, each at most `max_length`
    as measured by `length`.

    `length` counts words by default. Pass a count from the embedding
    model's tokenizer to measure in its tokens instead.

    The last `overlap` paragraphs of each chunk are repeated at the start
    of the next, if they fit. A paragraph containing RESOLVED or
    RECOMMENDED is kept together with the list items that follow it, so a
    resolution isn't split. A chunk doesn't end on a heading if it can be
    moved to the start of the next one. Anything still longer than
    `max_length` is split at line breaks, then sentences, then, as a last
    resort, words.
    """
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    pieces = []
    for unit in _resolution_units(paragraphs):
        pieces.extend(_split_to_fit(unit, "\n\n", max_length, length))
    return [_join(chunk) for chunk in _pack(pieces, max_length, overlap, length)]


def _resolution_units(paragraphs):
    """Group each resolution with the list items that follow it."""
    units = []
    i = 0
    while i < len(paragraphs):
        end = i + 1
        if _RESOLUTION.search(paragraphs[i]):
            while end < len(paragraphs) and _LIST_ITEM.match(paragraphs[end]):
                end += 1
        units.append("\n\n".join(paragraphs[i:end]))
        i = end
    return units


#: How to break up text that's too long, from the largest unit down, and
#: what to put back between the parts when they end up in the same chunk.
_SPLITTERS = (
    (lambda text: text.split("\n\n"), "\n\n"),
    (lambda text: text.split("\n"), "\n"),
    (lambda text: _sentences(text), " "),
    (lambda text: text.split(), " "),
)


def _split_to_fit(text, separator, max_length, length, level=0):
    if length(text) <= max_length or level == len(_SPLITTERS):
        return [_Piece(text, separator)]
    split, inner_separator = _SPLITTERS[level]
    parts = [part for part in split(text) if part.strip()]
    pieces = []
    for i, part in enumerate(parts):
        pieces.extend(
            _split_to_fit(
                part,
                separator if i == 0 else inner_separator,
                max_length,
                length,
                level + 1,
            )
        )
    return pieces


def _sentences(text):
    sentences = []
    for sentence in _SENTENCE_END.split(text):
        if sentences and _LIST_NUMBER.fullmatch(sentences[-1]):
            # "1. The Council..." splits after "1." otherwise.
            sentences[-1] = f"{sentences[-1]} {sentence}"
        else:
            sentences.append(sentence)
    return sentences


def _pack(pieces, max_length, overlap, length):
    """Fill chunks with pieces up to max_length, carrying `overlap` over."""
    chunks = []
    current = []
    #: How many pieces at the start of `current` repeat the previous chunk.
    repeated = 0
    for piece in pieces:
        if current and length(_join(current + [piece])) > max_length:
            # Move headings at the end of this chunk to the start of the
            # next, keeping at least one piece that isn't repeated.
            moved = []
            while (
                len(moved) < _MAX_HEADINGS_MOVED
                and len(current) - repeated > 1
                and _is_heading(current[-1])
                and length(_join(current[-1:] + moved + [piece])) <= max_length
            ):
                moved.insert(0, current.pop())
            chunks.append(current)
            carried = current[-overlap:] if overlap else []
            # Drop the overlap, oldest first, rather than overflow.
            while carried and length(_join(carried + moved + [piece])) > max_length:
                carried = carried[1:]
            current = carried + moved
            repeated = len(carried)
        current.append(piece)
    if current:
        chunks.append(current)
    return chunks


def _is_heading(piece):
    """
    Whether a piece introduces what comes after it, so shouldn't be left
    at the end of a chunk.

    Only whole paragraphs are considered. The parts of a paragraph split
    into sentences or words are separated by spaces. Table rows are short
    and rarely punctuated, so would nearly all look like headings.
    """
    text = piece.text
    if piece.separator == " " or "\n" in text or " | " in text:
        return False
    if _LEAD_IN.search(text):
        return True
    if _LIST_ITEM.match(text) or count_words(text) > _HEADING_MAX_WORDS:
        return False
    last = text.rstrip("\"'”’)]")[-1:]
    return bool(last) and last not in ".!?;,"


def _join(pieces):
    if not pieces:
        return ""
    return pieces[0].text + "".join(p.separator + p.text for p in pieces[1:])
