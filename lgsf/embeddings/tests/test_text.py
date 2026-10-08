from lgsf.embeddings.text import html_to_text, split_text

# The shape ModernGov's Word-generated minutes take: list numbers inside
# spacer spans, and an empty &nbsp; paragraph between real ones.
WORD_LIST_HTML = """
<div class="WordSection1">
<p style="font-weight: bold" class="MsoNormal">RESOLVED that
Cabinet:</p>
<p style="font-weight: bold" class="MsoNormal">&nbsp;</p>
<p class="MsoListParagraphCxSpFirst" style="text-indent:-18.0pt">
<span style=""><span style="">1.<span style=
"font:7.0pt &quot;Times New Roman&quot;">&nbsp;&nbsp;&nbsp;&nbsp;</span></span></span>
Notes the Government&rsquo;s July decision<span style="">&nbsp;</span></p>
<p class="MsoListParagraphCxSpLast" style="text-indent:-18.0pt">
<span style=""><span style="">2.<span style=
"font:7.0pt &quot;Times New Roman&quot;">&nbsp;&nbsp;&nbsp;&nbsp;</span></span></span>
Agrees the changes at Appendix E.</p>
</div>
"""


def words(n, word="word"):
    return " ".join([word] * n)


def sentence(n, word="word"):
    """
    n words ending in a full stop. Without one, a short paragraph is taken
    for a heading and kept with the paragraph after it.
    """
    return words(n, word) + "."


def test_list_numbers_survive():
    text = html_to_text(WORD_LIST_HTML)
    assert "1. Notes the Government’s July decision" in text.split("\n\n")
    assert "2. Agrees the changes at Appendix E." in text.split("\n\n")


def test_nbsp_paragraphs_are_dropped():
    assert html_to_text(WORD_LIST_HTML).split("\n\n") == [
        "RESOLVED that Cabinet:",
        "1. Notes the Government’s July decision",
        "2. Agrees the changes at Appendix E.",
    ]


def test_ordered_list_items_are_numbered():
    html = '<ol start="3"><li>Notes the report.</li><li>Agrees.</li></ol>'
    assert html_to_text(html) == "3. Notes the report.\n\n4. Agrees."


def test_br_is_a_line_break_within_a_paragraph():
    html = "<p>three themes:<br/>\n<b>Connect</b> (belonging),<br/><b>Grow</b></p>"
    assert html_to_text(html) == "three themes:\nConnect (belonging),\nGrow"


def test_table_rows_come_out_as_lines():
    html = """
    <p>Before</p>
    <table><tbody>
    <tr><td><p>Group</p></td><td><p>Funding
    requested</p></td></tr>
    <tr><td><p>Axewoods Co-Op</p></td><td><p>&pound;1,395</p></td></tr>
    <tr><td><p>&nbsp;</p></td><td><p>&nbsp;</p></td></tr>
    </tbody></table>
    <p>After</p>
    """
    assert html_to_text(html).split("\n\n") == [
        "Before",
        "Group | Funding requested\nAxewoods Co-Op | £1,395",
        "After",
    ]


def test_nested_table_is_its_own_block():
    # Word wraps tables in a one-cell layout table. Folding the inner
    # table into that cell would put every row on one line.
    html = """
    <table><tr><td>
      <table>
        <tr><td>Group</td><td>Decision</td></tr>
        <tr><td>Happi Beats</td><td>Agreed</td></tr>
      </table>
    </td></tr></table>
    """
    assert html_to_text(html) == "Group | Decision\nHappi Beats | Agreed"


def test_text_under_the_limit_is_one_chunk():
    text = "First paragraph.\n\nSecond paragraph."
    assert split_text(text, max_length=10) == [text]


def test_long_text_splits_with_overlap():
    paragraphs = [sentence(4, w) for w in ("a", "b", "c", "d", "e")]
    chunks = split_text("\n\n".join(paragraphs), max_length=12)
    assert chunks == [
        "\n\n".join(paragraphs[0:3]),
        "\n\n".join(paragraphs[2:5]),
    ]


def test_overlap_is_dropped_when_it_would_not_fit():
    paragraphs = [sentence(8, "a"), sentence(8, "b")]
    assert split_text("\n\n".join(paragraphs), max_length=10) == paragraphs


def test_resolved_block_is_not_split():
    # Packing paragraph by paragraph would put the break between 1. and 2.
    resolution = "\n\n".join(["RESOLVED:", "1. " + words(5), "2. " + words(5)])
    text = "\n\n".join([sentence(6, "intro"), resolution, sentence(5, "after")])
    chunks = split_text(text, max_length=15, overlap=0)
    assert chunks == [sentence(6, "intro"), resolution, sentence(5, "after")]


def test_oversized_resolution_falls_back_to_paragraphs():
    resolution = ["RESOLVED:", "1. " + words(9), "2. " + words(9)]
    chunks = split_text("\n\n".join(resolution), max_length=12, overlap=0)
    assert chunks == ["RESOLVED:\n\n1. " + words(9), "2. " + words(9)]


def test_oversized_paragraph_splits_at_sentences():
    sentences = [f"Sentence {n} has five words." for n in range(6)]
    chunks = split_text(" ".join(sentences), max_length=12, overlap=1)
    assert chunks == [
        " ".join(sentences[0:2]),
        " ".join(sentences[1:3]),
        " ".join(sentences[2:4]),
        " ".join(sentences[3:5]),
        " ".join(sentences[4:6]),
    ]


def test_sentence_split_keeps_list_number_with_its_sentence():
    text = "1. The first point. Then " + words(9) + "."
    chunks = split_text(text, max_length=10, overlap=0)
    assert chunks == ["1. The first point.", "Then " + words(9) + "."]


def test_oversized_table_splits_between_rows():
    rows = [f"Row {n} | " + words(4) for n in range(4)]
    chunks = split_text("\n".join(rows), max_length=14, overlap=0)
    assert chunks == ["\n".join(rows[0:2]), "\n".join(rows[2:4])]


def test_heading_moves_to_the_next_chunk():
    paragraphs = [sentence(6, "a"), sentence(6, "b"), "REASON:", sentence(6, "c")]
    chunks = split_text("\n\n".join(paragraphs), max_length=14, overlap=0)
    assert chunks == [
        "\n\n".join(paragraphs[0:2]),
        "\n\n".join(paragraphs[2:4]),
    ]


def test_heading_moves_after_the_overlap():
    paragraphs = [
        sentence(6, "a"),
        sentence(6, "b"),
        "Highways Services",
        sentence(6, "c"),
    ]
    chunks = split_text("\n\n".join(paragraphs), max_length=15, overlap=1)
    assert chunks == [
        "\n\n".join(paragraphs[0:2]),
        "\n\n".join(paragraphs[1:4]),
    ]


def test_heading_stays_if_it_would_not_fit_with_what_follows():
    paragraphs = [sentence(6, "a"), "REASON:", sentence(14, "c")]
    chunks = split_text("\n\n".join(paragraphs), max_length=14, overlap=0)
    assert chunks == ["\n\n".join(paragraphs[0:2]), paragraphs[2]]


def test_short_list_items_and_sentences_are_not_headings():
    for last in ["c. Play Park consolidation study", "The motion was carried."]:
        paragraphs = [sentence(6, "a"), last, sentence(6, "c")]
        chunks = split_text("\n\n".join(paragraphs), max_length=12, overlap=0)
        assert chunks == ["\n\n".join(paragraphs[0:2]), paragraphs[2]]


def test_table_rows_are_not_headings():
    rows = [f"Row {n} | " + words(4) for n in range(4)]
    text = "\n\n".join([sentence(6, "a"), "\n".join(rows)])
    chunks = split_text(text, max_length=20, overlap=0)
    assert chunks == [
        sentence(6, "a") + "\n\n" + "\n".join(rows[0:2]),
        "\n".join(rows[2:4]),
    ]


def test_at_most_two_headings_move():
    names = ["Highways", "Waste", "Parks"]
    paragraphs = [sentence(6, "a"), *names, sentence(6, "c")]
    chunks = split_text("\n\n".join(paragraphs), max_length=10, overlap=0)
    assert chunks == [
        "\n\n".join(paragraphs[0:2]),
        "\n\n".join(paragraphs[2:5]),
    ]


def test_oversized_sentence_splits_at_words():
    # Each word is a piece. None of them may be taken for a heading and
    # moved, or a chunk would be emptied into the next.
    chunks = split_text(words(25), max_length=10, overlap=0)
    assert chunks == [words(10), words(10), words(5)]


def test_length_function_is_used():
    text = "\n\n".join(["abcde.", "ghijk.", "mnopq."])
    chunks = split_text(text, max_length=14, overlap=0, length=len)
    assert chunks == ["abcde.\n\nghijk.", "mnopq."]
