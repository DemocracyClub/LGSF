"""
Councils that share one website.

Some councils share a management team and one democracy website: Adur and
Worthing have a single ModernGov install, democracy.adur-worthing.gov.uk,
holding both councils' committees, meetings and decisions. Each council's
scraper reads the whole site, so each has to leave out what is only the
other's.

The site doesn't say which council a committee or decision belongs to, but
the names do: "Worthing Planning Committee", "Joint Strategic Sub-Committee
(Adur)", "Adur Cabinet Member for Housing, Worthing Cabinet Member for
Housing". What names only the other council is the other council's. What
names this one, both, or neither (a joint committee, an officer) is kept.
"""

from __future__ import annotations

import re


def _named(name: str, text: str) -> bool:
    return re.search(rf"\b{re.escape(name)}\b", text, re.I) is not None


def only_other_councils(text: str | None, council: str, others) -> bool:
    """
    True if `text` names one of `others` and doesn't name `council`.

    >>> only_other_councils("Worthing Planning Committee", "Adur", ["Worthing"])
    True
    >>> only_other_councils("Joint Strategic Committee", "Adur", ["Worthing"])
    False
    >>> only_other_councils(
    ...     "Adur Leader, Worthing Leader", "Adur", ["Worthing"]
    ... )
    False
    """
    if not text:
        return False
    if _named(council, text):
        return False
    return any(_named(other, text) for other in others)
