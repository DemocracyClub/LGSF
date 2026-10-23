"""
The per-council scraper types the AWS run knows about, in the order a
council's types run.

Deliberately free of imports: the CDK app reads this to make a log group
per type, and it runs without the scraper dependencies installed.

Each type maps to its settings:

- ``daily``: included in the scheduled run, and in any run that doesn't
  name its types. A type that isn't daily can still be run on request,
  e.g. ``manage.py minutes --aws --council KIR``, which is how a new type
  is tried in AWS before it joins the schedule.

The daily run takes each council in turn and runs its types one at a time,
in this order, so no two scrapes of one council ever write to its
repository at once. Adding a type here is all it takes to run it in AWS:
the state machine reads the list at run time.
"""

SCRAPER_TYPES: dict[str, dict] = {
    "councillors": {"daily": True},
    "minutes": {"daily": False},
    "decisions": {"daily": False},
}


def daily_scraper_types() -> list[str]:
    """The types the scheduled run does, in run order."""
    return [name for name, config in SCRAPER_TYPES.items() if config.get("daily")]


def ordered(scraper_types) -> list[str]:
    """The given types in run order, refusing any that aren't registered."""
    wanted = list(dict.fromkeys(scraper_types))
    unknown = [t for t in wanted if t not in SCRAPER_TYPES]
    if unknown:
        raise ValueError(
            f"Unknown scraper type(s): {', '.join(unknown)}. "
            f"Registered: {', '.join(SCRAPER_TYPES)}"
        )
    return [t for t in SCRAPER_TYPES if t in wanted]
