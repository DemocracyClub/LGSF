class SkipCouncillorException(ValueError):
    """
    An exception class used to skip the saving / scraping of a councillor
    for some reason.

    This could be because some councils use "no councillor" as fake people to
    show that vacancies, or in some cases because a single source website
    contains information about more than one council, meaning we need to skip
    some people
    """
    pass
