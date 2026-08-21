class SkipMeetingException(Exception):
    """Raised by a scraper to drop a meeting from the run entirely."""


class MeetingNotModifiedException(Exception):
    """
    Raised when the server answered 304 to a conditional request for a
    meeting, meaning what we already have stored is still current and there
    is nothing to re-scrape.
    """
