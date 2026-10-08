import datetime
from urllib.parse import quote, unquote, urljoin, urlparse

from lgsf.decisions.exceptions import SkipDecisionException
from lgsf.decisions.scrapers import CustomHTMLDecisionsScraper


class Scraper(CustomHTMLDecisionsScraper):
    """
    Full Council decision notices.

    The council publishes one PDF per meeting, listed under a heading per
    year with the link text giving the day and month. Each notice covers
    every decision taken at that meeting, so one notice is one record here,
    with the PDF as its document.
    """

    # The list goes back to 2021 and is not always kept up to date, so a
    # one year window can come back empty. Notices are only a list entry
    # each, so reading them all is cheap.
    years_back = 6

    def get_decisions(self):
        soup = self.get_page(self.base_url)
        content = soup.select_one("#Main-Content-Container .col-xl-10")
        start, end = self.date_range

        for heading in content.select("h3"):
            year = heading.get_text(strip=True)
            if not year.isdigit():
                continue
            listing = heading.find_next_sibling("ul")
            if not listing:
                continue

            for item in listing.select("li"):
                link = item.select_one("a[href]")
                if not link:
                    continue
                day_month = link.get_text(" ", strip=True)
                try:
                    date = datetime.datetime.strptime(
                        f"{day_month} {year}", "%d %B %Y"
                    ).date()
                except ValueError:
                    self.console.log(
                        f"[yellow]Could not read date {day_month!r} {year}[/yellow]"
                    )
                    continue
                if not start <= date <= end:
                    continue

                # Some filenames contain en-dashes, which wreq sends in a
                # form the server 404s on, so encode them first.
                url = quote(urljoin(self.base_url, link["href"]), safe=":/%")
                # Text after the link marks a special meeting and its time,
                # e.g. "(Special) 6.30 p.m.", where there was more than one
                # meeting that day.
                note = item.get_text(" ", strip=True)[len(day_month) :].strip()
                yield {
                    "url": url,
                    # The document path, minus the shared prefix, is unique
                    # per meeting even on days with several special meetings.
                    "identifier": unquote(urlparse(url).path).split("/Council/", 1)[-1],
                    "date": date.isoformat(),
                    "note": note,
                    # The list gives no publication date. Notices go up
                    # within days of the meeting, so its date stands in for
                    # deciding when one has settled, but isn't recorded.
                    "published_date": date.isoformat(),
                    "raw": str(item),
                }

    def get_single_decision(self, decision_data):
        date = decision_data["date"]
        if not date:
            raise SkipDecisionException(decision_data["url"])

        day = datetime.date.fromisoformat(date)
        title = f"Council decision notice – {day.day} {day:%B %Y}"
        if decision_data["note"]:
            title = f"{title} {decision_data['note']}"

        decision = self.add_decision(
            decision_data["url"],
            identifier=decision_data["identifier"],
            title=title,
            date=date,
            decision_maker="Council",
        )
        decision.documents = [{"title": title, "url": decision_data["url"]}]
        decision.source = {"url": self.base_url}
        return decision

    def prettify_decision_str(self, decision_raw_str):
        return decision_raw_str["raw"]
