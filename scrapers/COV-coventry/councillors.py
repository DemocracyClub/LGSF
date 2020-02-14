from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    disabled = True

    base_url = "https://www.coventry.gov.uk/councillors/name"
