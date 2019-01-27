from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = (
        "https://cmis.hullcc.gov.uk/cmis/YourCouncillors/Councillors.aspx"
    )
