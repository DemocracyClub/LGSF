from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = (
        "https://southlanarkshire.cmis.uk.com/southlanarkshire/Councillors.aspx"
    )
