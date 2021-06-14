from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://cmispublic.walsall.gov.uk/cmis/Councillors.aspx"
    disabled = True
