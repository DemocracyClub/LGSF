from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://mid-ulster.cmis-ni.org/midulster/Councillors.aspx"
    division_text = "DEA:"
