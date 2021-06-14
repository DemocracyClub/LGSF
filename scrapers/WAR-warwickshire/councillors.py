from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://democratic.warwickshire.gov.uk/cmis5/Councillors.aspx"
    division_text = "Division:"
