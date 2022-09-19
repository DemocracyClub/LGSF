from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://www.nottinghamshire.gov.uk/dms/Councillors/tabid/63/ScreenMode/Alphabetical/Default.aspx"
    division_text = "Division:"
