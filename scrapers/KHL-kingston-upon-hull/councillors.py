from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://cmis.hullcc.gov.uk/cmis/CouncillorsandSeniorOfficers/CouncillorsandSeniorOfficers.aspx"
    division_text = "Constituency:"
