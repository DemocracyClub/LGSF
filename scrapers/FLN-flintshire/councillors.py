from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://cyfarfodyddpwyllgor.siryfflint.gov.uk"
    verify_requests = False
