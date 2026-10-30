from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    use_proxy = True
    http_lib = "playwright"
    # 403s or times out from AWS on every run, even through the proxy;
    # works from a home connection (Oct 2026).
    run_on_aws = False
