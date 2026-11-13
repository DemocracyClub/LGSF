from lgsf.councillors.scrapers import HTMLCouncillorScraper


def decode_cfemail(hexstr: str) -> str:
    data = bytes.fromhex(hexstr)
    key = data[0]
    return bytes(b ^ key for b in data[1:]).decode("utf-8")


class Scraper(HTMLCouncillorScraper):
    disabled = False
    list_page = {
        "container_css_selector": ".container .col-md-8",
        "councillor_css_selector": ".col-sm-4",
    }

    def get_single_councillor(self, councillor_html):
        url = "https://www.angus.gov.uk{}".format(
            councillor_html.find_all("a")[0]["href"]
        )
        identifier = url.split("/")[-1]
        name = councillor_html.find_all("a")[0].text
        division = councillor_html.find("div", {"class": "councillor-ward"}).text
        party = councillor_html.find("div", {"class": "councillor-party"}).text
        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )
        soup = self.get_page(url)
        # Cloudflare hides the address: decode it from data-cfemail.
        email = soup.select_one(".field--name-field-email [data-cfemail]")
        if email:
            councillor.email = decode_cfemail(email["data-cfemail"])
        else:
            mailto = soup.select_one(".field--name-field-email a[href^=mailto]")
            if mailto:
                councillor.email = mailto.get_text(strip=True)

        return councillor
