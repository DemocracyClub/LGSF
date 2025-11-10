from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


def decode_cfemail(hexstr: str) -> str:
    data = bytes.fromhex(hexstr)
    key = data[0]
    return bytes(b ^ key for b in data[1:]).decode("utf-8")


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "#COUNCILLORSLISTBYNAME_HTML",  # lol
        "councillor_css_selector": "tbody td a",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html["href"])
        soup = self.get_page(url).select_one("#COUNCILLORDETAIL_HTML")

        name = soup.select_one("h2").get_text(strip=True).replace("Councillor ", "")
        info_table = soup.select_one("table")
        division = info_table.select("tbody tr td")[1].get_text(strip=True)
        party = info_table.select("tbody tr td")[2].get_text(strip=True)

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )

        councillor.email = decode_cfemail(
            soup.select_one(".email-address span.__cf_email__")["data-cfemail"]
        )
        image = info_table.select_one("img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )

        return councillor
