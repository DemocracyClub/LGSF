from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


def decode_cfemail(hexstr: str) -> str:
    data = bytes.fromhex(hexstr)
    key = data[0]
    return bytes(b ^ key for b in data[1:]).decode("utf-8")


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".list-councillors",
        "councillor_css_selector": ".col-4",
    }

    def get_single_councillor(self, councillor_html):
        councillor_url = councillor_html.select_one("a")["href"]
        url = urljoin(self.base_url, councillor_url)
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)

        party, division = (
            soup.select_one(".councillor-party").get_text(strip=True).split(" | ")
        )
        councillor = self.add_councillor(
            url=url, identifier=url, name=name, division=division, party=party
        )

        container = soup.select_one(".councillor-specific")

        # Emails are obfuscated by Cloudflare's email protection
        email = container.select_one(".councillor-email a[href*=email-protection]")
        if email:
            councillor.email = decode_cfemail(email["href"].split("#", 1)[1])

        image = container.select_one("img")
        if image:
            councillor.photo_url = image["src"]

        return councillor
