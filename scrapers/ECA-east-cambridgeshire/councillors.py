from urllib.parse import urljoin

from lgsf.councillors.scrapers import PagedHTMLCouncillorScraper


class Scraper(PagedHTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".localgov-directory__content",
        "councillor_css_selector": "article",
        "next_page_css_selector": "a[rel=next]",
    }

    def get_next_link(self, soup):
        try:
            next_link = soup.select_one(self.list_page["next_page_css_selector"])
            if next_link:
                return urljoin(self.base_url, next_link["href"])
        except Exception:
            pass
        return None

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url).select_one("main")
        name = (
            soup.h1.get_text(strip=True)
            .replace("Councillor ", "")
            .replace("Cllr ", "")
            .strip()
        )

        # Ward and party are now in a ul list - last two items
        list_items = soup.select("ul li")
        if len(list_items) >= 2:
            division = list_items[-2].get_text(strip=True)
            party = list_items[-1].get_text(strip=True)
        else:
            division = "Unknown"
            party = "Unknown"

        councillor = self.add_councillor(
            url, identifier=url, name=name, division=division, party=party
        )

        # Email is now in a link with href starting with mailto
        email_link = soup.select_one("a[href^=mailto]")
        if email_link:
            councillor.email = email_link["href"].replace("mailto:", "")

        # Photo - look for img with Cllr in alt, or any img in the page
        photo_element = soup.select_one("img[alt*='Cllr'], img[alt*='cllr']")
        if not photo_element:
            # Fallback: find first img that's not a logo/icon
            imgs = soup.select("img")
            for img in imgs:
                src = img.get("src", "")
                # Skip logos, icons, and data URIs
                if not any(
                    x in src.lower()
                    for x in ["logo", "icon", "badge", "data:", "avatar"]
                ):
                    photo_element = img
                    break

        if photo_element and photo_element.get("src"):
            src = photo_element["src"]
            if not src.startswith("data:"):
                councillor.photo_url = urljoin(self.base_url, src)

        return councillor
