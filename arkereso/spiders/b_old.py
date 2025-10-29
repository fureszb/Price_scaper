import scrapy
from scrapy_playwright.page import PageMethod

class BauhausSpider(scrapy.Spider):
    #name = "bauhaus"

    def __init__(self, termek="", enable_ai_validation=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.termek = termek
        self.enable_ai_validation = enable_ai_validation

    def start_requests(self):
        url = f"https://www.bauhaus.hu/prefixbox/search?q={self.termek}"

        yield scrapy.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
            },
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", ".prefixbox-product-container")
                ],
            },
            callback=self.parse,
        )

    def parse(self, response):
        products = response.css(".prefixbox-product-container")

        if not products:
            self.logger.warning(f"❌ Nincs találat a Bauhaus oldalon: {self.termek}")

        for product in products:
            nev = product.css(".prefixbox-product-name span::text").get()
            ar = product.css(".prefixbox-product-price::text").get()
            url = product.css(".prefixbox-product-name::attr(href)").get()

            if nev and ar:
                ar = ar.replace("\xa0", "").replace("Ft", "").strip()
                if url and not url.startswith("http"):
                    url = "https://www.bauhaus.hu" + url

                yield {
                    "aruhaz": "Bauhaus",
                    "termek": self.termek,
                    "nev": nev.strip(),
                    "ar": ar,
                    "url": url,
                }
