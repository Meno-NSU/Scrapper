from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class ListUrlsSpider(CrawlSpider):
    name = "nsu_urls"

    allowed_domains = ["nsu.ru", "www.nsu.ru", "new-research.nsu.ru"]
    start_urls = [
        "https://new-research.nsu.ru/portal",
        "https://www.nsu.ru/n/",
    ]

    # Важное: настройки именно для этого паука
    custom_settings = {
        # Мимикрия под обычный браузер
        "USER_AGENT": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        ),
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        },
        # Рекомендуется для университета: уважать robots.txt
        "ROBOTSTXT_OBEY": True,
        # Чтобы не создавать лишнюю нагрузку
        "DOWNLOAD_DELAY": 1.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
    }

    rules = (
        Rule(
            LinkExtractor(
                allow_domains=allowed_domains,
                deny=(r"^https?://e-lib\.nsu\.ru/.*", r".*search\-filter.*"),
            ),
            callback="parse_item",
            follow=True,
        ),
    )

    def parse_item(self, response):
        yield {"url": response.url}

    # Иначе start_urls обрабатываются стандартным parse, а не parse_item
    def parse_start_url(self, response):
        yield from self.parse_item(response)
