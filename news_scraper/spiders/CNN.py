import scrapy, news_scraper.utils as myparser
from news_scraper.items import NewsScraperItem
import random


from bs4 import BeautifulSoup
from urllib.parse import quote_plus


class CnnSpider(scrapy.Spider):
    name = "CNN"
    allowed_domains = ["cnnindonesia.com","www.cnnindonesia.com"]
    handle_httpstatus_list = [301]
    custom_settings = {
        "ITEM_PIPELINES": {
            "news_scraper.pipelines.DateFilterPipeline": 110,
        },
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "scrapy_useragents.downloadermiddlewares.useragents.UserAgentsMiddleware": 500,
        },
    }


    def generateUserAgent(self):
        return "Chrome/103.0.5060.129 Mobile Safari/537.36" + str(random.randint(1,100))

    def start_requests(self):
        keywords = getattr(self, "keyword", "").split(",") if getattr(self, "keyword", None) else []
        if not keywords:
            self.logger.warning("No keyword provided to CNN spider; nothing to crawl.")
            return

        for keyword in keywords:
            keyword = keyword.strip()
            if not keyword:
                continue
            q = quote_plus(keyword)
            # for cnn - encode the query to ensure spaces/special chars are safe
            url = f"https://www.cnnindonesia.com/api/v2/search?query={q}&start=0&limit=10"
            yield scrapy.Request(
                url=url,
                headers={"user-agent": self.generateUserAgent()},
                callback=self.parse_search,
                meta={"keyword": keyword, "source": "cnnindonesia.com"},
            )

    def parse_search(self, response):
        start = int(response.meta.get("start", 0))

        try:
            data = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse JSON from CNN API for %s: %s", response.url, e)
            return

        # debug log to see what the API returned
        items = data.get("data") or []
        message = data.get("message")
        self.logger.debug("CNN parse_search: keyword=%r start=%s returned %d items message=%r", response.meta.get("keyword"), start, len(items), message)

        if not items:
            return

        # schedule next page if present
        response.meta["start"] = start + 10
        q = quote_plus(response.meta.get("keyword"))
        next_url = f"https://www.cnnindonesia.com/api/v2/search?query={q}&start={response.meta['start']}&limit=10"
        yield scrapy.Request(
            url=next_url,
            callback=self.parse_search,
            headers={"user-agent": self.generateUserAgent()},
            meta=response.meta,
        )

        for article in items:
            # article may not have a valid url; guard for it
            url = article.get("url")
            if not url:
                continue
            yield scrapy.Request(
                url=url,
                headers={"user-agent": self.generateUserAgent()},
                callback=self.parse,
                meta=response.meta,
            )

    def parse(self, response):
        if "location" in response.headers:
            yield scrapy.Request(
                url=response.headers["location"].decode("utf-8"),
                callback=self.parse,
                headers={
                      'user-agent':self.generateUserAgent(),
                    },
                meta=response.meta,
            )
            return

        soup = BeautifulSoup(response.text, "html.parser")
        article = soup.find("div", {"class": "detail-text"})

        if article:
            title = soup.find("h1")
            title = title.text if title else ""

            publish_date = soup.select_one("h1+div+div")
            publish_date = publish_date.text if publish_date else ""

            author = soup.find("div", {"class": "author"})
            author = author.text if author else ""

            content = article.text.strip()

            item = NewsScraperItem()
            item["title"] = title
            item["publish_date"] = myparser.indo_to_datetime(publish_date)
            item["author"] = author
            item["content"] = (
                f"""
judul: {title}
author: {author}
tanggal: {publish_date}
{content}
""".strip()
            )
            item["keyword"] = response.meta["keyword"]
            item["source"] = response.meta["source"]
            item["link"] = response.url

            yield item
