import scrapy
import re
from bs4 import BeautifulSoup
from news_scraper.items import NewsScraperItem
from news_scraper.utils import *
from urllib.parse import urlsplit, quote_plus
import json
from urllib.parse import urlencode


class MongabaySpider(scrapy.Spider):
    name = "Mongabay"
    allowed_domains = ["mongabay.co.id"]

    custom_settings = {
        "ITEM_PIPELINES": {
            "news_scraper.pipelines.DateFilterPipeline": 110,
        },
    }

    def start_requests(self):
        kw = getattr(self, "keyword", None)
        if not kw:
            self.logger.warning("No keyword provided to MongabaySpider; nothing to crawl.")
            return

        keywords = [k.strip() for k in kw.split(",") if k.strip()]
        for keyword in keywords:
            q = quote_plus(keyword)
            # primary: query the site's GraphQL endpoint (returns JSON of content nodes)
            graphql_query = (
                'query{contentNodes(where:{status:PUBLISH,search:"%s",contentTypes:[POST,SHORT_ARTICLE,VIDEOS,PODCASTS,SPECIALS]}first:24){edges{node{...on Post{__typename,title,link,date,byline{nodes{name}}}...on ShortArticle{__typename,title,link,date,byline{nodes{name}}}...on Video{__typename,title,link,date,byline{nodes{name}}}...on Podcast{__typename,title,link,date,byline{nodes{name}}}}}}}'
                % keyword.replace('"', '\\"')
            )
            graphql_url = f"https://www.mongabay.co.id/graphql?query={quote_plus(graphql_query)}"

            meta = {"keyword": keyword, "source": "mongabay.co.id"}
            yield scrapy.Request(url=graphql_url, callback=self.parse_graphql, meta=meta)

            # fallback: also request the regular search page (rendered via Playwright) to preserve prior behavior
            page_url = f"https://www.mongabay.co.id/page/1?s={q}"
            page_meta = dict(meta)
            page_meta.update({
                "playwright": True,
                "playwright_page_methods": [
                    {"name": "wait_for_selector", "args": ["article"], "kwargs": {"timeout": 5000}}
                ],
            })
            yield scrapy.Request(url=page_url, callback=self.parse_search, meta=page_meta)

    def parse_search(self, response):
        soup = BeautifulSoup(response.text, "html.parser")

        articles = soup.select("article.post-news")
        if not articles:
            articles = soup.select("article") or soup.select(".post")

        self.logger.debug(f"parse_search: found {len(articles)} article elements for {response.url}")

        for article in articles:
            a = article.select_one(".post-title-news a") or article.select_one("a")
            if a is None:
                continue
            href = a.get("href")
            if not href:
                continue

            yield response.follow(url=href, callback=self.parse, meta=response.meta)

        pagings = soup.select("a.page-numbers") or soup.select("a.next") or []
        for paging in pagings:
            href = paging.get("href")
            if not href:
                continue

            yield response.follow(url=href, callback=self.parse_search, meta=response.meta)
        # look for links that look like article URLs (contain a year or date) and follow them.
        if not articles:
            anchors = soup.select("a[href]")
            candidates = []
            seen = set()
            for a in anchors:
                href = a.get("href")
                if not href:
                    continue
                # normalize relative links
                href = response.urljoin(href)
                if href in seen:
                    continue
                seen.add(href)

                # prefer links on the same domain
                if "mongabay.co.id" not in href:
                    continue

                # heuristics: article URLs often contain a 4-digit year or /YYYY/ pattern
                if re.search(r"/\d{4}/", href) or re.search(r"/\d{4}-\d{2}-\d{2}/", href):
                    candidates.append(href)

            self.logger.debug(f"parse_search fallback: found {len(candidates)} candidate article links")
            for href in candidates:
                yield response.follow(url=href, callback=self.parse, meta=response.meta)

    def parse_graphql(self, response):
        """Parse the GraphQL JSON and follow article links found in contentNodes.edges.node.link"""
        try:
            data = json.loads(response.text)
        except Exception:
            self.logger.exception("Failed to parse GraphQL response for %s", response.url)
            return

        edges = data.get("data", {}).get("contentNodes", {}).get("edges", [])
        self.logger.debug(f"parse_graphql: found {len(edges)} edges for {response.url}")
        for edge in edges:
            node = edge.get("node") or {}
            link = node.get("link")
            title = node.get("title")
            date = node.get("date")
            # author: try byline.nodes[0].name
            author = None
            byline = node.get("byline", {}).get("nodes") if node.get("byline") else None
            if byline and isinstance(byline, list) and len(byline) > 0:
                author = byline[0].get("name")

            if not link:
                continue

            # follow article page to get full content; pass known metadata so parse() can use it
            yield response.follow(
                url=link,
                callback=self.parse,
                meta={"keyword": response.meta.get("keyword"), "source": response.meta.get("source"), "publish_date_override": date, "author_override": author},
            )

    def parse(self, response):
        soup = BeautifulSoup(response.text, "html.parser")

        try:
            title_el = soup.select_one("#headline .article-headline h1")
            title = title_el.text.strip() if title_el else ""

            content_el = soup.select_one("#main")
            content = content_el.text.strip() if content_el else ""

            author_el = soup.select_one(".single-article-meta a")
            author = author_el.text.strip() if author_el else ""

            el = soup.select_one(".single-article-meta")
            publish_date = None
            if el:
                for i in el.find_all():
                    i.decompose()
                raw_date = el.text.strip()
                splitted = raw_date.split(" ")
                if len(splitted) >= 3:
                    date_str = f"{splitted[-3]} {splitted[-2]} {splitted[-1]}"
                    try:
                        publish_date = indo_to_datetime(date_str, format="%d %B %Y")
                    except Exception:
                        publish_date = None

            # apply overrides from GraphQL if provided
            author_override = response.meta.get("author_override")
            if author_override:
                author = author_override

            publish_override = response.meta.get("publish_date_override")
            if publish_override and not publish_date:
                try:
                    # try to parse override (ISO or other) to the project's string format
                    dt = string_to_datetime(publish_override)
                    if dt:
                        publish_date = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    publish_date = publish_override

        except Exception as e:
            self.logger.exception("Error parsing article %s: %s", response.url, e)
            return

        self.logger.debug("parse: title=%r author=%r publish_date=%r", title, author, publish_date)

        # Only yield if have at least a title or content
        if not title and not content:
            self.logger.debug("Skipping empty article page: %s", response.url)
            return

        item = NewsScraperItem()
        item["title"] = title
        item["link"] = response.url
        item["content"] = content
        item["author"] = author
        item["publish_date"] = publish_date
        item["keyword"] = response.meta.get("keyword")
        item["source"] = response.meta.get("source")

        yield item
