import feedparser

RSS_FEEDS = [
    {"name": "Dawn", "url": "https://www.dawn.com/feeds/home"},
    {"name": "Express Tribune", "url": "https://tribune.com.pk/feed"},
    {"name": "The News", "url": "https://www.thenews.com.pk/rss/1/1"},
]


def get_latest_news():
    news = []
    for feed_cfg in RSS_FEEDS:
        feed = feedparser.parse(feed_cfg["url"])
        for entry in feed.entries[:5]:
            news.append(
                {
                    "source": feed_cfg["name"],
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                }
            )
    return news
