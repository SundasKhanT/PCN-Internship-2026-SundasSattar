from src.services.rss_service import get_latest_news
from src.config.kafka_config import get_kafka_producer
from src.config.settings import NEWS_TOPIC
from src.utils.monitoring import log_ingestion

producer = get_kafka_producer()

news = get_latest_news()

for article in news:
    producer.send(NEWS_TOPIC, article)

producer.flush()
log_ingestion("RSS/Media", records_attempted=len(news), records_succeeded=len(news))
print(f"✓ RSS News sent to Kafka! ({len(news)} articles)")
