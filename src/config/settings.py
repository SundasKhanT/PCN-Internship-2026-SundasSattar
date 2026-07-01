KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

# Kafka topics — one per source
NIH_TOPIC = "nih-surveillance-topic"
WEATHER_TOPIC = "weather-topic"
NDMA_TOPIC = "ndma-alerts-topic"
NEWS_TOPIC = "news-topic"
POLICY_TOPIC = "policy-topic"

# Open-Meteo base URL (lat/lon appended per-district at request time)
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Districts we collect weather for (raw names on purpose — exercises entity resolution)
DISTRICTS = [
    {"name": "Lahore", "lat": 31.5497, "lon": 74.3436},
    {"name": "D.G Khan", "lat": 30.0561, "lon": 70.6346},
    {"name": "R. Y Khan", "lat": 28.4202, "lon": 70.2952},
    {"name": "Karachi", "lat": 24.8607, "lon": 67.0011},
    {"name": "Peshawar", "lat": 34.0151, "lon": 71.5249},
    {"name": "Quetta", "lat": 30.1798, "lon": 66.9750},
    {"name": "Islamabad", "lat": 33.6844, "lon": 73.0479},
]
