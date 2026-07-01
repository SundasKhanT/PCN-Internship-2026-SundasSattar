# PCN Internship 2026, Task 3: Real Time Multi Source Data Integration

This repository contains my submission for Task 3 of the PCN Research Internship Assessment 2026. I built a producer consumer pipeline using Apache Kafka and Apache Spark Structured Streaming that ingests five different data sources related to disease surveillance, weather, disasters, climate policy, and news, and harmonizes them into a single normalized schema keyed by epidemiological week.

## What this project does

The pipeline has five independent producers, each publishing to its own Kafka topic:

1. NIH Pakistan weekly surveillance data (dengue, diarrhoea, ILI, malaria case counts by district)
2. PMD weather data, pulled live from the Open-Meteo API for each district
3. NDMA disaster alerts (floods, landslides, droughts, heatwaves)
4. Ministry of Climate Change policy updates
5. Media RSS feeds from Dawn, Express Tribune, and The News

A single Spark Structured Streaming job consumes all five topics, parses them against explicit schemas, tags each record with the correct temporal key (epidemiological week for surveillance and disaster data, calendar month for policy data since that is inherently periodic), resolves district name variants to a canonical form, geocodes them, and writes the harmonized output to partitioned Parquet files.

On top of that I built an entity linker that joins the NIH, weather, and NDMA harmonized outputs on district and epidemiological week, so you can see disease case counts next to the weather conditions and any concurrent disaster alerts for the same place and week. As a bonus I also built a small knowledge graph (RDF/Turtle) and a few summary charts from this linked data.

## Why I chose Task 3

I chose Task 3 over Task 1 and Task 2 because it is a systems engineering problem rather than a pure extraction problem, and that matched my background better. Given the time available I felt I could deliver a working, well tested pipeline for all five sources rather than a partial knowledge graph extraction from a large, messy document set.

## What is fully working, partially working, or stubbed out

Fully working:
- All five Kafka producers run end to end and publish real, schema validated messages.
- The Spark consumer reads all five topics, parses them, tags temporal keys correctly per the assessment's rule (week for NIH/NDMA/weather, month for policy), and writes partitioned Parquet output.
- District entity resolution (handling aliases like D.G Khan vs Dera Ghazi Khan) and geocoding through GeoPy and Nominatim, with a local cache and a fallback so the pipeline never silently drops coordinates.
- Cross source entity linking between NIH, weather, and NDMA data.
- Basic ingestion monitoring, logged per run to a JSONL file with success and failure counts.

Partially working or simplified due to the 62 hour window:
- The NIH surveillance producer generates synthetic but realistic data (seeded random case counts within plausible ranges for each disease) rather than parsing real NIH PDF bulletins. I did not have time to build a robust PDF table extractor for this source in addition to everything else, so I made a deliberate choice to simulate this source so the rest of the pipeline, including the temporal and entity linking logic, could be demonstrated end to end. This is disclosed here and in the technical report.
- The RSS producer pulls live from Dawn, Express Tribune, and The News, but I only keep the five most recent items per feed to keep the demo fast.
- HDFS was not stood up for this submission. Output is written to local partitioned Parquet, which is the same columnar format and partitioning scheme you would use on HDFS, just without an actual Hadoop cluster behind it, because setting up HDFS inside the time limit was not a good use of the remaining hours.

Not attempted:
- Nothing further on the knowledge graph side. Both Neo4j and Protege were used against the real output (see report/report.docx for screenshots of each).

## Repository structure

```
PCN-Internship-2026-SundasSattar/
  README.md                 this file
  report/                   technical report (PDF/DOCX) and architecture diagram
  src/                      all source code (producers, consumer, utils, entity linker, KG builder)
  outputs/                  harmonized Parquet output, linked dataset, charts, knowledge graph files
  dscrapped/                sample raw payloads from each source before harmonization
  demo_video/               link to the screen recording (see below)
  docker-compose.yml        Kafka and Spark services
  requirements.txt          Python dependencies
```

## How to run it

1. Install dependencies.
```
pip install -r requirements.txt
```

2. Start Kafka and Spark.
```
docker compose up -d
```

3. Run the producers (each can be run independently, any number of times).
```
python -m src.producers.nih_producer
python -m src.producers.weather_producer
python -m src.producers.ndma_producer
python -m src.producers.policy_producer
python -m src.producers.rss_producer
```

4. Start the Spark consumer. It listens to all five topics at once and writes harmonized Parquet to `data/sample_output/`.
```
python -m src.consumers.spark_consumer
```

5. Once at least the NIH and weather outputs exist, build the cross source linked dataset.
```
python -m src.processors.entity_linker
```

6. Optional: build the knowledge graph and charts from the harmonized output.
```
python -m src.knowledge_graph.build_kg
python -m src.visualization.visualize
```

7. Check ingestion health at any point.
```
python -m src.utils.monitoring
```

## Assumptions

- Kafka and Spark run locally through Docker on `localhost:9092` and `localhost:7077`.
- The set of districts covered is a representative sample of seven (Lahore, Dera Ghazi Khan, Rahim Yar Khan, Karachi, Peshawar, Quetta, Islamabad) rather than all districts of Pakistan, again to keep runtime manageable within 62 hours.
- "Real time" for weather means a live call to Open-Meteo's current weather endpoint at the time the producer runs, not a continuously polling stream, since Open-Meteo itself is a request/response API rather than a push feed.

## Demo video

[Loom Demonstration Video](https://www.loom.com/share/38d4d43aef57404c90c89c6c1c6793e2)





