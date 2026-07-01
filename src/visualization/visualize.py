import os
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, avg, count

os.makedirs("outputs/charts", exist_ok=True)

spark = SparkSession.builder.appName("Visualization").master("local[*]").getOrCreate()

# NIH
nih = spark.read.parquet("data/sample_output/nih_harmonized")

cases = (
    nih.groupBy("district_canonical")
    .agg(spark_sum("case_count").alias("cases"))
    .orderBy("cases", ascending=False)
)

rows = cases.collect()

districts = [r["district_canonical"] for r in rows]
values = [r["cases"] for r in rows]

plt.figure(figsize=(10, 5))
plt.bar(districts, values)
plt.xticks(rotation=45)
plt.title("Disease Cases by District")
plt.tight_layout()
plt.savefig("outputs/charts/disease_cases.png")
plt.close()

# Weather
weather = spark.read.parquet("data/sample_output/weather_harmonized")

temps = weather.groupBy("district_canonical").agg(avg("temperature_c").alias("temp"))

rows = temps.collect()

districts = [r["district_canonical"] for r in rows]
values = [r["temp"] for r in rows]

plt.figure(figsize=(10, 5))
plt.bar(districts, values)
plt.xticks(rotation=45)
plt.title("Average Temperature")
plt.tight_layout()
plt.savefig("outputs/charts/temperature.png")
plt.close()

# NDMA
ndma = spark.read.parquet("data/sample_output/ndma_harmonized")

alerts = ndma.groupBy("district_canonical").agg(count("*").alias("alerts"))

rows = alerts.collect()

districts = [r["district_canonical"] for r in rows]
values = [r["alerts"] for r in rows]

plt.figure(figsize=(10, 5))
plt.bar(districts, values)
plt.xticks(rotation=45)
plt.title("Disaster Alerts")
plt.tight_layout()
plt.savefig("outputs/charts/disaster_alerts.png")
plt.close()

print("Charts generated successfully!")
