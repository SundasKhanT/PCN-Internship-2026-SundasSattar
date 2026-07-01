from pathlib import Path

import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF

EX = Namespace("http://pcn2026.org/kg/")

g = Graph()
g.bind("ex", EX)


def load_parquet(folder):
    path = Path(folder)
    files = list(path.rglob("*.parquet"))

    if not files:
        print(f"No parquet found in {folder}")
        return pd.DataFrame()

    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


nih = load_parquet("data/sample_output/nih_harmonized")
weather = load_parquet("data/sample_output/weather_harmonized")
ndma = load_parquet("data/sample_output/ndma_harmonized")
policy = load_parquet("data/sample_output/policy_harmonized")


for _, row in nih.iterrows():
    district = EX["district/" + row["district_canonical"].replace(" ", "_")]
    disease = EX["disease/" + row["disease"]]

    g.add((district, RDF.type, EX.District))
    g.add((disease, RDF.type, EX.Disease))

    g.add((district, EX.hasDisease, disease))
    g.add((district, EX.caseCount, Literal(int(row["case_count"]))))


for _, row in weather.iterrows():
    district = EX["district/" + row["district_canonical"].replace(" ", "_")]

    g.add((district, EX.temperature, Literal(float(row["temperature_c"]))))
    g.add((district, EX.humidity, Literal(float(row["humidity_pct"]))))


for _, row in ndma.iterrows():
    district = EX["district/" + row["district_canonical"].replace(" ", "_")]
    disaster = EX["disaster/" + row["event_type"]]

    g.add((disaster, RDF.type, EX.Disaster))
    g.add((district, EX.hasDisaster, disaster))
    g.add((district, EX.severity, Literal(row["severity"])))


for _, row in policy.iterrows():
    policy_node = EX["policy/" + row["title"].replace(" ", "_")]

    g.add((policy_node, RDF.type, EX.Policy))
    g.add((policy_node, EX.title, Literal(row["title"])))
    g.add((policy_node, EX.status, Literal(row["status"])))


Path("outputs").mkdir(exist_ok=True)

g.serialize("outputs/knowledge_graph.ttl", format="turtle")
g.serialize("outputs/knowledge_graph.rdf", format="xml")

print("--------------------------------")
print("Knowledge Graph Built Successfully")
print(f"Total RDF Triples: {len(g)}")
print("--------------------------------")
