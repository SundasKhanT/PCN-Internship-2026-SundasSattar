import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from rdflib import Graph, RDF

# Load variables from .env into environment
load_dotenv()

URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

g = Graph()
g.parse("outputs/knowledge_graph.ttl", format="turtle")


def clean(uri):
    return str(uri).split("/")[-1]


with driver.session() as session:

    session.run("MATCH (n) DETACH DELETE n")

    for s, p, o in g:

        s_name = clean(s)
        p_name = clean(p)
        o_name = clean(o)

        if "hasDisease" in p_name:
            session.run(
                """
                MERGE (d:District {name:$d})
                MERGE (di:Disease {name:$di})
                MERGE (d)-[:HAS_DISEASE]->(di)
                """,
                d=s_name,
                di=o_name,
            )

        elif "hasDisaster" in p_name:
            session.run(
                """
                MERGE (d:District {name:$d})
                MERGE (dis:Disaster {name:$dis})
                MERGE (d)-[:HAS_DISASTER]->(dis)
                """,
                d=s_name,
                dis=o_name,
            )

        elif "title" in p_name:
            session.run(
                """
                MERGE (p:Policy {name:$p})
                """,
                p=s_name,
            )

print("Knowledge Graph imported into Neo4j!")

driver.close()
