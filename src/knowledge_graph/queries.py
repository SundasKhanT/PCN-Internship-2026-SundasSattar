from rdflib import Graph

g = Graph()
g.parse("outputs/knowledge_graph.ttl", format="turtle")

query = """
PREFIX ex: <http://pcn2026.org/kg/>

SELECT ?policy ?status
WHERE {
    ?policy a ex:Policy .
    ?policy ex:status ?status .
}
"""

results = g.query(query)

print("\nDistrict -> Disease\n")

for row in results:
    print(f"{row.policy.split('/')[-1]} -> {row.status}")
