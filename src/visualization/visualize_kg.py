from rdflib import Graph
import networkx as nx
import matplotlib.pyplot as plt

# Load RDF knowledge graph
rdf_graph = Graph()
rdf_graph.parse("outputs/knowledge_graph.ttl", format="turtle")

# Convert to NetworkX
G = nx.DiGraph()

for s, p, o in rdf_graph:
    s = str(s).split("/")[-1]
    p = str(p).split("/")[-1]
    o = str(o).split("/")[-1]

    G.add_node(s)
    G.add_node(o)
    G.add_edge(s, o, label=p)

# Draw graph
plt.figure(figsize=(16, 12))

pos = nx.spring_layout(G, seed=42)

nx.draw_networkx_nodes(G, pos, node_size=800)
nx.draw_networkx_labels(G, pos, font_size=8)
nx.draw_networkx_edges(G, pos, arrows=True, arrowsize=15)

edge_labels = nx.get_edge_attributes(G, "label")
nx.draw_networkx_edge_labels(
    G,
    pos,
    edge_labels=edge_labels,
    font_size=6,
)

plt.title("Knowledge Graph")
plt.axis("off")

plt.savefig("outputs/kg_graph.png", dpi=300, bbox_inches="tight")

print("✓ Knowledge Graph image saved to outputs/kg_graph.png")

plt.show()
