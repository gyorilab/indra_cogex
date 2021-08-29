"""This module makes available an object model for representing Odinson
interpretations and for processing into this representation as well
as generating visualizations."""

import glob
import gzip
import json
import tqdm
import os.path

import networkx


class Token:
    def __init__(self, raw, word, tag, lemma, entity, chunk):
        self.raw = raw
        self.word = word
        self.tag = tag
        self.lemma = lemma
        self.entity = entity
        self.chunk = chunk

    def to_json(self):
        return {
            "raw": self.raw,
            "word": self.word,
            "tag": self.tag,
            "lemma": self.lemma,
            "entity": self.entity,
            "chunk": self.chunk,
        }

    def __str__(self):
        return (
            f"Token({self.raw}, {self.word}, {self.tag},"
            f"{self.lemma}, {self.entity}, {self.chunk})"
        )

    def __repr__(self):
        return str(self)


class Sentence:
    def __init__(self, sentence_data):
        ntokens = sentence_data["numTokens"]
        fields = sentence_data["fields"]
        token_data = [{} for _ in range(ntokens)]
        roots = None
        edges = None
        for field in fields:
            if field["$type"] == "ai.lum.odinson.TokensField":
                for idx in range(ntokens):
                    token_data[idx][field["name"]] = field["tokens"][idx]
            elif field["$type"] == "ai.lum.odinson.GraphField":
                edges = field["edges"]
                roots = field["roots"]
        self.tokens = [Token(**td) for td in token_data]
        if not roots and not edges:
            self.dependency_graph = None
        else:
            self.dependency_graph = make_graph(self.tokens, roots, edges)

    def draw_graph(self, fname):
        if self.dependency_graph:
            return draw_graph(self.dependency_graph, fname)

    def __str__(self):
        return "Sentence(%s)" % ", ".join([t.word for t in self.tokens])

    def __repr__(self):
        return str(self)


class Document:
    def __init__(self, document_data):
        self.doc_id = document_data["id"]
        self.metadata = document_data["metadata"]
        self.sentences = [Sentence(s) for s in document_data["sentences"]]

    def draw_graph(self, fname):
        graphs = []
        for idx, sentence in enumerate(self.sentences):
            if not sentence.dependency_graph:
                continue
            g = networkx.relabel_nodes(
                sentence.dependency_graph,
                mapping={
                    i: "%s:%s" % (idx, i) for i in range(len(sentence.dependency_graph))
                },
                copy=True,
            )
            graphs.append(g)
        joint_graph = networkx.compose_all(graphs)
        draw_graph(joint_graph, fname)

    def __str__(self):
        return f"Document({self.doc_id}, {len(self.sentences)} sentences)"

    def __repr__(self):
        return str(self)


def make_graph(tokens, roots, edges):
    nodes = []
    for idx, token in enumerate(tokens):
        node_data = token.to_json()
        node_data["label"] = token.raw
        nodes.append([idx, node_data])
    for root in roots:
        nodes[root][1]["root"] = True

    edges_to_add = []
    for s, t, label in edges:
        edges_to_add.append((s, t, {"label": label}))

    g = networkx.DiGraph()
    g.add_nodes_from(nodes)
    g.add_edges_from(edges_to_add)
    return g


def draw_graph(g, fname):
    g = g.copy()
    for node in g.nodes:
        if "\\" in g.nodes[node]["label"]:
            g.nodes[node]["label"] = g.nodes[node]["label"].replace("\\", "[backslash]")
    ag = networkx.nx_agraph.to_agraph(g)
    # Add some visual styles to the graph
    ag.node_attr["shape"] = "plaintext"
    ag.graph_attr["splines"] = True
    ag.graph_attr["rankdir"] = "TD"
    ag.draw(fname, prog="dot")
    return ag


def process_into_graphs(docs_path, cached=True):
    fnames = glob.glob(os.path.join(docs_path, "*.json.gz"))
    for fname in tqdm.tqdm(fnames):
        doc = process_document(fname)
        out_fname = os.path.join(docs_path, "%s.pdf" % doc.doc_id)
        if cached and os.path.exists(out_fname):
            continue
        try:
            doc.draw_graph(out_fname)
        except Exception as e:
            continue


def process_document(json_gz_path):
    with gzip.open(json_gz_path, "r") as fh:
        document_data = json.load(fh)
    return Document(document_data)
