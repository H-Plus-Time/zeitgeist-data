import pandas as pd
import pubmed_parser as pp
from pprint import pprint
from pyspark import SparkConf, SparkContext
import asyncio
from itertools import chain
import uvloop
import boto
import os
from collections import Counter
import shutil
import json
import jsonlines
from io import StringIO
import tempfile
import textacy
import time
import random
from subprocess import call
import spacy                         # See "Installing spaCy"
import requests

# URI scheme for Cloud Storage.
GOOGLE_STORAGE = 'gs'
# URI scheme for accessing local files.
LOCAL_FILE = 'file'

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from goblin import driver
from goblin.driver.serializer import GraphSONMessageSerializer
from gremlin_python.process.traversal import T

conf = (SparkConf()
    .set('spark.driver.maxResultSize', '3g')
    .set('spark.executor.cores', 2))

sc = SparkContext(conf = conf)

def gen_id():
    return "{}-{}-{}".format(os.uname()[1], time.time(), random.random())

def extract_article(path):
    data = pp.parse_pubmed_xml(path)
    pmid = data.get('pmid', '')
    pmc = data.get('pmc', '')
    doi = data.get('doi', '')
    return (pmid, pmc, doi)

def extract_graph(k_v_pair):
    xml = bytes(k_v_pair[1])
    data = pp.parse_pubmed_xml(xml)
    pmid = data.get('pmid', '')
    pmc = data.get('pmc', '')
    doi = data.get('doi', '')
    id_tuple = [pmid, pmc, doi]
    author_list = []
    for author in data['author_list']:
        author_list.append(tuple(author[0:-1] + [next(filter(lambda x: x[0] == author[-1], data['affiliation_list']), ['',''])[1]]))

    # generate article ID, given articles are guaranteed to be unique
    article_id = gen_id()
    wrote_edges = []
    for i, author in enumerate(author_list):
        wrote_edges.append((author, article_id, i+1))


    return {"article": tuple([article_id] + id_tuple), "authors": tuple(author_list),
            "abstract_edge": (article_id, data['abstract']),"wrote_edges": tuple(wrote_edges)}

def extract_keywords(abstract_iter):

    nlp = spacy.load('en')
    ret_keywords = []
    art_ids, abstracts = zip(*abstract_iter)
    for art_id, doc in zip(art_ids, nlp.pipe(abstracts, batch_size=1000, n_threads=3)):
        words = [(token.text, token.tag_) for token in doc if token.is_stop != True and token.is_punct != True]
        word_freq = tuple(Counter(words).items())
        for keyword in word_freq:
            yield (keyword[0], art_id, keyword[1])

def translate_to_graphson(obj, id_str, inEdges={}, outEdges={}, label="vertex"):
    properties = {}
    for i, (k,v) in enumerate(obj.items()):
        properties[k] = [{"id": i, "value": v}]
    return {"id": id_str,
            "outE": inEdges, "inE": outEdges, "label": label,
            "properties": properties}

def gen_art_graphson(art):
    return translate_to_graphson({"pmid": art[0], "pmc": art[1],
                                "doi": art[2]}, art[0], {}, {}, "article")

def gen_author_graphson(author_group):
    author_id = gen_id()
    wrote_edges = list(map(lambda e: {"id": gen_id(), "inV": e[1],
                    "properties": {"precedence": e[2]}}, author_group[1]))
    if type(author_group[0]) == tuple and author_group[0][1] != None:
        first_name = author_group[0][1]
    else:
        first_name = ""

    if type(author_group[0]) == tuple and author_group[0][0] != None:
        sur_name = author_group[0][0]
    else:
        sur_name = ""

    return translate_to_graphson({"sur_name": sur_name,
        "first_name": first_name}, author_id, {}, {"wrote": wrote_edges}, "author")

def gen_keyword_graphson(keyword_group):
    keyword_id = gen_id()
    kw_edges = list(map(lambda e: {"id": gen_id(), "inV": e[1],
        "properties": {"count": e[2]}}, keyword_group[1]
    ))
    return translate_to_graphson({"keyword": keyword_group[0][0], "annotation": keyword_group[0][1]},
        keyword_id, {}, {"present_in": kw_edges}, "keyword")

temp_dir = tempfile.mkdtemp(prefix='googlestorage')

path_rdd = sc.sequenceFile('gs://zeitgeist-store-1234/pubmed_seq', minSplits=10)

pubmed_oa_all = sc.parallelize(path_rdd.take(10000)).map(lambda p: extract_graph(p))

# Extract authors

# All information available at this point, just need to group author links
pubmed_wrote_edges = pubmed_oa_all.flatMap(lambda p: p['wrote_edges']).groupBy(lambda v: v[0])
author_nodes = pubmed_wrote_edges.map(gen_author_graphson)
with jsonlines.open(os.path.join(temp_dir, "pubmed_authors.graphson"), "w") as writer:
    writer.write_all(author_nodes.collect())
# abstract_bulk = pubmed_oa_all.map(lambda p: p['abstract_edge']).collect()
# extracts = []
# for i in range(0, len(abstract_bulk), 10000):
#     print("{}th of {} abstracts".format(i, len(abstract_bulk)))
#     extracts += list(extract_keywords(abstract_bulk[i:i+10000]))
# # extracts = list(extract_keywords(keyword_bulk))
# keywords = sc.parallelize(extracts)
#
# keyword_nodes = keywords.groupBy(lambda v: v[0]).map(gen_keyword_graphson)
#
# with jsonlines.open(os.path.join(temp_dir, "pubmed_keywords.graphson"), "w") as writer:
#     writer.write_all(keyword_nodes.collect())

with open(os.path.join(temp_dir,"wrote_edges.json"), "w") as f:
    json.dump(pubmed_oa_all.flatMap(lambda p: p['wrote_edges']).collect(), f)

# Extract articles
article_nodes = pubmed_oa_all.map(lambda p: p['article']).map(gen_art_graphson)
with jsonlines.open(os.path.join(temp_dir, "pubmed_articles.graphson"), "w") as writer:
    writer.write_all(article_nodes.collect())



paths = ['pubmed_articles.graphson', 'pubmed_authors.graphson', 'pubmed_keywords.graphson', 'wrote_edges.json']
call(["gsutil", "-m", "cp", temp_dir + "/*son", "gs://zeitgeist-store-1234"])
