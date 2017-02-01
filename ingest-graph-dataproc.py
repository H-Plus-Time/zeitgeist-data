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
    for art_id, doc in zip(art_ids, nlp.pipe(abstracts, batch_size=10000, n_threads=3)):
        words = [(token.text, token.tag_) for token in doc if token.is_stop != True and token.is_punct != True]
        word_freq = tuple(Counter(words).items())
        yield tuple(map(lambda keyword: (keyword[0], art_id, keyword[1]), word_freq))

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
    return translate_to_graphson({"keyword": keyword_group[0]},
        keyword_id, {}, {"present_in": kw_edges}, "keyword")

temp_dir = tempfile.mkdtemp(prefix='googlestorage')

path_rdd = sc.sequenceFile('gs://zeitgeist-store-1234/pubmed_seq', minSplits=10)

pubmed_oa_all = path_rdd.sample(False, 0.001, 81).map(lambda p: extract_graph(p))

# Extract authors

# All information available at this point, just need to group author links
# pubmed_wrote_edges = pubmed_oa_all.flatMap(lambda p: p['wrote_edges']).groupBy(lambda v: v[0])
# author_nodes = pubmed_wrote_edges.map(gen_author_graphson)
# with jsonlines.open(os.path.join(temp_dir, "pubmed_authors.graphson"), "w") as writer:
#     writer.write_all(author_nodes.collect())
# Extract keywords
# pubmed_keywords = pubmed_oa_all.flatMap(lambda p: p['keywords']).distinct().keyBy(lambda v: v)

# keyword_edges = pubmed_oa_all.map(lambda p: p['abstract_edges']).groupBy(lambda v: v[0])
# keyword_nodes = keyword_edges.map(gen_keyword_graphson)
# with jsonlines.open(os.path.join(temp_dir, "pubmed_keywords.graphson"), "w") as writer:
#     writer.write_all(keyword_nodes.collect())

keywords = sc.parallelize(extract_keywords(pubmed_oa_all.map(lambda p: p['abstract_edge']).collect()))

keyword_nodes = keywords.map(gen_keyword_graphson)

with jsonlines.open(os.path.join(temp_dir, "pubmed_keywords.graphson"), "w") as writer:
    writer.write_all(keyword_nodes.collect())

# Extract articles
article_nodes = pubmed_oa_all.map(lambda p: p['article']).map(gen_art_graphson)
with jsonlines.open(os.path.join(temp_dir, "pubmed_articles.graphson"), "w") as writer:
    writer.write_all(article_nodes.collect())



paths = ['pubmed_articles.graphson', 'pubmed_authors.graphson', 'pubmed_keywords.graphson']
call(["gsutil", "-m", "cp", temp_dir + "/*son", "gs://zeitgeist-store-1234"])
# authors_mapped = sc.parallelize(list(map(lambda author: deposit_author(author), pubmed_authors)))
# article_ids = sc.parallelize(list(map(lambda article: deposit_article(article), pubmed_articles)))
# auth_we_edge = pubmed_wrote_edges.flatMap(lambda w_e: w_e).join(authors_mapped)
# auth_art_we_edge = auth_we_edge.flatMap(lambda w_e: w_e[1:]).join(article_ids).collect()
# wrote_edge_ids = list(map(lambda w_edge: deposit_wrote_edges(w_edge), auth_art_we_edge))

# print(article_ids[0])
# print(wrote_edge_ids)
# path_rdd.map(lambda p: pp.parse_pubmed_xml(p)).saveAsPickleFile('pubmed_oa.pickle') # or to save to pickle
# pprint.pprint(auth_art_we_edge[0])
# pprint.pprint(article_ids.collect()[0])
