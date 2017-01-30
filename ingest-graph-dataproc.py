import pandas as pd
import pubmed_parser as pp
import pprint
from pyspark import SparkConf, SparkContext
import asyncio
from itertools import chain
import uvloop
import boto
import os
import shutil
import json
from io import StringIO
import tempfile
import time

# URI scheme for Cloud Storage.
GOOGLE_STORAGE = 'gs'
# URI scheme for accessing local files.
LOCAL_FILE = 'file'

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from goblin import driver
from goblin.driver.serializer import GraphSONMessageSerializer
from gremlin_python.process.traversal import T

conf = (SparkConf()
    .set('spark.executor.cores', 2))
sc = SparkContext(conf = conf)

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
    id_tuple = (pmid, pmc, doi)
    author_list = []
    for author in data['author_list']:
        author_list.append(tuple(author[0:2] + [next(filter(lambda x: x[0] == author[-1], data['affiliation_list']), ['',''])[1]]))

    wrote_edges = []
    for i, author in enumerate(author_list):
        wrote_edges.append((author, id_tuple, i+1))

    abstract_words = textacy.Doc(data['abstract'], lang='en').to_bag_of_terms(
        ngrams=2, named_entities=True,lemmatize=True,as_strings=True
    )
    abstract_keywords = tuple(abstract_words.keys())
    abstract_art_edges = []
    for k,v in abstract_keywords.items():
        abstract_art_edges.append((k, id_tuple, v))
    return {"article": id_tuple, "authors": tuple(author_list),
            "abstract_edges": tuple(abstract_art_edges),
            "keywords": abstract_keywords, "wrote_edges": tuple(wrote_edges)}

def translate_to_graphson(obj):
    properties = {}
    for i, (k,v) in enumerate(obj.items()):
        properties[k] = [{"id": i, "value": v}]
    ret_obj = {"id": os.uname()[1] + str(int(time.time())),
                "outE": [], "inE": [],
                "properties": properties}

from subprocess import call
path_rdd = sc.sequenceFile('gs://zeitgeist-store-1234/pubmed_seq', minSplits=10)
# pubmed_articles = path_rdd.map(lambda p: extract_article(p)).collect()
# print(pubmed_articles)
pubmed_oa_all = path_rdd.map(lambda p: extract_graph(p))
pubmed_articles = pubmed_oa_all.map(lambda p: translate_to_graphson(p['article'])).collect()
print(pubmed_articles[0])
# write article graphjson

pubmed_authors = (pubmed_oa_all.flatMap(lambda p: p['authors']).distinct()
                    .map(lambda p: translate_to_graphson(p)).collect()
)
print(pubmed_authors[0])


# write author graphson
pubmed_wrote_edges = pubmed_oa_all.map(lambda p: p['wrote_edges']).collect()
print(pubmed_wrote_edges[0])
keyword_edges = pubmed_oa_all.map(lambda p: p['abstract_edges']).collect()
print(keyword_edges[0])

# Write all the edges!

temp_dir = tempfile.mkdtemp(prefix='googlestorage')
with open("pubmed_articles.graphson", "w") as f:
    json.dump(pubmed_articles, f)

with open("pubmed_authors.graphson", "w") as f:
    json.dump(pubmed_authors, f)

with open("pubmed_wrote_edges.json", "w") as f:
    json.dump(pubmed_wrote_edges, f)

for filename in ['pubmed_articles.graphson', 'pubmed_authors.graphson', 'pubmed_wrote_edges.json']:
    with open(os.path.join(temp_dir, filename), 'r') as localfile:

        dst_uri = boto.storage_uri(
            "zeitgeist-store-1234" + '/' + filename, GOOGLE_STORAGE)
        dst_uri.new_key().set_contents_from_file(localfile)
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
