import pandas as pd
import pubmed_parser as pp
from pprint import pprint
import pyspark
import pyspark.sql.functions as F
from pyspark.conf import SparkConf
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, LongType
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

# conf = (SparkConf()
#     .set('spark.driver.maxResultSize', '3g')
#     .set('spark.executor.cores', 2))
#
# sc = SparkContext(conf = conf)
spark = (SparkSession.builder
    .config(conf=SparkConf().set('spark.driver.memory', '18g')
        .set('spark.driver.maxResultSize', '18g')
        .set('spark.rpc.message.maxSize', '2047')
    )
    .getOrCreate()
)
sc = spark.sparkContext
def convert_to_indexed(df):
    row = Row("char")
    row_with_index = Row("char", "index")
    schema  = StructType(
    df.schema.fields[:] + [StructField("index", LongType(), False)])

    return (df.rdd # Extract rdd
        .zipWithIndex() # Add index
        .map(lambda ri: row_with_index(*list(ri[0]) + [ri[1]])) # Map to rows
        .toDF(schema))

def gen_id():
    return "{}-{}-{}".format(os.uname()[1], time.time(), random.random())

def removekeys(d, keys):
    r = dict(d)
    for key in keys:
        del r[key]
    return r

def extract_article(data):
    art = {}
    art['pmid'] = data.get('pmid', '')
    art['pmc'] = data.get('pmc', '')
    art['doi'] = data.get('doi', '')
    art['full_title'] = data.get('full_title', '')
    art['publication_year'] = data.get('publication_year', '')
    return art

def extract_graph(k_v_pair):
    xml = bytes(k_v_pair[1])
    data = pp.parse_pubmed_xml(xml)
    art = extract_article(data)
    author_list = []
    for author in data['author_list']:
        affiliation = next(filter(lambda x: x[0] == author[-1],
                        data['affiliation_list']), ['',''])[1]
        if any(elem == None for elem in author[1:-1]):
            first_name = ""
        else:
            first_name = " ".join(author[1:-1])
        author_list.append({"sur_name": author[0],"first_name": first_name,
            "affiliation": affiliation})
    if len(author_list) == 0:
        author_list.append({"sur_name": "", "first_name": "", "affiliation": ""})

    # generate article ID, given articles are guaranteed to be unique
    wrote_edges = []
    for i, author in enumerate(author_list):
        wrote_edges.append({**author, **{"priority": i+1}, **art})

    return {"article": art, "authors": author_list, "wrote_edges": wrote_edges,
            "abstract_edge": (art, data['abstract'])}

def extract_keywords(abstract_iter):
    nlp = spacy.load('en')
    ret_keywords = []
    articles, abstracts = zip(*abstract_iter)
    for article, doc in zip(articles, nlp.pipe(abstracts, batch_size=1000, n_threads=3)):
        words = [(token.text, token.tag_) for token in doc if token.is_stop != True and token.is_punct != True]
        word_freq = tuple(Counter(words).items())
        if len(word_freq) == 0:
            yield json.dumps({**{"keyword": "", "tag": "", "count": 0}, **article})
        for keyword in word_freq:
            yield json.dumps({**{"keyword": keyword[0][0], "tag": keyword[0][1], "count": keyword[1]}, **article})

def translate_to_graphson(obj, id_str, inEdges={}, outEdges={}, label="vertex"):
    properties = {}
    for i, (k,v) in enumerate(obj.items()):
        if type(v) == list:
            properties[k] = list(map(lambda prop: {"id": i*100, "value": prop}, v))
        else:
            properties[k] = [{"id": i, "value": v}]

    return {"id": id_str,
            "outE": outEdges, "inE": inEdges, "label": label,
            "properties": properties}

def gen_art_graphson(art):
    art_id = art['art_index']
    union_edges = {"wrote": [], "present_in": []}
    for edge in art['wrote_edges']:
        pair = edge.split("-")
        new_edge = {"id": gen_id(), "outV": "author-{}".format(pair[0])}
        new_edge['properties'] = {"priority": pair[1]}
        union_edges['wrote'].append(new_edge)

    for edge in art['present_in_edges']:
        pair = edge.split("-")
        new_edge = {"id": gen_id(), "outV": "keyword-{}".format(pair[0])}
        new_edge['properties'] = {"count": pair[1]}
        union_edges['present_in'].append(new_edge)
    omitted_keys = ["art_index", "wrote_edges", "present_in_edges"]
    return translate_to_graphson(removekeys(art.asDict(), omitted_keys),
        "art-{}".format(art_id), union_edges, {}, "article")

def gen_author_graphson(author):
    author_id = author['author_index']
    wrote_edges = []
    for edge in author['wrote_edges']:
        pair = edge.split("-")
        new_edge = {"id": gen_id(), "inV": "art-{}".format(pair[0])}
        new_edge['properties'] = {"priority": pair[1]}
        wrote_edges.append(new_edge)
    omitted_keys = ["author_index", "wrote_edges"]
    return translate_to_graphson(removekeys(author.asDict(), omitted_keys),
        "author-{}".format(author_id), {}, {"wrote": wrote_edges}, "author")

def gen_keyword_graphson(keyword):
    keyword_id = keyword['index']
    present_in_edges = []
    for edge in keyword['present_in_edges']:
        pair = edge.split("-")
        new_edge = {"id": gen_id(), "inV": "art-{}".format(pair[0])}
        new_edge['properties'] = {"count": pair[1]}
        present_in_edges.append(new_edge)
    omitted_keys = ["index", "present_in_edges"]
    return translate_to_graphson(removekeys(keyword.asDict(), omitted_keys),
        "keyword-{}".format(keyword_id), {}, {"present_in": present_in_edges}, "keyword")

temp_dir = tempfile.mkdtemp(prefix='googlestorage')

path_rdd = sc.sequenceFile('gs://zeitgeist-store-1234/pubmed_seq', minSplits=10)

pubmed_oa_all = path_rdd.sample(False, 0.01, 81).map(lambda p: extract_graph(p))
# pubmed_oa_all = sc.parallelize(path_rdd.take(10)).map(lambda p: extract_graph(p))

pubmed_articles = (spark.read.json(pubmed_oa_all.map(lambda v: v['article']))
                    .withColumn("index", F.monotonically_increasing_id()))
pre_wrote_edges = spark.read.json(pubmed_oa_all.flatMap(lambda v: v['wrote_edges']))
unique_authors = pre_wrote_edges.groupby(['first_name', 'sur_name']).agg(F.collect_list('affiliation').alias('affiliations'))
wrote_edges = pre_wrote_edges.join(unique_authors.withColumn("index",
    F.monotonically_increasing_id()), ['first_name', 'sur_name'])

article_authors = (
    pubmed_articles.join(wrote_edges, ['doi', 'pmc', 'pmid', 'full_title', 'publication_year'])
    .select('doi', 'pmc', 'pmid', 'full_title', 'publication_year','first_name', 'sur_name', 'affiliations',
        wrote_edges.priority, wrote_edges.index.alias('author_index'),
        pubmed_articles.index.alias('art_index'))
)

authors_complete = (
    article_authors.groupby(['first_name', 'sur_name', 'author_index', 'affiliations']).agg(F.collect_list(
        F.concat_ws('-', 'art_index', wrote_edges.priority)).alias('wrote_edges'))
)

articles_partial = (
    article_authors.groupby(['doi', 'pmc', 'pmid', 'full_title', 'publication_year', 'art_index'])
    .agg(F.collect_list(F.concat_ws('-', 'author_index', wrote_edges.priority))
        .alias('wrote_edges'))
)

abstract_bulk = pubmed_oa_all.map(lambda v: v['abstract_edge']).collect()
extracts = []
for i in range(0, len(abstract_bulk), 10000):
    print("{}th of {} abstracts".format(i, len(abstract_bulk)))
    extracts += list(extract_keywords(abstract_bulk[i:i+10000]))

precursor_keyword_edges = spark.read.json(sc.parallelize(extracts))
unique_keywords = precursor_keyword_edges.groupby(['keyword', 'tag']).agg(F.min('count'))
keyword_edges = precursor_keyword_edges.join(unique_keywords.withColumn(
    "index", F.monotonically_increasing_id()), ['keyword', 'tag'])

articles_complete = (
    articles_partial.join(keyword_edges, ['doi', 'pmid', 'pmc', 'full_title', 'publication_year'])
    .groupby(['doi', 'pmid', 'pmc', 'full_title', 'publication_year', 'art_index', 'wrote_edges'])
    .agg(F.collect_list(F.concat_ws('-', keyword_edges.index, 'count'))
        .alias('present_in_edges'))
)

keywords_complete = (
    keyword_edges.join(pubmed_articles, ['pmc', 'doi', 'pmid'])
    .groupby(['keyword', 'tag', keyword_edges.index]).agg(F.collect_list(
        F.concat_ws('-', pubmed_articles.index, 'count')).alias('present_in_edges'))
)
# pprint(articles_complete.rdd.map(gen_art_graphson).first())
# pprint(authors_complete.rdd.map(gen_author_graphson).first())
# pprint(keywords_complete.rdd.first())
# pprint(keywords_complete.rdd.map(gen_keyword_graphson).first())
with jsonlines.open(os.path.join(temp_dir, "pubmed_articles.graphson"), "w") as writer:
    writer.write_all(articles_complete.rdd.map(gen_art_graphson).collect())

with jsonlines.open(os.path.join(temp_dir, "pubmed_authors.graphson"), "w") as writer:
    writer.write_all(authors_complete.rdd.map(gen_author_graphson).collect())

with jsonlines.open(os.path.join(temp_dir, "pubmed_keywords.graphson"), "w") as writer:
    writer.write_all(keywords_complete.rdd.map(gen_keyword_graphson).collect())

os.system("cat {}/*.graphson >> {}/pubmed.graphson".format(temp_dir, temp_dir))

with jsonlines.open("/home/nicholas/Downloads/pubmed.graphson", "r") as reader:
    vertices = {"keyword": [], "author": [], "article": []}
    vertex_ids = {"keyword": [], "author": [], "article": []}
    edges = {
        "article": {"wrote": [], "present_in": []},
        "author": {"wrote": []},
        "keyword": {"present_in": []}
    }
    for vertex in reader:
        vertices[vertex['label']].append(vertex)
        vertex_ids[vertex['label']].append(vertex['id'])
        if vertex['label'] == "article":
            for edge_type in vertex['inE'].keys():
                edges[vertex['label']][edge_type] += list(map(lambda v: v['outV'], vertex['inE'][edge_type]))
        else:
            for edge_type in vertex['outE'].keys():
                edges[vertex['label']][edge_type] += list(map(lambda v: v['inV'], vertex['outE'][edge_type]))
            # assume outE

art_ids = set(edges['keyword']['present_in']).difference(set(vertex_ids['article']))
kw_matches = []
for kw in vertices['keyword']:
    matches = list(filter(lambda v: v['inV'] in art_ids, kw['outE']['present_in']))
    if len(matches) != 0:
        kw_matches.append(kw['id'])

with jsonlines.open("pubmed.graphson", "w") as writer:
    writer.write_all(vertices['article'])
    writer.write_all(vertices['author'])
    writer.write_all(list(filter(lambda v: not v['id'] in kw_matches, vertices['keyword'])))

call(["gsutil", "-m", "cp", temp_dir + "/*son", "gs://zeitgeist-store-1234"])
