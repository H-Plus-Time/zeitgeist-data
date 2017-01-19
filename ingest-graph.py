import pandas as pd
import pubmed_parser as pp
import pprint
from pyspark import SparkConf, SparkContext
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from goblin import driver
from goblin.driver.serializer import GraphSONMessageSerializer
from gremlin_python.process.traversal import T

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
path_all = pp.list_xml_path('/media/store/pubmed')
path_rdd = sc.parallelize(path_all, numSlices=100)

def extract_article(path):
    data = pp.parse_pubmed_xml(path)
    pmid = data.get('pmid', '')
    pmc = data.get('pmc', '')
    doi = data.get('doi', '')
    return (pmid, pmc, doi)

def extract_graph(path):
    data = pp.parse_pubmed_xml(path)
    pmid = data.get('pmid', '')
    pmc = data.get('pmc', '')
    doi = data.get('doi', '')
    id_tuple = (pmid, pmc, doi)
    author_list = []
    for author in data['author_list']:
        author_list.append(tuple(author[0:2] + [next(filter(lambda x: x[0] == author[-1], data['affiliation_list']), ['',''])[1]]))

    wrote_edges = []
    for i, author in enumerate(author_list):
        wrote_edges.append((id_tuple, author, i+1))
    return {"article": id_tuple, "authors": tuple(author_list), "wrote_edges": tuple(wrote_edges)}

def translate_to_graphson(obj):
    ret_obj = {"id": random.randint(), properties: {}}

def deposit_article(article):
    """
        A few options for deposition into DSE/Titan:
        1. few-slice spark procedures (n, where n is the number of DSE instances, should be safe), with transaction retrying.
            Cons:
             * May still fall foul of persistence exceptions.
             * Need to implement transaction retrying, and single-thread fallback
            Pros:
             * potentially much faster
        2. Parallel transform to GraphSON, merge in driver context, scp to DSE instance, execute bulkLoader.
            Cons:
             * Potentially non-automated
            Pros:
             * probably the fastest approach, given the amount of effort poured in by devs
        3. Write from driver context, in serial.
            Cons:
             * quite slow
            Pros:
             * simple to implement
    """

    async def inner_func(article):
        loop = asyncio.get_event_loop()
        remote_conn = await driver.Connection.open(
            "http://localhost:8182/gremlin", loop, message_serializer=GraphSONMessageSerializer)
        graph = driver.AsyncGraph()
        g = graph.traversal().withRemote(remote_conn)
        art = await g.addV(T.label, 'article').next()
        # await g.V(x['id']).property('pmid', article[0]).oneOrNone()
        # await g.V(x['id']).property('pmc', article[1]).oneOrNone()
        # await g.V(x['id']).property('doi', article[2]).oneOrNone()
        return art
    loop = asyncio.get_event_loop()
    thing = loop.run_until_complete(inner_func(article))
    return thing

def deposit_author(author):
    return author

def deposit_wrote_edges(wrote_edge):
    return wrote_edge


# pubmed_articles = path_rdd.map(lambda p: extract_article(p)).collect()
# print(pubmed_articles)
pubmed_oa_all = path_rdd.map(lambda p: extract_graph(p))
pubmed_articles = pubmed_oa_all.map(lambda p: p['article']).coalesce(1)
article_ids = pubmed_articles.map(lambda article: deposit_article(article)).collect()
pubmed_authors = pubmed_oa_all.map(lambda p: p['authors']).distinct()
author_ids = pubmed_authors.map(lambda author: deposit_author(author)).collect()
pubmed_wrote_edges = pubmed_oa_all.map(lambda p: p['wrote_edges'])
wrote_edge_ids = pubmed_wrote_edges.map(lambda w_edge: deposit_wrote_edges(w_edge)).collect()

print(article_ids[0])
print(author_ids[0])
print(wrote_edge_ids[0])
# path_rdd.map(lambda p: pp.parse_pubmed_xml(p)).saveAsPickleFile('pubmed_oa.pickle') # or to save to pickle
pprint.pprint(author_ids[0])
