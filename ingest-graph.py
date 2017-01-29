import pandas as pd
import pubmed_parser as pp
import pprint
from pyspark import SparkConf, SparkContext
import asyncio
from itertools import chain
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from goblin import driver
from goblin.driver.serializer import GraphSONMessageSerializer
from gremlin_python.process.traversal import T

conf = (SparkConf()
         .setMaster("local")
         .setAppName("Zeitgeist")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
path_all = pp.list_xml_path('/home/nicholas/host')
path_rdd = sc.parallelize(path_all, numSlices=1000)

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
        wrote_edges.append((author, id_tuple, i+1))
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
        art = await g.addV(T.label, 'article', 'pmid', article[0],'pmc', article[1],'doi', article[2]).next()
        return (article, art['id'])
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(inner_func(article))

def deposit_author(author):
    async def inner_func(author):
        loop = asyncio.get_event_loop()
        remote_conn = await driver.Connection.open(
            "http://localhost:8182/gremlin", loop, message_serializer=GraphSONMessageSerializer)
        graph = driver.AsyncGraph()
        g = graph.traversal().withRemote(remote_conn)
        auth = await g.addV(T.label, 'article', 'last_name', author[0], 'first_name', author[1], 'affiliation', author[2]).next()
        return (tuple(author), auth['id'])
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(inner_func(author))

def deposit_wrote_edges(wrote_edge):
    async def inner_func(wrote_edge):
        loop = asyncio.get_event_loop()
        remote_conn = await driver.Connection.open(
            "http://localhost:8182/gremlin", loop, message_serializer=GraphSONMessageSerializer)
        graph = driver.AsyncGraph()
        g = graph.traversal().withRemote(remote_conn)
        wrote_edge = await g.V(wrote_edge[1][0]).addE('wrote').to(g.V(wrote_edge[1][1])).next()
        return wrote_edge
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(inner_func(wrote_edge))


# pubmed_articles = path_rdd.map(lambda p: extract_article(p)).collect()
# print(pubmed_articles)
pubmed_oa_all = path_rdd.map(lambda p: extract_graph(p))
pubmed_articles = pubmed_oa_all.map(lambda p: p['article']).collect()
pubmed_authors = pubmed_oa_all.flatMap(lambda p: p['authors']).distinct().collect()
pubmed_wrote_edges = pubmed_oa_all.map(lambda p: p['wrote_edges'])
authors_mapped = sc.parallelize(list(map(lambda author: deposit_author(author), pubmed_authors)))
article_ids = sc.parallelize(list(map(lambda article: deposit_article(article), pubmed_articles)))
auth_we_edge = pubmed_wrote_edges.flatMap(lambda w_e: w_e).join(authors_mapped)
auth_art_we_edge = auth_we_edge.flatMap(lambda w_e: w_e[1:]).join(article_ids).collect()
wrote_edge_ids = list(map(lambda w_edge: deposit_wrote_edges(w_edge), auth_art_we_edge))

# print(article_ids[0])
print(wrote_edge_ids)
# path_rdd.map(lambda p: pp.parse_pubmed_xml(p)).saveAsPickleFile('pubmed_oa.pickle') # or to save to pickle
# pprint.pprint(auth_art_we_edge[0])
# pprint.pprint(article_ids.collect()[0])
