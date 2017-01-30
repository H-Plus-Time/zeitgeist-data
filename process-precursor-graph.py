import textacy
import json
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
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from goblin import driver
from goblin.driver.serializer import GraphSONMessageSerializer
from gremlin_python.process.traversal import T

def dump_to_graph_db():
    pass
    # Deposit graphson files via goblin

    with open("gs://zeitgeist-store-1234/pubmed_wrote_edges.json", "r") as f:
        w_edges = json.load(f)
        for edge in w_edges:
            pass