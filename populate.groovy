graph = TitanFactory.open('/root/titan/conf/titan-cassandra-es.properties')
graph.io(IoCore.graphson()).readGraph('/root/pubmed.graphson')
