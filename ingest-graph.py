import pandas as pd
import pubmed_parser as pp
path_all = pp.list_xml_path('gs://zeitgeist-data/pubmed/')
path_rdd = sc.parallelize(path_all, numSlices=10000)
pubmed_oa_all = path_rdd.map(lambda p: pp.parse_pubmed_xml(p)).collect() # load to memory
# path_rdd.map(lambda p: pp.parse_pubmed_xml(p)).saveAsPickleFile('pubmed_oa.pickle') # or to save to pickle
pubmed_oa_df = pd.DataFrame(pubmed_oa_all) # transform to pandas DataFrame