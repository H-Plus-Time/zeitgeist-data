import jsonlines

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

print(vertex_ids['article'][0])
print(edges['author']['wrote'][0])
print('-----------------')
print("wrote edges")
print(len(set(vertex_ids['article']).difference(set(edges['author']['wrote']))))
print(len(set(edges['author']['wrote']).difference(set(vertex_ids['article']))))
print('-----------------')
print("present_in edges")
print(len(set(vertex_ids['article']).difference(set(edges['keyword']['present_in']))))
art_ids = set(edges['keyword']['present_in']).difference(set(vertex_ids['article']))
print(art_ids)
kw_matches = []
for kw in vertices['keyword']:
    matches = list(filter(lambda v: v['inV'] in art_ids, kw['outE']['present_in']))
    if len(matches) != 0:
        # print(matches)
        kw_matches.append(kw['id'])

print(len(kw_matches))
print(len(vertex_ids['keyword']))

with jsonlines.open("new_pubmed.graphson", "w") as writer:
    writer.write_all(vertices['article'])
    writer.write_all(vertices['author'])
    writer.write_all(list(filter(lambda v: not v['id'] in kw_matches, vertices['keyword'])))
