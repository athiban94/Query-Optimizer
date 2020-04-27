import json

with open('query.json') as f:
    data = json.load(f)

gV_suchThat = {}
for aggr in data['st']:
    for groupVar in range(1, data['n']+1):
        if str(groupVar) in aggr:
            if groupVar in gV_suchThat:
                gV_suchThat[groupVar].append(aggr)
            else:
                gV_suchThat[groupVar] = [aggr]


for key, value in gV_suchThat.items():
    print(key , value)