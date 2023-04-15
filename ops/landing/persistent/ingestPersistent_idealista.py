import json


def ingestPersistent_idealista(filePath):
    with open(filePath) as file:
        data = json.load(file)
        for row in data:

