import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import datetime
import sys
from hdfs import InsecureClient


def ingestPersistent_idealista(filePath):
    srcPath="/user/bdm/landing/temporal/"+"idealista"+"/"+filePath
    timestamp=int(datetime.datetime.utcnow().timestamp())
    dstPath="/user/bdm/landing/persistent/"+"idealista"+"/"+filePath+"_"+str(timestamp)+".avro"
    schema_dict = {
        "type": "record",
        "name": "idealista",
        "fields": [
                    {
                        "name": "propertyCode",
                        "type": "string"
                    },
                    {
                        "name": "thumbnail",
                        "type": "string"
                    },
                    {
                        "name": "externalReference",
                        "type": "string"
                    },
                    {
                        "name": "numPhotos",
                        "type": "int"
                    },
                    {
                        "name": "floor",
                        "type": "string"
                    },
                    {
                        "name": "price",
                        "type": "double"
                    },
                    {
                        "name": "propertyType",
                        "type": "string"
                    },
                    {
                        "name": "operation",
                        "type": "string"
                    },
                    {
                        "name": "size",
                        "type": "double"
                    },
                    {
                        "name": "exterior",
                        "type": "boolean"
                    },
                    {
                        "name": "rooms",
                        "type": "int"
                    },
                    {
                        "name": "bathrooms",
                        "type": "int"
                    },
                    {
                        "name": "address",
                        "type": "string"
                    },
                    {
                        "name": "province",
                        "type": "string"
                    },
                    {
                        "name": "municipality",
                        "type": "string"
                    },
                    {
                        "name": "district",
                        "type": "string"
                    },
                    {
                        "name": "country",
                        "type": "string"
                    },
                    {
                        "name": "neighborhood",
                        "type": "string"
                    },
                    {
                        "name": "latitude",
                        "type": "double"
                    },
                    {
                        "name": "longitude",
                        "type": "double"
                    },
                    {
                        "name": "showAddress",
                        "type": "boolean"
                    },
                    {
                        "name": "url",
                        "type": "string"
                    },
                    {
                        "name": "distance",
                        "type": "string"
                    },
                    {
                        "name": "hasVideo",
                        "type": "boolean"
                    },
                    {
                        "name": "status",
                        "type": "string"
                    },
                    {
                        "name": "newDevelopment",
                        "type": "boolean"
                    },
                    {
                        "name": "hasLift",
                        "type": "boolean"
                    },
                    {
                        "name": "priceByArea",
                        "type": "double"
                    },
                    {
                        "name": "typology",
                        "type": "string"
                    },
                    {
                        "name": "subtitle",
                        "type": "string"
                    },
                    {
                        "name": "title",
                        "type": "string"
                    },
                    {
                        "name": "hasPlan",
                        "type": "boolean"
                    },
                    {
                        "name": "has3DTour",
                        "type": "boolean"
                    },
                    {
                        "name": "has360",
                        "type": "boolean"
                    },
                    {
                        "name": "hasStaging",
                        "type": "boolean"
                    },
                    {
                        "name": "topNewDevelopment",
                        "type": "boolean"
                    }
        ]
    }
    client = InsecureClient('http://localhost:9870', user='bdm')
    schema = avro.schema.Parse(json.dumps(schema_dict))

    with client.read(srcPath) as json_file:
    # Open the output Avro file
        with client.write(dstPath, overwrite=True) as avro_file:
                writer = DataFileWriter(avro_file, DatumWriter(), schema)
                data=json.load(json_file)
                for row in data:
                    row['typology']=row['detailedType']['typology']
                    del row['detailedType']
                    row['title']=row['suggestedTexts']['title']
                    row['subtitle']=row['suggestedTexts']['subtitle']
                    del row['suggestedTexts']

                    writer.append(row)
                writer.close()
                    

if __name__ == '__main__':
    file = sys.argv[1]
    ingestPersistent_idealista(file)