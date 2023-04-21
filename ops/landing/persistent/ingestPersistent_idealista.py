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
        "fields": [ {'name': 'propertyCode', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'thumbnail', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'externalReference',
                    'type': ['string', 'null'],
                    'default': 'unknown'},
                    {'name': 'numPhotos', 'type': ['int', 'null'], 'default': 'unknown'},
                    {'name': 'floor', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'price', 'type': ['double', 'null'], 'default': 'unknown'},
                    {'name': 'propertyType', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'operation', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'size', 'type': ['double', 'null'], 'default': 'unknown'},
                    {'name': 'exterior', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'rooms', 'type': ['int', 'null'], 'default': 'unknown'},
                    {'name': 'bathrooms', 'type': ['int', 'null'], 'default': 'unknown'},
                    {'name': 'address', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'province', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'municipality', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'district', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'country', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'neighborhood', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'latitude', 'type': ['double', 'null'], 'default': 'unknown'},
                    {'name': 'longitude', 'type': ['double', 'null'], 'default': 'unknown'},
                    {'name': 'showAddress', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'url', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'distance', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'hasVideo', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'status', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'newDevelopment', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'hasLift', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'priceByArea', 'type': ['double', 'null'], 'default': 'unknown'},
                    {'name': 'typology', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'subtitle', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'title', 'type': ['string', 'null'], 'default': 'unknown'},
                    {'name': 'hasPlan', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'has3DTour', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'has360', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'hasStaging', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'topNewDevelopment',
                    'type': ['boolean', 'null'],
                    'default': 'unknown'}]
    }
    # access to hadoop
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
                writer.flush()

                    

if __name__ == '__main__':
    file = sys.argv[1]
    ingestPersistent_idealista(file)