import csv
import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import sys
import datetime
from hdfs import InsecureClient

def ingestPersistent_price(filePath):
    srcPath = "/user/bdm/landing/temporal/"+"opendatabcn-price"+"/"+filePath
    timestamp=int(datetime.datetime.utcnow().timestamp())
    dstPath="/user/bdm/landing/persistent/"+"opendatabcn-price"+"/"+filePath+"_"+str(timestamp)+".avro"

    # define the Avro schema
    schema_dict={
        "type": "record",
        "name": "price",
        "fields": [ {"name": "Any", "type": "int"},
                    {"name": "Trimestre", "type": "int"},
                    {"name": "Codi_Districte", "type": "int"},
                    {"name": "Nom_Districte", "type": "string"},
                    {"name": "Codi_Barri", "type": "int"},
                    {"name": "Nom_Barri", "type": "string"},
                    {"name": "Preu_mitja_habitatge", "type": "string"},
                    {"name": "Valor", "type": "float"}]
        }
    schema = avro.schema.Parse(json.dumps(schema_dict))

    # access to hadoop
    client = InsecureClient('http://localhost:9870', user='bdm')

    with client.read(srcPath) as csvfile:

        with client.write(dstPath, overwrite=True) as avrofile:
            writer = DataFileWriter(avrofile, DatumWriter(), schema)
            data = csv.reader(csvfile)

            for row in data:
                row=row.split(",")
                writer.append({
                    "Any": int(row[0]),
                    "Trimestre": int(row[1]),
                    "Codi_Districte": int(row[2]),
                    "Nom_Districte": row[3],
                    "Codi_Barri": int(row[4]),
                    "Nom_Barri": row[5],
                    "Preu_mitja_habitatge": row[6],
                    "Valor": float(row[7])
                    
                })
            writer.flush()

if __name__ == '__main__':
    file = sys.argv[1]
    ingestPersistent_price(file)


