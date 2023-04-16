import csv
import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import sys
import datetime
from hdfs import InsecureClient


def ingestPersistent_income(filePath):
    srcPath = "/user/bdm/landing/temporal/"+"opendatabcn-income"+"/"+filePath
    timestamp=int(datetime.datetime.utcnow().timestamp())
    dstPath="/user/bdm/landing/persistent/"+"opendatabcn-income"+"/"+filePath+"_"+str(timestamp)+".avro"
    # Define the Avro schema
    schema_dict={
        "type": "record",
        "name": "income",
        "fields": [ {"name": "Any", "type": "int"},
                    {"name": "Codi_Districte", "type": "int"},
                    {"name": "Nom_Districte", "type": "string"},
                    {"name": "Codi_Barri", "type": "int"},
                    {"name": "Nom_Barri", "type": "string"},
                    {"name": "Població", "type": "int"},
                    {"name": "Índex RFD Barcelona = 100", "type": "float"}]
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
                    "Codi_Districte": int(row[1]),
                    "Nom_Districte": row[2],
                    "Codi_Barri": int(row[3]),
                    "Nom_Barri": row[4],
                    "Població": int(row[5]),
                    "Índex RFD Barcelona = 100": float(row[6])
                    
                })
            writer.flush()    

if __name__ == '__main__':
    file = sys.argv[1]
    ingestPersistent_income(file)