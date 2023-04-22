from ops.Controller import Controller

def ingestPersistent_opendatabcn_price(ctr: Controller,filePath: str, timestamp: int):
    srcPath = "landing/temporal/"+"opendatabcn-price"+"/"+filePath
    dstPath="landing/persistent/"+"opendatabcn-price"+"/"+filePath+"_"+str(timestamp)+".avro"

    # define the Avro schema
    schema_dict={
        "type": "record",
        "name": "price",
        "fields": [ {"name": "Any", "type": "int"},
                    {"name": "Codi_Districte", "type": "int"},
                    {"name": "Nom_Districte", "type": "string"},
                    {"name": "Codi_Barri", "type": "int"},
                    {"name": "Nom_Barri", "type": "string"},
                    {"name": "Preu_mitja_habitatge", "type": "string"},
                    {"name": "Valor", "type": ["float","null"],"default":"unknown"}]
        }
    rows=ctr.readHDFS_CSV(srcPath)
    def rowGenerator():
        for row in rows:
            row={
                    "Any": int(row[0]),
                    "Codi_Districte": int(row[1]),
                    "Nom_Districte": row[2],
                    "Codi_Barri": int(row[3]),
                    "Nom_Barri": row[4],
                    "Preu_mitja_habitatge": row[5],
                    "Valor": float(row[6]) if row[6]!="NA" else None              
                }
            yield row
    
    ctr.writeHDFS_Avro(rowGenerator(),schema_dict,dstPath)


