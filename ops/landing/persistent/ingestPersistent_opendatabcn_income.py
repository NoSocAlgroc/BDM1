from ops.Controller import Controller

def ingestPersistent_opendatabcn_income(ctr: Controller,filePath: str, timestamp: int):
    srcPath = "landing/temporal/"+"opendatabcn-income"+"/"+filePath
    dstPath="landing/persistent/"+"opendatabcn-income"+"/"+filePath+"_"+str(timestamp)+".avro"
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
                    {"name": "Índex RFD Barcelona", "type": ["float","null"],'default': 'unknown'}]
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
                    "Població": int(row[5]),
                    "Índex RFD Barcelona": float(row[6]) if row[6]!='-' else None                    
                }
            yield row
    
    ctr.writeHDFS_Avro(rowGenerator(),schema_dict,dstPath)
