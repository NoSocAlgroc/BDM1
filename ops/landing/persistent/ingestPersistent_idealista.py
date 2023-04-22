from ops.Controller import Controller

def ingestPersistent_idealista(ctr: Controller,filePath: str, timestamp: int):
    srcPath="landing/temporal/"+"idealista"+"/"+filePath
    dstPath="landing/persistent/"+"idealista"+"/"+filePath+"_"+str(timestamp)+".avro"
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
                    {'name': 'topNewDevelopment', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'hasParkingSpace', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'isParkingSpaceIncludedInPrice', 'type': ['boolean', 'null'], 'default': 'unknown'},
                    {'name': 'newDevelopmentFinished', 'type': ['boolean', 'null'], 'default': 'unknown'}
                    ]
    }

    data=ctr.readHDFS_JSON(srcPath)
    
    def rowGenerator():
         for row in data:
            if 'suggestedTexts' in row:
                row['typology']=row['detailedType']['typology']
                del row['detailedType']
            if 'suggestedTexts' in row:
                row['title']=row['suggestedTexts']['title']
                row['subtitle']=row['suggestedTexts']['subtitle']
                del row['suggestedTexts'] 
            if 'parkingSpace' in row:
                row['hasParkingSpace']=row['parkingSpace']['hasParkingSpace']
                row['isParkingSpaceIncludedInPrice']=row['parkingSpace']['isParkingSpaceIncludedInPrice']
                del row['parkingSpace'] 
            yield row

    ctr.writeHDFS_Avro(rowGenerator(),schema_dict,dstPath)

