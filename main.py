#Ingests a local file stored in the VM into the system
#e.g:       python3 main.py opendatabcn-income .* 
#e.g:       python3 main.py opendatabcn-price .* 
#e.g:       python3 main.py idealista .* 
import os
import re
import sys
from ops.Controller import Controller
#Import layer modules
from ops.landing.temporal.ingestTemporalLocal import ingestTemporalLocal
from ops.landing.temporal.ingestTemporalCollect import ingestTemporalCollect
from ops.landing.persistent.ingestPersistent_idealista import ingestPersistent_idealista
from ops.landing.persistent.ingestPersistent_opendatabcn_income import ingestPersistent_opendatabcn_income
from ops.landing.persistent.ingestPersistent_opendatabcn_price import ingestPersistent_opendatabcn_price

#Instantiate Controller instance
ctr=Controller(hdfsAddress="10.4.41.39",hdfsPort="9870",hdfsUser="bdm")

#Get data source and tiemstamp
source=sys.argv[1]
timestamp=ctr.getTimestamp()
temporalFiles=[]

if source != "opendatabcn-price":
    #Local ingestion
    #Get regex to match files to be ingested
    files = sys.argv[2]

    #Get all files in the local directory of the given source
    all_files = os.listdir("data/"+source)

    #Filter only the files that match the given regular expression, works with single file names
    matching_files = [f for f in all_files if re.match(files, f)]

    #Ingest each of these new files into the temporal landing
    for file in matching_files:
        createdFile=ingestTemporalLocal(ctr,source,file)
        temporalFiles.append(createdFile)
else:
    #Remote collection
    #Get year to get price data from
    year = sys.argv[2]
    
    #Get all years
    all_years = map(str,range(2013,2019))

    #Match years
    matching_year = [y for y in all_years if re.match(year, y)]

    #For each matched year, perform the ingestion
    for year in matching_year:
        createdFile=ingestTemporalCollect(ctr,year)
        temporalFiles.append(createdFile)


#Persistent landing 
#Each different source requires different transformations and avro schema
for file in temporalFiles:
    if source=="idealista":
        ingestPersistent_idealista(ctr,file,timestamp)
    elif source=="opendatabcn-income":
        ingestPersistent_opendatabcn_income(ctr,file,timestamp)
    elif source=="opendatabcn-price":
        ingestPersistent_opendatabcn_price(ctr,file,timestamp)

    