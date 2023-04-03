#Ingests a local file stored in the VM into the system
#e.g:       python3 ops/main.py opendatabcn-income .* 
import os
import re
import sys

from landing.temporal.ingestTemporalLocal import ingestTemporalLocal
from landing.temporal.ingestTemporalCollect import ingestTemporalCollect


#Get data source and file pattern for files of that source to be ingested
source=sys.argv[1]
if source != "opendatabcn-price":

    files = sys.argv[2]

    #Get all files in the local directory of the given source
    all_files = os.listdir("data/"+source)

    #Filter only the files that match the given regular expression, works with single file names
    matching_files = [f for f in all_files if re.match(files, f)]

    #Ingest each of these new files into the temporal landing
    for file in matching_files:
        ingestTemporalLocal(source,file)
else:
    year = sys.argv[2]
    
    all_years = map(str,range(2013,2019))
    matching_year = [y for y in all_years if re.match(year, y)]

    for year in matching_year:
        ingestTemporalCollect(year)

