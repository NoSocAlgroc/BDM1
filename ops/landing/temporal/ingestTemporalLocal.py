#Ingests a local file in the VM into the temporal landing in HDFS
from ops.Controller import Controller

def ingestTemporalLocal(ctr: Controller, source: str, file: str):
    #Local path relative to project root
    srcPath="data/"+source+"/"+file
    #Destination path in Hadoop
    dstPath="landing/temporal/"+source+"/"+file
    #Clone file
    ctr.uploadLocal(srcPath,dstPath)

