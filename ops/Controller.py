from hdfs import InsecureClient
import requests
import json
import csv
from typing import Union, Generator, List
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import datetime

#Controller class that interfaces with the outside world
class Controller:

    #Initializes the controller with a HDFS connection
    def __init__(self,hdfsAddress="localhost",hdfsPort="9870",hdfsUser="bdm",hdfsDataDir="/user/bdm") -> None:
        self.client=InsecureClient('http://'+hdfsAddress+':'+hdfsPort, user=hdfsUser)
        self.hdfsDataDir=hdfsDataDir

    #Copies a local file into HDFS
    def writeLocal(self,srcPath:str, dstPath: str) -> None:
        HDFSdstPath=self.hdfsDataDir+"/"+dstPath
        self.client.upload(HDFSdstPath,srcPath,overwrite=True)

    #Copies a remote file into HDFS
    def writeRemote(self,srcUrl:str, dstPath: str) -> None:
        with requests.get(srcUrl, stream=True) as res:
            with self.writeHDFS(dstPath,overwrite=True) as writer:
                for chunk in res.iter_content(chunk_size=1<<20):
                    writer.write(chunk)

    #Reads a file from HDFS and returns the context manager
    def readHDFS(self, srcPath: str, **args):
        HDFSsrcPath=self.hdfsDataDir+"/"+srcPath
        return self.client.read(HDFSsrcPath,**args)
    #Reads a JSON from HDFS and returns the parsed object
    def readHDFS_JSON(self,srcPath: str,**args):
        with self.readHDFS(srcPath,**args) as file:
            data=json.load(file)
        return data
    #Reads a CSV from HDFS and returns a generator for the rows in order to stream it
    def readHDFS_CSV(self, srcPath: str, skipHeader=True, **args):        
        def rowGenerator():
            with self.readHDFS(srcPath,encoding='utf-8',**args) as file:
                csv_reader=csv.reader(file)
                if skipHeader:
                    header=next(csv_reader)
                for row in csv_reader:
                    yield row
        return rowGenerator()
    
    #Opens a HDFS file for write and returns its context manager
    def writeHDFS(self, dstPath: str,**args):
        HDFSdstPath=self.hdfsDataDir+"/"+dstPath
        return self.client.write(HDFSdstPath, **args)
    #Writes a set of rows into an avro file in HDFS. Accepts a generator to stream the data.
    def writeHDFS_Avro(self,rows: Union[List[dict],Generator[dict,None,None]], schemaDict: dict, dstPath: str):
        schema = avro.schema.Parse(json.dumps(schemaDict))
        with self.writeHDFS(dstPath, overwrite=True) as avro_file:
            writer = DataFileWriter(avro_file, DatumWriter(), schema, codec='snappy')
            for row in rows:
                writer.append(row)
            writer.flush()

    #Downloads and parses a JSON from the web and returns the parsed object
    def downloadJSON(self,srcUrl:str):
        with requests.get(srcUrl) as res:
            jsonFile=json.loads(res.text)
        return jsonFile
    
    #Gets current timestamp
    def getTimestamp(self):
        return int(datetime.datetime.utcnow().timestamp())

    
