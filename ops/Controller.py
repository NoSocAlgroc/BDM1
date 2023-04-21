from hdfs import InsecureClient
import requests
import json
class Controller:
    def __init__(self,hdfsAddress="localhost",hdfsPort="9870",hdfsUser="bdm",hdfsDataDir="/user/bdm") -> None:
        self.client=InsecureClient('http://'+hdfsAddress+':'+hdfsPort, user=hdfsUser)
        self.hdfsDataDir=hdfsDataDir

    def uploadLocal(self,srcPath:str, dstPath: str) -> None:
        HDFSdstPath=self.hdfsDataDir+"/"+dstPath
        self.client.upload(HDFSdstPath,srcPath,overwrite=True)

    def uploadRemote(self,srcUrl:str, dstPath: str) -> None:
        HDFSdstPath=self.hdfsDataDir+"/"+dstPath
        with requests.get(srcUrl, stream=True) as res:
            with self.client.write(HDFSdstPath,overwrite=True) as writer:
                for chunk in res.iter_content(chunk_size=1<<20):
                    writer.write(chunk)

    def downloadJSON(self,srcUrl:str):
        with requests.get(srcUrl) as res:
            jsonFile=json.loads(res.text)
        return jsonFile
    
