#Ingests a local file in the VM into the temporal landing in HDFS
import sys
import subprocess

def ingestTemporalLocal(source,file):
    #Local path in the VM relative to project root
    srcPath="data/"+source+"/"+file
    #Destination path in Hadoop
    dstPath="/user/bdm/data/"+source+"/"+file
    #Put command
    command="~/BDM_Software/hadoop/bin/hdfs dfs -put "+srcPath+" "+dstPath
    subprocess.run(command,shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, universal_newlines=True)


if __name__ == '__main__':
    source=sys.argv[1]
    file = sys.argv[2]
    ingestTemporalLocal(source,file)
