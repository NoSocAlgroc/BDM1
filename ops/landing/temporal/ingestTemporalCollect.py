from ops.Controller import Controller

def ingestTemporalCollect(ctr:Controller, year):
    url = "https://opendata-ajuntament.barcelona.cat/data/api/3/action/package_search?q=name:est-mercat-immobiliari-compravenda-preu-total"
    data=ctr.downloadJSON(url)
    fileName=str(year)+"_comp_vend_preu.csv"
    url=[resource for resource in data['result']['results'][0]['resources'] if resource['name'].lower()==fileName][0]['url']

    dstPath="landing/temporal/opendatabcn-price/"+fileName
    ctr.uploadRemote(url,dstPath)

