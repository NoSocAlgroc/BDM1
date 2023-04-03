import requests
import json
import csv
import sys

from ingestTemporalLocal import ingestTemporalLocal
def ingestTemporalCollect(year):
    url = "https://opendata-ajuntament.barcelona.cat/data/api/3/action/package_search?q=name:est-mercat-immobiliari-compravenda-preu-total"
    response = requests.get(url)
    data = json.loads(response.text)
    fileName=str(year)+"_comp_vend_preu_trim.csv"
    resId=[resource for resource in data['result']['results'][0]['resources'] if resource['name'].lower()==fileName][0]['id']

    url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id="+resId
    response = requests.get(url)
    data = json.loads(response.text)
    records=data['result']['records']

    headers = list(records[0].keys())
    with open('data/opendatabcn-price/'+fileName, 'w', newline='') as f:
        writer = csv.writer(f)

        # Write the headers to the first row of the CSV file
        writer.writerow(headers)

        # Write each dictionary in the list to a new row in the CSV file
        for row in records:
            writer.writerow(list(row.values()))

    ingestTemporalLocal("opendatabcn-price",fileName)


if __name__ == '__main__':
    year=sys.argv[1]
    ingestTemporalCollect(year)
