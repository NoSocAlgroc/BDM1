
*Albert Martín Garcia*

*Maria Paraskeva*

## BDM P1: Landing zone

This directory contains our deliverable for the first part of the course project.

### Instructions

Required python packages are found in *requirements.txt*, and may be isntalled with:

    pip install -r requirements.txt

In order to run our program, *main.py* must be run through python, specifying the source as the first argument and the particular information about the files to be uploaded as the second argument.

For example, to upload a local file in the *idealista* data folder, the following code must be ran in the root directory:

    python3 main.py idealista 2020_01_02_idealista.json

The same for opendatabcn-income files:

    python3 main.py opendatabcn-income 2007_Distribucio_territorial_renda_familiar.csv

And since opendatabcn-price is a remote directory, only the year needs to be specified:

    python3 main.py opendatabcn-price 2013

Our program accepts regular expressions to make the ingestion of multiple files much easier.

In order to ingest all files, the following three commands can be run:

    python3 main.py opendatabcn-income .* 
    python3 main.py opendatabcn-price .* 
    python3 main.py idealista .* 

Any other regular expression is accepted, so ingesting only files of a certain year is also possible.
