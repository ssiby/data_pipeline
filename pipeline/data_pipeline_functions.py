import json
import csv
import re
#from collections import OrderedDict
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import os
import pandas as pd
from collections import Counter


working_path="./tmp"
output_path="./output"
#os.mkdir(working_path)
#os.mkdir(output_path)
drug_file_name="drugs.csv" 
pubmed_file_name="pubmed.csv"
clinical_trials_file_name="clinical_trials.csv"
drug_tree_filename="drugs_relation_tree.json"
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
source_blob_container = os.getenv('SOURCE_BLOB_CONTAINER')
target_blob_container = os.getenv('TARGET_BLOB_CONTAINER')


def get_drug_relation_tree():
    #init working environment
    if not os.path.exists(working_path):
        os.mkdir(working_path)
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    # download files from blolb container
    download_file_from_container(source_blob_container, drug_file_name)
    download_file_from_container(source_blob_container, pubmed_file_name)
    download_file_from_container(source_blob_container, clinical_trials_file_name)
    # Treat files execute business rules
    drugRelationTree = build_drugs_tree();
    # Upload drug tree json file to target blob container
    drugTreeFilePath = os.path.join(output_path, drug_tree_filename)
    upload_file_to_container(target_blob_container, drugTreeFilePath)
    # Clean up working environment
    clean_up(source_blob_container)
    # return drug tree values for API
    return drugRelationTree


def get_best_journal():
    # download files from blolb container
    download_file_from_container(source_blob_container, pubmed_file_name)
    download_file_from_container(source_blob_container, clinical_trials_file_name)
    # getting best journal
    medData = pd.read_csv(os.path.join(working_path, pubmed_file_name))
    cliData = pd.read_csv(os.path.join(working_path, clinical_trials_file_name))
    publication_list = medData['journal'].to_list() + cliData['journal'].to_list()
    clean_up(source_blob_container)
    counter = Counter(publication_list)
    return max(publication_list, key=counter.get)
    
    
def build_drugs_tree():
    jsonArray = []
    drugFilePath = os.path.join(working_path, drug_file_name)
    pubMedFilePath = os.path.join(working_path, pubmed_file_name)
    clinicalFilePath = os.path.join(working_path, clinical_trials_file_name)
    drugTreeFilePath = os.path.join(output_path, drug_tree_filename)
    #read csv drugs file
    with open(drugFilePath, encoding='utf-8') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 
        for row in csvReader: 
            #building drug relation
            row["pubmeds"] = get_drug_publication(row["drug"], "title", pubMedFilePath)
            row["clinical_trials"] = get_drug_publication(row["drug"], "scientific_title", pubMedFilePath)
            row["journals"] = get_drug_publication_journal_list(row["drug"], pubMedFilePath, clinicalFilePath)
            jsonArray.append(row)
    #convert python jsonArray to JSON String and write to file
    with open(drugTreeFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)
    return jsonString


def get_drug_publication(drug, searchColumn, csvPub):
    pubRowData = []
    #read csv file
    with open(csvPub, encoding='utf-8') as csvf:
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf)
        #getting pub row which title contains drug name
        for row in csvReader:
            if row[searchColumn] != None and re.search(drug, row[searchColumn], re.IGNORECASE):
                pubRowData.append(row)
    print(pubRowData)
    return pubRowData


def get_drug_publication_journal(drug, searchColumn, csvPub):
    pubJournalRowData = []
    #read csv file
    with open(csvPub, encoding='utf-8') as csvf:
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf)
        #getting pub row which title contains drug name
        for row in csvReader:
            if row[searchColumn] != None and re.search(drug, row[searchColumn], re.IGNORECASE):
                if row["journal"] != None and row["journal"] != "":
                    pubJournalRowData.append(row["journal"])
    #print(pubJournalRowData)
    return pubJournalRowData


def get_drug_publication_journal_list(drug, csvPub, csvCli):
    #read csv file
    pubJournalRowData = get_drug_publication_journal(drug, "title", csvPub) 
    cliJournal = get_drug_publication_journal(drug, "scientific_title", csvCli)
    if cliJournal != None and cliJournal != []:
        pubJournalRowData.append(cliJournal)
    pubJournalRowData = list(dict.fromkeys(pubJournalRowData))
    print(pubJournalRowData)
    return pubJournalRowData


def download_file_from_container(container_name, file_name):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    download_file_path = os.path.join(working_path, file_name)
    print("\nDownloading blob to \n\t" + download_file_path)
    with open(download_file_path, "wb") as download_file:
      download_file.write(blob_client.download_blob().readall())


def upload_file_to_container(container_name, file_name):
    upload_file_path = os.path.join(output_path, file_name)
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    print("\nUploading to Azure Storage as blob:\n\t" + file_name)
    # Upload the created file
    with open(upload_file_path, "rb") as data:
      blob_client.upload_blob(data)

def clean_up(container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    # Clean up
    #print("Deleting files within container...")
    #container_client.delete_blob(container_name, drug_file_name)
    #container_client.delete_blob(container_name, pubmed_file_name)
    #container_client.delete_blob(container_name, clinical_trials_file_name)
    print("Deleting the local source and downloaded files...")
    os.rmdir(working_path)
    os.rmdir(output_path)
    print("Done")

