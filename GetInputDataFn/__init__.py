# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

from azure.storage.blob import BlobServiceClient
from azure.storage.blob.aio import BlobClient
import os

def read_file(filename: str) -> list :
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=storagemapred;AccountKey=kiMdxWKgC5EK7GfuKUaN57Y3wvWueFZ/zfhBpdOPhV8S0+1+faxgpC9W+VgIOLagx8VrEkTfhX8x+AStdztYRg==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client(container="mapreducecontainer")
    file = container_client.download_blob(filename).readall()
    
    line_index = 0
    lines = list()
    for line in file.decode().split("\n") :
        lines.append([line_index,line.rstrip()])
        line_index += 1            
    return lines


def main(filename: str) -> str:
    return read_file(filename)
