# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging

def main(line) -> list:
#input a <key, value> pair, where key is the line number and value is the line string
#output a list of key—value pairs

    result = list()
    L=line[1].split()
    for word in L:
        result.append((word,1))
    return result
