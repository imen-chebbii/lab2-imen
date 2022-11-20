# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
#input a key-value pair, where key is a word and value is a list
#output a key—value pair where key is word and value is total count

def main(input: dict) -> dict:
    result = dict()
    for word in input.keys():
        result[word] = len(input[word])
    return result
