# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging


def main(input) -> dict:
    """
     input: map outputs (i.e a list of k-v pairs) 
     output: (<key, [list of values]>) which will be the input of reduce function
    """
    result = dict()
    for word in input :
        if not result.get(word[0]) :
            result[word[0]] = list()
        result[word[0]].append(word[1])
    return result
