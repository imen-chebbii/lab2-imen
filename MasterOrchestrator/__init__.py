# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

import azure.functions as func
import azure.durable_functions as df


inputFiles = ["mrinput-1.txt",  "mrinput-2.txt",  "mrinput-3.txt",  "mrinput-4.txt"]

REDUCERS_NUMBER = 10


def transf_one_list(arr: list) -> list :
    """
    Tranform array of type [[],[],[]...] into [...]
    """
    result = list()
    for x in arr :
        result += x
    return result

def merge(d: dict) -> dict :
    result = dict()
    for x in d :
        result = result | x
    return result



def orchestrator_function(context: df.DurableOrchestrationContext):

    # first we read the input files
 
    tasks = []
    for filename in inputFiles:
        tasks.append(context.call_activity('GetInputDataFn', filename))
    tmp = yield context.task_all(tasks)

    lines = transf_one_list(tmp)
    
 
    # Then we start the Mapper phase

    tasks = []
    for line in lines:
        tasks.append(context.call_activity('Mapper', line))

    tmp = yield context.task_all(tasks)
    
    # Concatenate the output of each mappers
    mappers_output = transf_one_list(tmp)

    # Start the shuffle phase

    shuffler_output = yield context.call_activity('Shuffler', mappers_output)

    # Start Reducer phase

    # number of key-value pairs for each reducer
    n = (len(shuffler_output) + REDUCERS_NUMBER - 1 ) // REDUCERS_NUMBER # rounding up the division!
    tasks = []
    
    # tranform the shuffler_output from dict to list in order to split it 
    shuffler_output = list(shuffler_output.items())
    for i in range(REDUCERS_NUMBER):
        # splitting the dictionary
        d_i = dict(shuffler_output[i*n:(i+1)*n])
        tasks.append(context.call_activity('Reducer', d_i))
    tmp = yield context.task_all(tasks)
    
    # Merge the output of each reducer
    reducers_output = merge(tmp)

    ##############################################################
    
    return reducers_output

main = df.Orchestrator.create(orchestrator_function)