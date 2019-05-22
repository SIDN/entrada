import boto3
import json
import sys
import os
import operator
import datetime

from functools import reduce

def f(string:str):
    return string.format(env=os.environ, **globals())

def recFormat(obj):
    t = type(obj)
    if t == dict:
        for k,v in obj.items():
            obj[k] = recFormat(v)

    elif t == list:
        for i,v in enumerate(obj):
            obj[i] = recFormat(v)

    elif t == str:
        obj = f(obj)

    return obj

def recUpdate(baseObj, newObj):
    def inner(key, seq):
        # if the key doesn't exist previously we can just add it with its value
        if not key in reduce(operator.getitem, seq, baseObj).keys():
            # below equals: basobj[seq[0]][seq[1]]...[seq[n]][key] = newObj[seq[0]][seq[1]]...[seq[n]][key]
            reduce(operator.getitem, seq, baseObj)[key] = reduce(operator.getitem, seq + [key], newObj)
            
        else:
            # since key exists in baseobj, get values. any changes to these
            # values will propagate to baseObj and newObj respectively
            baseVal = reduce(operator.getitem, seq + [key], baseObj)
            newVal = reduce(operator.getitem, seq + [key], newObj)

            # if both values are dicts, perform inner() on every key k in newVal
            if all(t==dict for t in [type(baseVal), type(newVal)]):
                for k in newVal:
                    inner(k, seq + [key])

            # if both values are lists, append newVal to baseVal      
            elif all(t==list for t in [type(baseVal), type(newVal)]):
                # this makes it so we can't replace list values but for now that's how it's going to be
                baseVal.append(newVal)

            
            # if they are of the same type (and not dict/list) overwrite
            # baseVal with newVal
            elif type(baseVal) == type(newVal):
                baseVal == newVal

            # if the values aren't of the same type then something is wrong with
            # either the input or the config.
            else:
                raise ValueError(f"The types of {baseVal} and {newVal} do not match.")

    if type(newObj) == dict:
        for k in newObj:
            inner(k, [])

    return baseObj
    

def main(eventInput, context):
    global event, date, bucket
    # event needs to be global for certain functions to work as intended, eg f(string)
    event = eventInput

    date = str(datetime.datetime.now()).replace(" ", "_")
    
    env = os.environ
    region = env["AWS_REGION"]
    name = env["AWS_LAMBDA_FUNCTION_NAME"]
    
    bucket = event["Bucket"]
    
    
    # open config specified by event["Mode"], then format and update the configs
    # with input from event
    with open(f"configs/{event['Mode']}.json") as file:
        config = json.load(file)
    config = recFormat(config)
    config = recUpdate(config, event["EMRconfig"])
    
    # Create a cluster using the configuration
    emr = boto3.client("emr")
    return emr.run_job_flow(**config)
