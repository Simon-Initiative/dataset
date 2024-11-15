
import json

def encode_array(v):
    """
    Encodes a Python list of integers as a string for CSV output, with double quotes around it.
    For example, [1, 2, 3] becomes '"1,2,3"'
    """
    return '"' + ','.join(map(str, v)) + '"'

def encode_json(json_obj):
    """
    Encodes a JSON object as a single-line JSON string for CSV output, with double quotes around it.
    Removes any newline characters.
    """
    json_str = json.dumps(json_obj, separators=(',', ':'))  # Compact JSON format
    return '"' + json_str.replace('\n', '') + '"'

def parallel_map(sc, bucket_name, keys, map_func, context, columns):
    
    bucket_keys = [(bucket_name, key) for key in keys]
    
    pkeys = sc.parallelize(bucket_keys)
    activation = pkeys.flatMap(lambda key: map_func(key, context, columns))
    
    # Collect the results to the driver and print them
    results = activation.collect()
    
    return results

def prune_fields(record, excluded_indices):
    for index in excluded_indices:
        del record[index]
    return record

