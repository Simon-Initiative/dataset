
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
    """Remove fields at the specified indices from ``record``.

    ``excluded_indices`` may be in any order. Deleting items from a list by
    index causes later indices to shift, so we delete from the highest index to
    the lowest to ensure the correct elements are removed.
    """

    for index in sorted(excluded_indices, reverse=True):
        del record[index]
    return record

def serial_map(bucket_name, keys, map_func, context, columns):
    bucket_keys = [(bucket_name, key) for key in keys]
    results = []

    for key in bucket_keys:
        results.extend(map_func(key, context, columns))
    
    return results

def guarentee_int(value):
    if isinstance(value, str):
        return int(value)
    else:
        return value