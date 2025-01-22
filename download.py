import argparse
import os
import json
import urllib.request

def download_chunks(json_file_path, output_dir):
    # Read the JSON file
    with open(json_file_path, "r") as f:
        data = json.load(f)
    
    # Ensure the "chunks" key exists in the JSON
    if "chunks" not in data:
        raise KeyError("'chunks' attribute is missing from the JSON file")
    
    # Get the list of URLs
    chunk_urls = data["chunks"]

    for index, url in enumerate(chunk_urls):
        
            print(f"Downloading chunk {index+1} from {url}...")
             
            # Build the output file path
            output_xml_path = os.path.join(output_dir, f"chunk_{index+1}.xml")

            with open(output_xml_path, "w") as output_file:

                # Open URL and read contents
                with urllib.request.urlopen(url) as response:
                    data = response.read().decode("utf-8").splitlines()
                    output_file.write("\n".join(data) + "\n")
            
    print(f"File downloaded and saved to {output_xml_path}")

import urllib.request
import os

def download_json_file(url, save_dir=".", filename="downloaded.json"):
    """
    Downloads a JSON file from the given URL and saves it locally.
    
    Args:
        url (str): The URL of the JSON file.
        save_dir (str): The directory to save the file. Default is the current directory.
        filename (str): The name to save the file as. Default is "downloaded.json".
    
    Returns:
        str: The path where the file is saved.
    """
    # Ensure the save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Build the full file path
    file_path = os.path.join(save_dir, filename)

    # Download and save the file
    try:
        print(f"Downloading JSON file from {url}...")
        with urllib.request.urlopen(url) as response:
            content = response.read().decode("utf-8")
            with open(file_path, "w") as f:
                f.write(content)
        print(f"File saved to {file_path}")
    except Exception as e:
        print(f"Error downloading the file: {e}")
        return None

    return file_path


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Dataset file download")
    parser.add_argument("--manifest_url", required=True, help="manifest url")
    parser.add_argument("--output_dir", required=True, help="output directory")

    args = parser.parse_args()
    manifest_url = args.manifest_url
    output_dir = args.output_dir

    file_path = download_json_file(manifest_url, output_dir)
    download_chunks(file_path, output_dir)
