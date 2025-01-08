import argparse
import os
import json
import urllib.request

def download_chunks_and_merge(json_file_path, output_csv_path="data.csv"):
    # Read the JSON file
    with open(json_file_path, "r") as f:
        data = json.load(f)
    
    # Ensure the "chunks" key exists in the JSON
    if "chunks" not in data:
        raise KeyError("'chunks' attribute is missing from the JSON file")
    
    # Get the list of URLs
    chunk_urls = data["chunks"]

    # Open the output CSV for writing
    with open(output_csv_path, "w") as output_file:
        for index, url in enumerate(chunk_urls):
            print(f"Downloading chunk {index+1} from {url}...")

            # Open URL and read contents
            with urllib.request.urlopen(url) as response:
                csv_data = response.read().decode("utf-8").splitlines()

            # Write the first file fully (including header), skip header for the rest
            if index == 0:
                output_file.write("\n".join(csv_data) + "\n")
            else:
                # Skip the header line (first line)
                output_file.write("\n".join(csv_data[1:]) + "\n")

    print(f"Data successfully merged into {output_csv_path}")

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

    parser = argparse.ArgumentParser(description="CSV file merge")
    parser.add_argument("--manifest_url", required=True, help="manifest url")

    args = parser.parse_args()
    manifest_url = args.manifest_url

    file_path = download_json_file(manifest_url)
    download_chunks_and_merge(file_path, output_csv_path="data.csv")
