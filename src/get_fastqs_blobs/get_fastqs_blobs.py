#!/usr/bin/env python

import json
import sys
from pathlib import Path


def create_blobs_file(mapping_file: str, year: str, seq_id: str):
    blob_content = []

    with open(mapping_file) as mapping_file, open(
        "fastq_blobs.txt", "w"
    ) as blobs_file:
        jsonified_content = json.loads(mapping_file.read())
        for _, lib_data in jsonified_content["libraries"].items():
            if lib_data["rd1_path"]:
                blob_content.append(lib_data["rd1_path"])
            if lib_data["rd2_path"]:
                blob_content.append(lib_data["rd2_path"])

        for path in blob_content:
            blob = Path(path).name
            blobs_file.write(f"fq/{year}/{seq_id}/{blob}\n")

    print([Path(path).name for path in blob_content])


if __name__ == "__main__":
    file_name = sys.argv[1]
    year = sys.argv[2]
    seq_id = sys.argv[3]
    create_blobs_file(file_name, year, seq_id)
