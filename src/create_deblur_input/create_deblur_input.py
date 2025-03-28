import sys
from pathlib import Path


def parse_fasta(fasta_file):
    sequences = {}
    header = None
    sequence = []

    with open(fasta_file, "r") as f:
        for line in f:
            line = line.strip()
            if line.startswith(">"):
                if header:
                    sequences[header] = "".join(sequence)
                header = line[1:]
                sequence = []
            else:
                sequence.append(line)
        if header:
            sequences[header] = "".join(sequence)

    return sequences


def create_deblur_input(fasta_file: str, cutoff: str) -> Path:

    out_path = "deblur_" + Path(fasta_file).name
    file_data = parse_fasta(fasta_file)

    with open(out_path, "w") as out_file:
        for _, sequence in file_data.items():
            out_file.write(f">oligotype_{cutoff}\n{sequence}\n")


if __name__ == "__main__":
    file_name = sys.argv[1]
    cutoff = sys.argv[2]
    create_deblur_input(file_name, cutoff)
