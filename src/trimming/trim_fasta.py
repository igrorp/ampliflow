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


def trim_primer(fasta_file: str) -> Path:

    out_path = "trimmed_" + Path(fasta_file).name
    with open(out_path, "w") as out_file:
        for header, sequence in parse_fasta(fasta_file).items():
            trimmed = sequence[20:-20]
            out_file.write(f">{header}\n{trimmed}\n")


if __name__ == "__main__":
    file_name = sys.argv[1]
    trim_primer(file_name)
