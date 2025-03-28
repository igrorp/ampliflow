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


def standarize_sequences(fasta_file: str) -> Path:

    out_path = "std_" + Path(fasta_file).name
    file_data = parse_fasta(fasta_file)
    min_seq_len = min([len(seq) for seq in file_data.values()])

    with open(out_path, "w") as out_file:
        for header, sequence in file_data.items():
            standarized = sequence[:min_seq_len]
            out_file.write(f">{header}\n{standarized}\n")


if __name__ == "__main__":
    file_name = sys.argv[1]
    standarize_sequences(file_name)
