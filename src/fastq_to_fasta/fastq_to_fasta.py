import sys
from pathlib import Path


def fastq_to_fasta(fastq_file, fasta_file):
    with open(fastq_file, "r") as fq, open(fasta_file, "w") as fa:
        while True:
            header = fq.readline().strip()
            sequence = fq.readline().strip()
            _ = fq.readline()
            _ = fq.readline()

            if not header:
                break
            fa.write(f">{header[1:]}\n{sequence}\n")


if __name__ == "__main__":
    fastq_file = sys.argv[1]
    fasta_file = Path(fastq_file).with_suffix(".fasta")
    fastq_to_fasta(fastq_file, fasta_file)
