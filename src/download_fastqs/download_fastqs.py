import sys
from pathlib import Path

from shared.gcs_manager import GCSManager


def download_file(file_path: str, out_path: Path) -> Path:
    gcs_manager = GCSManager(bucket="fastq_cold")
    gcs_manager.download_blob(file_path, out_path=out_path)
    print("downloaded", out_path.resolve())


if __name__ == "__main__":
    blob = sys.argv[1]
    fastq_out = Path(Path(blob).name)
    download_file(blob, fastq_out)
