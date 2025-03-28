import logging
from pathlib import Path
from typing import Union

from google.cloud import storage

from .file_manager import FileManager

DMD_PATH = Path.home().joinpath("storage/results/dmd/")
DMD_PATH.mkdir(parents=True, exist_ok=True)


class GCSManager:
    def __init__(self, bucket: str) -> None:
        self.logger = logging.getLogger("neoflow")
        self.client = storage.Client.from_service_account_json(
            json_credentials_path="/home/neoprospecta/.credentials/biodrive-neopct-c528dcdd212e.json"
        )
        self.bucket = self.client.bucket(bucket)

    def download_blob(self, blob: str, out_path: Path) -> Path:
        """Downloads a given blob from the bucket.

        Args:
            blob (str): The blob path
            out_path (Union[str, Path]): The path to where the downloaded data
            should be stored.

        Raises:
            Exception: If the provided blob does not exist on the bucket.
        """
        if out_path.exists():
            self.logger.info(
                f"Blob {blob} is not going to be downloaded because "
                f"'{out_path}' already exists"
            )
            return out_path
        new_fastq_blob = self.bucket.blob(blob)
        if not new_fastq_blob.exists():
            raise Exception(
                f"Provided blob {blob} does not exist on bucket {self.bucket}"
            )
        new_fastq_blob.download_to_filename(filename=str(out_path))
        self.logger.debug(f"Downloaded blob {blob} to {out_path}")

        return out_path

    def download_dmd_result(
        self, id: str, compression_suffix: str = ".tar.bz2"
    ) -> Path:
        """Downloads data from a DMD result, given its Biodrive ID. Compressed
        data is downloaded from Google Cloud Storage and decompressed to the
        standard location ~/storage/results/dmd/{ID}.

        Args:
            id (str): The Biodrive ID from the result

        Returns:
            Path: The Path to where the data was downloaded to.
        """
        if not DMD_PATH.exists():
            DMD_PATH.mkdir(parents=True)
        out_path = DMD_PATH.joinpath(id)
        if out_path.exists() and not out_path.is_mount():
            self.logger.debug(
                f"Result ID {id} already exists at '{out_path}'. If you want to"
                " replace the data, please delete the directory and run again."
            )
            return out_path
        if len(id) == 15:
            year = id[:4]
        elif len(id) == 13:
            year = "20" + id[:2]
        else:
            raise Exception(
                f"The provided ID {id} escapes the expected year pattern"
            )
        blob = f"results/{year}/{id}{compression_suffix}"
        comp_out_path = DMD_PATH.joinpath(f"{id}{compression_suffix}")
        self.download_blob(blob=blob, out_path=comp_out_path)
        file_manager = FileManager(path=comp_out_path)
        file_manager.decompress_tar_bz2(out_path=DMD_PATH)

        return out_path

    def upload_to_blob(
        self, filename: Union[str, Path], dest_blob: str, public: bool = False
    ) -> str:

        if not Path(filename).exists():
            raise Exception(f"Could not find {filename}")

        chunk_size = 10485760  # 1024 * 1024 B * 10 = 10 MB
        blob = self.bucket.blob(dest_blob, chunk_size=chunk_size)
        blob.upload_from_filename(str(filename), timeout=120)
        if public:
            self.logger.debug("Making the uploaded file public")
            blob.make_public()
            self.logger.info(f"Public URL: {blob.public_url}")

        return blob.name
