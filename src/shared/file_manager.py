import glob
import json
import re
import tarfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd


class FileManager:

    def __init__(self, path: Union[str, Path]) -> None:
        if not path:
            raise Exception("Please provided a valid path")

        self.file_path = Path(path).resolve()
        if not self.file_path.exists():
            raise Exception(f"Could not find file '{self.file_path}'")

    @staticmethod
    def get_files_paths(
        workdir: Union[str, Path],
        basenames: List[str],
        prefix: str = "*",
        suffix: str = "*",
        extension: str = ".tsv",
    ) -> List[Path]:
        formatted_paths = []
        work_path = Path(workdir).resolve()

        if not work_path.is_dir():
            raise Exception(
                f"The provided path {workdir} is not a valid directory."
            )

        for file in basenames:
            filepath = work_path.joinpath(f"{prefix}{file}{suffix}{extension}")
            filematch = glob.glob(str(filepath))
            if len(filematch) > 1:
                raise Exception(
                    f"Found multiple matches for the file pattern: {filematch}"
                )
            elif len(filematch) == 0:
                raise Exception(f"Found no matches for pattern '{filepath}'")
            else:
                filepath = Path(filematch[0]).resolve()

            formatted_paths.append(filepath)

        return formatted_paths

    def decompress_tar_bz2(self, out_path: Union[str, Path]) -> None:
        with tarfile.open(self.file_path, "r:bz2") as new_file:
            new_file.extractall(path=out_path)

    def compress_to_tar_bz2(self) -> Path:
        out_path = self.file_path.with_suffix(".tar.bz2")
        with tarfile.open(name=out_path, mode="w:bz2") as tar:
            tar.add(
                self.file_path, arcname=self.file_path.name, recursive=False
            )

        return out_path

    @staticmethod
    def compress_dir_to_tar_bz2(
        dir: Union[str, Path], out_name: Optional[str] = None
    ) -> Path:
        dir_path = Path(dir).resolve()
        if not dir_path.is_dir():
            raise Exception("You need to provided a valid directory path")

        if not out_name:
            out_name = dir_path.name

        out_path = dir_path.joinpath(out_name).with_suffix(".tar.bz2")
        with tarfile.open(name=out_path, mode="w:bz2") as tar:
            tar.add(dir_path, arcname=out_name)

        return out_path


class FastaFile(FileManager):
    def __init__(self, path: Union[str, Path]) -> None:
        super().__init__(path)

    def read(self) -> List[Tuple[str, str]]:
        with self.file_path.open() as fasta:
            return [
                (part[0], part[2].replace("\n", ""))
                for part in [
                    entry.partition("\n")
                    for entry in fasta.read().split(">")[1:]
                ]
            ]

    def as_df(self) -> pd.DataFrame:
        dtypes = {"header": "object", "sequence": "object"}

        fasta_data = self.read()
        fasta_df = pd.DataFrame(data=fasta_data, columns=dtypes.keys())
        fasta_df = fasta_df.astype(dtype=dtypes)

        return fasta_df


class JSONFile(FileManager):
    def __init__(self, path: Union[str, Path]) -> None:
        super().__init__(path)

    def load(self) -> Dict[Any, Any]:
        with open(self.file_path, "r") as json_file:
            json_data = json.loads(json_file.read())

        return json_data

    @classmethod
    def write_json(
        cls, json_obj: Dict[Any, Any], out_path: Union[str, Path]
    ) -> Path:
        json_path = Path(out_path).resolve()
        if json_path.is_dir():
            json_path = json_path.joinpath("file.json")

        json_str = json.dumps(
            obj=json_obj, indent=4, default=str, sort_keys=True
        )

        with json_path.open(mode="w") as json_file:
            json_file.write(json_str)

        return json_path


class TSVFile(FileManager):
    def __init__(
        self,
        path: Union[str, Path],
        col_patterns: Optional[Dict[Any, Any]] = None,
        header: bool = False,
    ) -> None:
        # ? Initializing superclass
        super().__init__(path)
        self.header = 0 if header else None
        self.col_patterns = col_patterns

        # ? Validating column types with regex
        if self.col_patterns:
            self.validate_cols()

    def validate_cols(self) -> None:
        n_cols = len(self.col_patterns)
        input_data: Dict[Any, Any] = {col: [] for col in self.col_patterns}

        with self.file_path.open() as tsv:
            if self.header == 0:
                tsv.readline()
            for n, line in enumerate(tsv.readlines()):
                data = line.strip().split("\t")
                if len(data) != n_cols:
                    raise Exception(
                        f"There was a problem parsing file {self.file_path}. "
                        f"Found an unexpect number of columns at line {n+1}."
                    )
                for data_field, field in zip(data, input_data):
                    input_data[field].append(data_field)

        for field, field_data in input_data.items():
            field_pattern = self.col_patterns[field]
            nonpattern = [
                data
                for data in field_data
                if not re.fullmatch(field_pattern, data)
            ]
            if nonpattern:
                raise Exception(
                    "Found out-of-pattern data on the file for field "
                    f"{field}: {nonpattern}"
                )

    def as_df(
        self,
        dtypes: Dict[str, str],
    ) -> pd.DataFrame:
        df = pd.read_csv(
            self.file_path,
            sep="\t",
            dtype=dtypes,
            header=self.header,
            names=list(dtypes.keys()),
        )

        return df

    def as_data_list(self, dtypes: Dict[str, str]) -> List[List[Any]]:
        as_df = self.as_df(dtypes=dtypes)
        return as_df.values.tolist()
