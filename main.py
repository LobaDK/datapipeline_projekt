from typing import Dict, List
import luigi
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from pandas import DataFrame


from utility import run_ELT, combine_dataframes, extract, load, create_graph
from transformers import calculate_percentage_of_identity_theft_online_per_year


class CalculatePercentageOfIdentityTheftOnlinePerYear(luigi.Task):
    csv_files: List[Path] = [
        Path("data", "Crime_Data_from_2020_to_Present.csv"),
        Path("data", "Crime_Data_from_2010_to_2019.csv"),
    ]
    output_files: List[Path] = [
        Path("output", f"{file.stem}_transformed.csv") for file in csv_files
    ]

    for file in output_files:
        if file.exists():
            file.unlink()

    def output(self) -> Dict[str, luigi.LocalTarget]:
        return {
            f"output_{i}": luigi.LocalTarget(path=file)
            for i, file in enumerate(iterable=self.output_files)
        }

    def run(self) -> None:
        with ProcessPoolExecutor() as executor:
            executor.map(
                run_ELT,
                self.csv_files,
                self.output_files,
                [calculate_percentage_of_identity_theft_online_per_year]
                * len(self.csv_files),
            )


class CombinePercentageOfIdentityTheftOnlinePerYear(luigi.Task):
    output_file: Path = Path(
        "output", "combined_percentage_of_identity_theft_online_per_year.csv"
    )
    input_files: List[Path] = [
        Path("output", f"{file.stem}_transformed.csv")
        for file in CalculatePercentageOfIdentityTheftOnlinePerYear.csv_files
    ]

    if output_file.exists():
        output_file.unlink()

    def requires(self) -> Dict[str, luigi.Task]:
        return {
            f"task_{i}": CalculatePercentageOfIdentityTheftOnlinePerYear()
            for i in range(len(self.input_files))
        }

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(path=self.output_file)

    def run(self) -> None:
        dataframes: List[DataFrame] = [
            extract(file_name=file) for file in self.input_files
        ]
        combined_data: DataFrame = combine_dataframes(
            dataframes=dataframes, sort_by=["YEAR"]
        )
        load(data=combined_data, file_name=self.output_file)


class CreateGraphs(luigi.Task):
    output_file: Path = Path("output", "graphs.png")
    input_file: Path = Path(
        "output", "combined_percentage_of_identity_theft_online_per_year.csv"
    )

    if output_file.exists():
        output_file.unlink()

    def requires(self) -> Dict[str, luigi.Task]:
        return {"task": CombinePercentageOfIdentityTheftOnlinePerYear()}

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(path=self.output_file)

    def run(self) -> None:
        data: DataFrame = extract(file_name=self.input_file)
        data.plot(x="YEAR", y="PERCENTAGE", kind="line")
        create_graph(data=data, x="YEAR", y="PERCENTAGE", output_file=self.output_file)


if __name__ == "__main__":
    luigi.build(
        tasks=[
            CreateGraphs(),
            CombinePercentageOfIdentityTheftOnlinePerYear(),
            CalculatePercentageOfIdentityTheftOnlinePerYear(),
        ],
        local_scheduler=False,
    )
