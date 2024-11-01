from typing import Dict, List, Callable
import luigi
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from pandas import DataFrame


from lib.utility import (
    run_ELT,
    combine_dataframes,
    extract,
    load,
    create_graph,
    LINEPLOT,
    RELPLOT,
)
from lib.transformers import (
    calculate_percentage_of_identity_theft_online_per_year,
    calculate_burglaries_at_construction_sites_per_month,
)


class CalculatePercentageOfIdentityTheftOnlinePerYear(luigi.Task):
    csv_files: Dict[Path, Callable[[DataFrame], DataFrame]] = {
        Path(
            "data", "Crime_Data_from_2020_to_Present.csv"
        ): calculate_percentage_of_identity_theft_online_per_year,
        Path(
            "data", "Crime_Data_from_2010_to_2019.csv"
        ): calculate_percentage_of_identity_theft_online_per_year,
    }
    output_files: List[Path] = [
        Path("output", f"{file.stem}_transformed.csv") for file in csv_files
    ]

    def output(self) -> Dict[str, luigi.LocalTarget]:
        return {
            f"output_{i}": luigi.LocalTarget(path=file)
            for i, file in enumerate(iterable=self.output_files)
        }

    def run(self) -> None:
        with ProcessPoolExecutor() as executor:
            executor.map(
                run_ELT,
                self.csv_files.keys(),
                self.output_files,
                self.csv_files.values(),
            )


class CombinePercentageOfIdentityTheftOnlinePerYear(luigi.Task):
    output_file: Path = Path(
        "output", "combined_percentage_of_identity_theft_online_per_year.csv"
    )
    input_files: List[Path] = [
        Path("output", f"{file.stem}_transformed.csv")
        for file in CalculatePercentageOfIdentityTheftOnlinePerYear.csv_files
    ]

    def requires(self) -> CalculatePercentageOfIdentityTheftOnlinePerYear:
        return CalculatePercentageOfIdentityTheftOnlinePerYear()

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


class CalculateBurglariesAtConstructionSitesPerMonth(luigi.Task):
    csv_files: Dict[Path, Callable[[DataFrame], DataFrame]] = {
        Path(
            "data", "Crimes_-_2001_to_Present.csv"
        ): calculate_burglaries_at_construction_sites_per_month,
    }
    output_files: List[Path] = [
        Path("output", f"{file.stem}_transformed.csv") for file in csv_files
    ]

    def output(self) -> Dict[str, luigi.LocalTarget]:
        return {
            f"output_{i}": luigi.LocalTarget(path=file)
            for i, file in enumerate(iterable=self.output_files)
        }

    def run(self) -> None:
        with ProcessPoolExecutor() as executor:
            executor.map(
                run_ELT,
                self.csv_files.keys(),
                self.output_files,
                self.csv_files.values(),
            )


class CreateIdentityTheftGraph(luigi.Task):
    output_file: Path = Path("output", "identity_theft_graph.png")
    input_file: Path = Path(
        "output", "combined_percentage_of_identity_theft_online_per_year.csv"
    )

    def requires(self) -> CombinePercentageOfIdentityTheftOnlinePerYear:
        return CombinePercentageOfIdentityTheftOnlinePerYear()

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(path=self.output_file)

    def run(self) -> None:
        data: DataFrame = extract(file_name=self.input_file)
        create_graph(
            data=data,
            output_file=self.output_file,
            graph_type=LINEPLOT,
            x="YEAR",
            y="PERCENTAGE",
        )


class CreateBurglariesGraph(luigi.Task):
    output_file: Path = Path("output", "burglaries_graph.png")
    input_file: Path = Path("output", "Crimes_-_2001_to_Present_transformed.csv")

    def requires(self) -> CalculateBurglariesAtConstructionSitesPerMonth:
        return CalculateBurglariesAtConstructionSitesPerMonth()

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(path=self.output_file)

    def run(self) -> None:
        data: DataFrame = extract(file_name=self.input_file)
        create_graph(
            data=data,
            output_file=self.output_file,
            graph_type=RELPLOT,
            kind="line",
            x="MONTH",
            y="BURGLARIES",
            hue="YEAR",
            marker="o",
            aspect=2.5,
        )


if __name__ == "__main__":
    if not Path("./output").exists():
        Path("./output").mkdir()
    luigi.build(
        tasks=[
            CreateBurglariesGraph(),
            CreateIdentityTheftGraph(),
            CombinePercentageOfIdentityTheftOnlinePerYear(),
            CalculatePercentageOfIdentityTheftOnlinePerYear(),
        ],
        local_scheduler=False,
        workers=4,
    )
