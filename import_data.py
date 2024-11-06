import re
from pathlib import Path

import polars as pl


def extract_file_info(file_name: str) -> dict:
    pattern = re.compile(r"(\d+)_(\d+)_Lane(\d+)")
    match = pattern.match(file_name)
    if match:
        return {
            "Sample_Name": f"{match.group(1)}_{match.group(2)}",
            "Lane_Id": int(match.group(3)),
        }
    else:
        raise ValueError(f"Invalid file name format: {file_name}")


def read_metrics_file(file_path: Path) -> pl.DataFrame:
    return pl.read_csv(
        file_path,
        separator=",",
        has_header=False,
        new_columns=["type", "adapter", "metric", "value", "percent"],
    )


def main():
    base_path = Path("qc_data")  # Der Basisordner wurde auf "qc_data" gesetzt
    all_data = []

    for metrics_file in base_path.iterdir():
        if metrics_file.is_file() and metrics_file.suffix == ".csv":
            file_info = extract_file_info(metrics_file.stem)

            df = read_metrics_file(metrics_file)
            # Filtern nach "MAPPING/ALIGNING SUMMARY" und Spalte "type" entfernen
            df = df.filter(pl.col("type") == "MAPPING/ALIGNING SUMMARY").drop(
                ["type", "adapter"]
            )
            # Hinzufügen der extrahierten Informationen als Spalten
            df = df.with_columns(
                pl.lit(file_info["Sample_Name"]).alias("Sample_Name"),
                pl.lit(file_info["Lane_Id"]).alias("Lane_Id"),
            )
            # Spaltenreihenfolge ändern, um "Sample_Name" und "Lane_Id" vorne zu haben
            df = df.select(
                ["Sample_Name", "Lane_Id"]
                + [col for col in df.columns if col not in ["Sample_Name", "Lane_Id"]]
            )
            all_data.append(df)

    if all_data:
        final_df = pl.concat(all_data)
        final_df.write_parquet("qc_metrics.parquet")
        print(final_df.sample(5))
    else:
        print("No data found.")


if __name__ == "__main__":
    main()
