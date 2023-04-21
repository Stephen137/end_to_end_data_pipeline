from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs() -> Path:
    """Download data from GCS"""
    gcs_path = Path(f"data/spotify.parquet")
    gcs_block = GcsBucket.load("de-zoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    return Path(f"{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Print some basic info"""
    df = pd.read_parquet(path)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df
 

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-gcp-creds")

    df.to_gbq(
        destination_table="spotify.spotify_one_point_two_million",
        project_id="de-zoomcamp-project137",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs()
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()