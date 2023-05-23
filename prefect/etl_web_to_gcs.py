from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pyarrow as pa
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
   """Some pandas transforms and print basic info"""
   df = df.drop(['id', 'album_id', 'artist_ids', 'track_number', 'disc_number', 'time_signature'], axis=1)
   df.loc[815351:815360,'year'] = 2018
   df.loc[450071:450076,'year'] = 1996
   df.loc[459980:459987,'year'] = 1991
   df['artists'] = df['artists'].str.strip("['']")
   df['danceability'] = df['danceability'].round(2)
   df['energy'] = df['energy'].round(2)
   df['loudness'] = df['loudness'].round(2)
   df['speechiness'] = df['speechiness'].round(2)
   df['acousticness'] = df['acousticness'].round(2)
   df['instrumentalness'] = df['instrumentalness'].round(2)
   df['liveness'] = df['liveness'].round(2)
   df['valence'] = df['valence'].round(2)
   df["tempo"] = df["tempo"].astype(int)
   df['year_date'] = pd.to_datetime(df['year'], format='%Y')
   df["duration_s"] = (df["duration_ms"] / 1000).astype(int).round(0)

   print(df.head(2))
   print(f"columns: {df.dtypes}")
   print(f"rows: {len(df)}")
   return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
   """Write DataFrame out locally as parquet file"""
   path = Path(f"data/{dataset_file}.parquet")
   df.to_parquet(path, compression="gzip")
   return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    dataset_file = "spotify"
    dataset_url = "data/spotify.csv"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,dataset_file)
    write_gcs(path)
    
if __name__ == "__main__":
    etl_web_to_gcs()
