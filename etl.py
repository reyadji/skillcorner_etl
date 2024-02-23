import argparse
import datetime
import json
import logging
import pandas as pd
import os
import sqlite3


# CONSTANTS
SQL_DB = "srcftbl.db"


class TrackingEtl:
    def __init__(self, metadata_file=None, tracking_file=None):
        self.metadata_file = metadata_file
        self.tracking_file = tracking_file
        self.metadata = dict()
        self.tracking_df = pd.DataFrame()
        self.metadata_df = pd.DataFrame()
        self.track_df = pd.DataFrame()
        self.possession_df = pd.DataFrame()
        self.frame_df = pd.DataFrame()
        self.players_df = pd.DataFrame()

    def extract_metadata(self):
        with open(self.metadata_file, "r") as f:
            metadata_str = f.read()
        self.metadata = json.loads(metadata_str)

    def extract_tracking(self):
        self.tracking_df = pd.read_json(self.tracking_file, lines=True)
        tracking_game_id = self.tracking_file.split("_")[0]
        self.tracking_df["game_id"] = tracking_game_id

    def transform_metadata(self):
        self.players_df = pd.DataFrame.from_dict(self.metadata.pop("players"))
        self.players_df["player_role"] = self.players_df["player_role"].astype(str)
        self.players_df["game_id"] = self.metadata["id"]
        self.metadata_df = pd.json_normalize(self.metadata)
        # Convert metadata dataframe to string dataype
        # for SQLite acceptability
        self.metadata_df = self.metadata_df.map(str)
        self.metadata_df = self.metadata_df.rename(columns={"id": "game_id"})

    def transform_tracking(self):
        # Filter out empty tracking data
        self.tracking_df = self.tracking_df[
            self.tracking_df["data"].apply(lambda x: len(x)) > 0
        ]
        self.tracking_df = self.tracking_df.dropna(subset=["timestamp"])

        def transform_track():
            self.track_df = self.tracking_df[["game_id", "data", "frame"]]
            self.track_df = self.track_df.explode("data").reset_index(drop=True)
            self.track_df = self.track_df.join(
                pd.json_normalize(self.track_df.pop("data"))
            )
            # Convert trackable object to string for uniformity
            self.track_df["trackable_object"] = self.track_df[
                "trackable_object"
            ].astype(str)

        def transform_possession():
            self.possession_df = self.tracking_df[["game_id", "possession"]]
            self.possession_df = self.possession_df.join(
                pd.json_normalize(self.possession_df.pop("possession"))
            )
            # Convert trackable object to string for uniformity
            self.possession_df["trackable_object"] = (
                self.possession_df["trackable_object"].astype("Int64").astype(str)
            )

        def transform_frame():
            self.frame_df = self.tracking_df[
                [
                    "game_id",
                    "frame",
                    "image_corners_projection",
                    "timestamp",
                    "period",
                ]
            ]
            # Drop rows with missing timestamp
            self.frame_df["timestamp"] = pd.to_datetime(self.frame_df["timestamp"])
            # Convert timestamp to seconds for SQLite acceptability, and also for easier filtering and aggregation
            self.frame_df["timestamp_in_seconds"] = (
                self.frame_df["timestamp"].dt.hour * 3600
                + self.frame_df["timestamp"].dt.minute * 60
                + self.frame_df["timestamp"].dt.second
            )
            self.frame_df["timestamp_in_seconds"] = self.frame_df[
                "timestamp_in_seconds"
            ].astype(int)
            self.frame_df = self.frame_df.drop(["timestamp"], axis=1)
            self.frame_df["period"] = self.frame_df["period"].astype(int)
            # Convert image corners projection to string for SQLite acceptability
            self.frame_df["image_corners_projection"] = self.frame_df[
                "image_corners_projection"
            ].astype(str)

        transform_track()
        transform_possession()
        transform_frame()

    def load_to_sql(self):
        with sqlite3.connect(SQL_DB) as con:
            for table, df in self.db_tables.items():
                inserted_rows = df.to_sql(table, con=con, if_exists="append")
                logging.info(f"Inserted {inserted_rows} rows into {table} table.")

    def load_to_parquet(self):
        for table, df in self.db_tables.items():
            parquet_file = f"{table}.parquet"
            if os.path.exists(parquet_file):
                df.to_parquet(parquet_file, engine="fastparquet", append=True)
            else:
                df.to_parquet(parquet_file, engine="fastparquet")

    def set_tables_dict(self):
        self.db_tables = {
            "metadata": self.metadata_df,
            "track": self.track_df,
            "possession": self.possession_df,
            "frame": self.frame_df,
            "player": self.players_df,
        }

    def run(self):
        start_time = datetime.datetime.now()
        # Extract
        self.extract_metadata()
        self.extract_tracking()

        # Transform
        self.transform_metadata()
        self.transform_tracking()

        self.set_tables_dict()

        # Load
        self.load_to_sql()
        self.load_to_parquet()

        run_time = datetime.datetime.now() - start_time

        logging.info(f"ETL process completed in {run_time}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m", "--metadata_file", type=str, help="Path to the metadata file."
    )
    parser.add_argument(
        "-t", "--tracking_file", type=str, help="Path to the tracking file."
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Increase output verbosity."
    )
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if not os.path.exists(args.metadata_file):
        raise FileNotFoundError(f"{args.metadata_file} does not exist.")

    if not os.path.exists(args.tracking_file):
        raise FileNotFoundError(f"{args.tracking_file} does not exist.")

    etl = TrackingEtl(args.metadata_file, args.tracking_file)
    etl.run()
