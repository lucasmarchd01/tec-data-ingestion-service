import pandas as pd
from sqlalchemy import create_engine, text
import os

# Database connection parameters
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "tec_data")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_PORT = os.environ.get("DB_PORT", "5432")  # Default PostgreSQL port

TABLE_NAME = "tec_data"


def get_db_engine():
    """Creates a SQLAlchemy database engine."""
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def create_table_if_not_exists(engine):
    """Creates the data table using SQLAlchemy if it doesn't exist."""
    with engine.connect() as connection:
        connection.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                loc VARCHAR(255),
                loc_zn VARCHAR(255),
                loc_name VARCHAR(255),
                loc_purp_desc VARCHAR(255),
                loc_qti VARCHAR(255),
                flow_ind VARCHAR(10),
                dc INTEGER,
                opc INTEGER,
                tsq INTEGER,
                oac INTEGER,
                it VARCHAR(10),
                auth_overrun_ind VARCHAR(10),
                nom_cap_exceed_ind VARCHAR(10),
                all_qty_avail VARCHAR(10),
                qty_reason VARCHAR(255)
            );
        """
            )
        )
        connection.commit()


def clean_column_names(df):
    """Cleans DataFrame column names to match database schema."""
    cols = df.columns
    new_cols = {col: col.lower().replace(" ", "_").replace("/", "_") for col in cols}
    # Specific renaming for columns that don't fit the general rule
    new_cols["loc/qti"] = "loc_qti"  # Ensure this matches if general rule isn't enough
    df = df.rename(columns=new_cols)
    return df


def insert_data_from_csv_pandas(engine, csv_filepath):
    """Parses a CSV file and inserts its data into the table using pandas.to_sql."""
    try:
        df = pd.read_csv(csv_filepath)
        df = clean_column_names(df)

        # Ensure all expected columns exist in the DataFrame, add if missing with None/NaN
        # This is important if some CSVs might have missing optional columns
        expected_db_cols = [
            "loc",
            "loc_zn",
            "loc_name",
            "loc_purp_desc",
            "loc_qti",
            "flow_ind",
            "dc",
            "opc",
            "tsq",
            "oac",
            "it",
            "auth_overrun_ind",
            "nom_cap_exceed_ind",
            "all_qty_avail",
            "qty_reason",
        ]
        for col in expected_db_cols:
            if col not in df.columns:
                df[col] = None  # Or pd.NA

        # Select only the columns that match the table schema to avoid errors
        df_to_insert = df[expected_db_cols]

        df_to_insert.to_sql(
            TABLE_NAME, engine, if_exists="append", index=False, chunksize=1000
        )  # Added chunksize
    except pd.errors.EmptyDataError:
        print(f"Warning: CSV file {csv_filepath} is empty. Skipping.")
    except Exception as e:
        print(f"Error processing file {csv_filepath} with pandas: {e}")


def main():
    """Main function to connect to the database, create table, and upload data."""
    engine = None
    try:
        engine = get_db_engine()
        create_table_if_not_exists(engine)

        data_dir = "data"
        if not os.path.isdir(data_dir):
            print(f"Error: Data directory '{data_dir}' not found.")
            return

        for filename in os.listdir(data_dir):
            if filename.endswith(".csv"):
                csv_filepath = os.path.join(data_dir, filename)
                print(f"Processing {csv_filepath}...")
                insert_data_from_csv_pandas(engine, csv_filepath)
                print(f"Finished processing {csv_filepath}.")

        print("Data upload complete.")

    except Exception as e:  # Catching a broader SQLAlchemyError or general Exception
        print(f"An error occurred: {e}")
    finally:
        if engine:
            engine.dispose()  # Dispose of the engine's connection pool


if __name__ == "__main__":
    main()
