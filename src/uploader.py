import pandas as pd
from sqlalchemy import create_engine, text, BOOLEAN, INTEGER
import os

# Import the validator function
from validator import validate_dataframe

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
                it BOOLEAN,
                auth_overrun_ind BOOLEAN,
                nom_cap_exceed_ind BOOLEAN,
                all_qty_avail BOOLEAN,
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


def convert_to_boolean(df, columns):
    """Converts 'Y'/'N' columns to boolean True/False."""
    for col in columns:
        if col in df.columns:
            df[col] = (
                df[col].map({"Y": True, "N": False, "": None}).astype(pd.BooleanDtype())
            )
    return df


def insert_data_from_csv_pandas(engine, csv_filepath):
    """Parses a CSV file, validates it, transforms data, and inserts it into the table using pandas.to_sql."""
    try:
        df = pd.read_csv(csv_filepath)
        if df.empty:
            print(f"Warning: CSV file {csv_filepath} is empty. Skipping.")
            return

        df = clean_column_names(df)

        # Columns to convert to boolean
        boolean_columns = [
            "it",
            "auth_overrun_ind",
            "nom_cap_exceed_ind",
            "all_qty_avail",
        ]
        df = convert_to_boolean(df, boolean_columns)

        # Validate the DataFrame
        df_validated = validate_dataframe(
            df.copy(), csv_filepath
        )  # Pass filepath for context in validation

        if df_validated is None:
            print(f"Validation failed for {csv_filepath}. Skipping insertion.")
            return

        # Ensure all expected columns exist in the DataFrame, add if missing with None/NaN
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
            if col not in df_validated.columns:
                df_validated[col] = pd.NA  # Use pd.NA for nullable dtypes

        # Select only the columns that match the table schema to avoid errors
        df_to_insert = df_validated[expected_db_cols]

        # Explicitly cast integer columns to handle potential pd.NA before to_sql
        # This is important because to_sql might struggle with mixed types if pd.NA is present in int columns
        int_columns = ["dc", "opc", "tsq", "oac"]
        for col in int_columns:
            if col in df_to_insert.columns:
                # Convert to float first to handle NA, then to Int64 (nullable integer)
                df_to_insert[col] = pd.to_numeric(
                    df_to_insert[col], errors="coerce"
                ).astype(pd.Int64Dtype())

        df_to_insert.to_sql(
            TABLE_NAME,
            engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            dtype={  # Specify SQLAlchemy types for boolean columns
                "it": BOOLEAN,
                "auth_overrun_ind": BOOLEAN,
                "nom_cap_exceed_ind": BOOLEAN,
                "all_qty_avail": BOOLEAN,
                "dc": INTEGER,
                "opc": INTEGER,
                "tsq": INTEGER,
                "oac": INTEGER,
            },
        )
    except (
        pd.errors.EmptyDataError
    ):  # Should be caught by the initial check, but good to keep
        print(
            f"Warning: CSV file {csv_filepath} is empty (caught by specific exception). Skipping."
        )
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
                # print(f"Finished processing {csv_filepath}.") # Removed to reduce noise, error messages will indicate issues

        print("Data upload complete.")

    except Exception as e:  # Catching a broader SQLAlchemyError or general Exception
        print(f"An error occurred: {e}")
    finally:
        if engine:
            engine.dispose()  # Dispose of the engine's connection pool


if __name__ == "__main__":
    main()
