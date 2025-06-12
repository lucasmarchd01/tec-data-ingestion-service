import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

EXPECTED_COLUMNS = {
    "loc": "object",  # String
    "loc_zn": "object",
    "loc_name": "object",
    "loc_purp_desc": "object",
    "loc_qti": "object",
    "flow_ind": "object",
    "dc": "Int64",  # Pandas nullable integer
    "opc": "Int64",
    "tsq": "Int64",
    "oac": "Int64",
    "it": "boolean",  # Pandas nullable boolean
    "auth_overrun_ind": "boolean",
    "nom_cap_exceed_ind": "boolean",
    "all_qty_avail": "boolean",
    "qty_reason": "object",
}

NUMERIC_COLUMNS = ["dc", "opc", "tsq", "oac"]
BOOLEAN_COLUMNS = ["it", "auth_overrun_ind", "nom_cap_exceed_ind", "all_qty_avail"]


def validate_dataframe(df: pd.DataFrame, file_path: str) -> pd.DataFrame | None:
    """
    Validates the DataFrame structure, data types, and basic content.

    Args:
        df: The pandas DataFrame to validate.
        file_path: The path of the CSV file being validated (for logging/error context).

    Returns:
        The validated (and potentially cleaned) DataFrame, or None if validation fails.
    """
    logging.info(f"Starting validation for {file_path}...")
    validated_df = df.copy()

    # 1. Check for missing columns
    missing_cols = set(EXPECTED_COLUMNS.keys()) - set(validated_df.columns)
    if missing_cols:
        logging.error(
            f"Error in {file_path}: Missing expected columns: {missing_cols}. Skipping file."
        )
        return None

    # 2. Data type conversion and validation
    for col, expected_type in EXPECTED_COLUMNS.items():
        if col not in validated_df.columns:
            continue

        # Handle numeric types with potential coercion issues
        if expected_type == "Int64":
            try:
                # Convert to numeric, coercing errors to NaT/NaN. Then attempt Int64 conversion.
                validated_df[col] = pd.to_numeric(validated_df[col], errors="coerce")
                # Check if all values became NaN after coercion, which might indicate a fully non-numeric column
                if validated_df[col].isnull().all() and not df[col].isnull().all():
                    logging.warning(
                        f"Warning in {file_path}: Column '{col}' contains mostly non-numeric values and was coerced to all NaNs."
                    )
                validated_df[col] = validated_df[col].astype(pd.Int64Dtype())
            except Exception as e:
                logging.error(
                    f"Error in {file_path}: Could not convert column '{col}' to Int64. Error: {e}. Skipping file."
                )
                return None
        elif expected_type == "boolean":
            # This conversion is now handled in uploader.py before calling validate_dataframe
            # but we can still check the dtype here.
            if not pd.api.types.is_bool_dtype(
                validated_df[col]
            ) and not pd.api.types.is_object_dtype(validated_df[col]):
                # If it's object, it might be mixed True/False/None from map, allow it.
                # If it's already boolean, great.
                # Otherwise, it's an issue.
                logging.warning(
                    f"Warning in {file_path}: Column '{col}' is not of boolean type after conversion. Actual type: {validated_df[col].dtype}"
                )
        else:  # For 'object' (string) types, mostly rely on read_csv, but can add specific checks
            try:
                validated_df[col] = validated_df[col].astype(expected_type)
            except TypeError as e:
                logging.error(
                    f"Error in {file_path}: Could not convert column '{col}' to {expected_type}. Error: {e}. Skipping file."
                )
                return None

    # 3. Check for negative values in numeric columns where it doesn't make sense
    # (e.g., capacities, quantities)
    for col in NUMERIC_COLUMNS:
        if col in validated_df.columns and (validated_df[col] < 0).any():
            logging.warning(
                f"Warning in {file_path}: Column '{col}' contains negative values. Review data integrity."
            )
            # Depending on policy, you might choose to:
            #   - return None (reject file)
            #   - set negative values to NaN: validated_df.loc[validated_df[col] < 0, col] = pd.NA
            #   - log and continue (current behavior)

    # 4. Check for nulls in critical columns (example: 'loc' - Location ID)
    critical_cols_for_null_check = ["loc"]
    for col in critical_cols_for_null_check:
        if col in validated_df.columns and validated_df[col].isnull().any():
            logging.warning(
                f"Warning in {file_path}: Critical column '{col}' contains null values."
            )
            # Depending on policy, might return None or filter out rows with nulls.

    # 5. Value range checks (example)

    # 6. Specific value checks for certain columns

    logging.info(f"Validation finished for {file_path}.")
    return validated_df
