"""
Shared utilities for Excel parsing operations.
Centralized header normalization and common DataFrame operations.
"""

import pandas as pd
import re
import json
import warnings
from pathlib import Path
from typing import Optional, Dict, List, Any
import logging

# Configure pandas to use future behavior for replace downcasting (suppresses FutureWarning)
pd.set_option("future.no_silent_downcasting", True)

# Suppress openpyxl warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Suppress pandas FutureWarning about downcasting behavior in replace
warnings.filterwarnings(
    "ignore", category=FutureWarning, message=".*Downcasting behavior in.*replace.*"
)


class HeaderNormalizer:
    """Centralized header normalization with consistent rules."""

    @staticmethod
    def normalize_header(header: Any) -> str:
        """
        Normalize column headers to consistent format.

        Args:
            header: Raw header value from Excel

        Returns:
            Normalized header string
        """
        if not isinstance(header, str):
            header = str(header)

        header = header.strip()

        # Remove question marks and non-ASCII characters
        header = re.sub(r"[？?]+", "", header)
        header = re.sub(r"[^\x00-\x7F]+", "", header)

        # Normalize common abbreviations
        header = re.sub(r"o_b_c", "obc", header, flags=re.IGNORECASE)
        header = re.sub(r"b_m_s", "bms", header, flags=re.IGNORECASE)
        header = re.sub(r"m_o_s", "mos", header, flags=re.IGNORECASE)

        # Character replacements
        replacements = {
            "\n": " ",
            "-": " ",
            ".": "_",
            "(": "_",
            ")": "",
            "/": "_",
            "%": "percent",
            "℃": "c",
            "Δ": "delta",
            "δ": "delta",
            "±": "",
            "Ω": "ohm",
        }

        for old, new in replacements.items():
            header = header.replace(old, new)

        # Clean up whitespace and underscores
        header = re.sub(r"\s+", "_", header)
        header = header.lower()
        header = re.sub(r"_+", "_", header)
        header = header.strip("_")
        header = header.replace(" ", "")

        return header


class DataFrameProcessor:
    """Optimized DataFrame processing operations."""

    @staticmethod
    def clean_dataframe(
        df: pd.DataFrame, drop_no_column: bool = True, normalize_headers: bool = True
    ) -> pd.DataFrame:
        """
        Clean and normalize DataFrame with performance optimizations.

        Args:
            df: Input DataFrame
            drop_no_column: Whether to drop 'No.' columns
            normalize_headers: Whether to normalize column headers

        Returns:
            Cleaned DataFrame
        """
        if df is None or df.empty:
            return df

        # Drop 'No.' column if requested - optimized check
        if drop_no_column and len(df.columns) > 0:
            first_col = str(df.columns[0]).lower().strip()
            if first_col in ["no.", "no"]:
                df = df.drop(df.columns[0], axis=1)

        # Normalize headers efficiently
        if normalize_headers:
            # Vectorized header normalization
            df.columns = [HeaderNormalizer.normalize_header(col) for col in df.columns]

        # Remove completely empty rows and columns efficiently
        # Use dropna with 'all' to remove only completely empty rows/columns
        if len(df) > 1000:  # For large DataFrames, be more selective
            # Remove rows where all values are NaN or empty strings
            # Use mask-based approach to avoid FutureWarning
            mask = df.astype(str).apply(lambda x: x.str.match(r"^\s*$")).any(axis=1)
            df.loc[mask] = pd.NA
            df = df.dropna(axis=0, how="all")  # Remove empty rows
            # Only drop empty columns if they are truly empty (all NaN)
            df = df.dropna(axis=1, how="all")  # Remove empty columns
        else:
            df = df.dropna(axis=0, how="all")  # Remove empty rows
            df = df.dropna(axis=1, how="all")  # Remove empty columns

        # Reset index for better performance after row removal
        if len(df) != len(df.index):
            df = df.reset_index(drop=True)

        return df

    @staticmethod
    def extract_header_from_row(df: pd.DataFrame, row_index: int) -> pd.DataFrame:
        """
        Extract headers from specific row efficiently.

        Args:
            df: Input DataFrame
            row_index: Row index to use as headers

        Returns:
            DataFrame with new headers
        """
        if df is None or df.empty or row_index >= df.shape[0]:
            return df

        # Extract headers efficiently
        new_headers = df.iloc[row_index].values
        df.columns = new_headers

        # Remove header row and reset index for better performance
        df = df.iloc[row_index + 1 :].reset_index(drop=True)

        return df

    @staticmethod
    def process_large_dataframe(
        df: pd.DataFrame, chunk_size: int = 10000
    ) -> pd.DataFrame:
        """
        Process large DataFrames in chunks for better memory efficiency.

        Args:
            df: Input DataFrame
            chunk_size: Size of chunks to process

        Returns:
            Processed DataFrame
        """
        if df is None or df.empty or len(df) <= chunk_size:
            return df

        # Process in chunks to avoid memory issues
        processed_chunks = []
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            # Apply basic cleaning to chunk
            chunk = chunk.dropna(axis=0, how="all")
            processed_chunks.append(chunk)

        # Combine chunks efficiently
        if processed_chunks:
            return pd.concat(processed_chunks, ignore_index=True)

        return df


class ExcelLoader:
    """Optimized Excel file loading with lazy loading and performance improvements."""

    @staticmethod
    def load_workbook(file_path: str) -> Dict[str, pd.DataFrame]:
        """
        Load all sheets from Excel workbook with optimizations.

        Args:
            file_path: Path to Excel file

        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        try:
            # Use optimized read_excel with performance settings
            sheets = pd.read_excel(
                file_path,
                sheet_name=None,
                engine="openpyxl",
                header=None,
                # Performance optimizations
                keep_default_na=False,  # Don't convert to NaN unnecessarily
                na_filter=False,  # Skip NaN detection for speed
            )
            return sheets
        except Exception as e:
            return {}

    @staticmethod
    def load_sheet(file_path: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """
        Load a single sheet with optimizations.

        Args:
            file_path: Path to Excel file
            sheet_name: Name of the sheet to load

        Returns:
            DataFrame or None if not found
        """
        try:
            # Load only the specific sheet for better performance
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                engine="openpyxl",
                header=None,
                # Performance optimizations
                keep_default_na=False,
                na_filter=False,
            )
            return df
        except Exception as e:
            return None

    @staticmethod
    def load_required_sheets(
        file_path: str, sheet_names: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """
        Load only required sheets for better performance.

        Args:
            file_path: Path to Excel file
            sheet_names: List of sheet names to load

        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        try:
            # Load only the required sheets
            sheets = pd.read_excel(
                file_path,
                sheet_name=sheet_names,
                engine="openpyxl",
                header=None,
                # Performance optimizations
                keep_default_na=False,
                na_filter=False,
            )
            return sheets
        except Exception as e:
            return {}


class ValidationUtils:
    """Utilities for data validation."""

    @staticmethod
    def validate_file_path(file_path: str) -> bool:
        """Validate if file path exists and is accessible."""
        import os

        return os.path.exists(file_path) and os.path.isfile(file_path)

    @staticmethod
    def validate_sheet_exists(sheets: Dict[str, pd.DataFrame], sheet_name: str) -> bool:
        """Check if sheet exists in loaded workbook."""
        return sheet_name in sheets

    @staticmethod
    def sanitize_data_for_json(data: Any) -> Any:
        """Optimized data sanitization for JSON serialization."""
        # Handle different data types efficiently
        if isinstance(data, dict):
            # Use dict comprehension for better performance
            return {
                k: ValidationUtils.sanitize_data_for_json(v) for k, v in data.items()
            }
        elif isinstance(data, list):
            # Use list comprehension for better performance
            return [ValidationUtils.sanitize_data_for_json(item) for item in data]
        elif pd.isna(data):
            return ""
        elif isinstance(data, (int, float)):
            # Quick check for NaN without conversion
            if pd.isna(data):
                return ""
            return data
        elif data is None:
            return ""
        else:
            # Convert to string efficiently
            return str(data) if data != "" else ""

    @staticmethod
    def fast_sanitize_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fast sanitization for large record lists."""
        if not records:
            return records

        # Vectorized approach for large datasets
        sanitized_records = []
        for record in records:
            sanitized_record = {}
            for key, value in record.items():
                if pd.isna(value):
                    sanitized_record[key] = ""
                elif isinstance(value, (int, float)):
                    sanitized_record[key] = value if not pd.isna(value) else ""
                elif value is None:
                    sanitized_record[key] = ""
                else:
                    sanitized_record[key] = str(value) if value != "" else ""
            sanitized_records.append(sanitized_record)

        return sanitized_records


class JSONFileUtils:
    """Utilities for JSON file operations."""

    @staticmethod
    def write_json_result(result: Dict[str, Any], output_path: str) -> bool:
        """
        Write parsed result to JSON file.

        Args:
            result: Parsed data to write
            output_path: Path for output JSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure output directory exists
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Sanitize data for JSON serialization
            sanitized_result = ValidationUtils.sanitize_data_for_json(result)

            # Write JSON file
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(sanitized_result, f, indent=2, ensure_ascii=False)

            logging.info(f"JSON result written to: {output_path}")
            return True

        except Exception as e:
            logging.error(f"Error writing JSON result to {output_path}: {e}")
            return False

    @staticmethod
    def generate_output_filename(input_path: str, output_dir: str = "result") -> str:
        """
        Generate output JSON filename from input Excel filename.

        Args:
            input_path: Path to input Excel file
            output_dir: Directory for output files (default: "result")

        Returns:
            Generated output file path
        """
        input_file = Path(input_path)
        output_filename = f"{input_file.stem}_parsed.json"
        return str(Path(output_dir) / output_filename)
