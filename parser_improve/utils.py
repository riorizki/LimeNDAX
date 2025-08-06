"""
Shared utilities for Excel parsing operations.
Centralized header normalization and common DataFrame operations.
"""

import pandas as pd
import re
import json
from pathlib import Path
from typing import Optional, Dict, List, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    """Centralized DataFrame processing operations."""

    @staticmethod
    def clean_dataframe(
        df: pd.DataFrame, drop_no_column: bool = True, normalize_headers: bool = True
    ) -> pd.DataFrame:
        """
        Clean and normalize DataFrame.

        Args:
            df: Input DataFrame
            drop_no_column: Whether to drop 'No.' columns
            normalize_headers: Whether to normalize column headers

        Returns:
            Cleaned DataFrame
        """
        if df is None or df.empty:
            return df

        # Drop 'No.' column if requested
        if drop_no_column and len(df.columns) > 0:
            if str(df.columns[0]).lower() in ["no.", "no"]:
                df = df.drop(df.columns[0], axis=1)

        # Normalize headers
        if normalize_headers:
            df.columns = [HeaderNormalizer.normalize_header(col) for col in df.columns]

        # Remove empty rows and columns
        df = df.dropna(axis=1, how="all")
        df = df.dropna(axis=0, how="all")

        # Convert comma decimals to dot decimals
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).str.replace(",", ".", regex=False)

        return df

    @staticmethod
    def extract_header_from_row(df: pd.DataFrame, header_row: int) -> pd.DataFrame:
        """
        Extract headers from specific row and clean DataFrame.

        Args:
            df: Input DataFrame
            header_row: Row index to use as headers (0-based)

        Returns:
            DataFrame with proper headers
        """
        if df is None or df.empty or header_row >= df.shape[0]:
            return df

        df.columns = df.iloc[header_row].values
        df = df.iloc[header_row + 1 :]
        return df


class ExcelLoader:
    """Centralized Excel file loading with error handling."""

    @staticmethod
    def load_workbook(file_path: str) -> Dict[str, pd.DataFrame]:
        """
        Load all sheets from Excel workbook.

        Args:
            file_path: Path to Excel file

        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        try:
            sheets = pd.read_excel(
                file_path, sheet_name=None, engine="openpyxl", header=None
            )
            logger.info(f"Successfully loaded {len(sheets)} sheets from {file_path}")
            return sheets
        except Exception as e:
            logger.error(f"Failed to load workbook {file_path}: {e}")
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
        """Sanitize data for JSON serialization."""
        if isinstance(data, dict):
            return {
                k: ValidationUtils.sanitize_data_for_json(v) for k, v in data.items()
            }
        elif isinstance(data, list):
            return [ValidationUtils.sanitize_data_for_json(item) for item in data]
        elif pd.isna(data):
            return ""
        elif isinstance(data, (int, float)):
            return data if not pd.isna(data) else ""
        else:
            return str(data)


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
