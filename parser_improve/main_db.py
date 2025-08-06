#!/usr/bin/env python3
"""
Main entry point for the parser improvement package with advanced database integration.
Simple interface with standardized JSON response format for JavaScript integration.
"""

import json
import re
import sys
import time
import uuid
import warnings
from pathlib import Path
from typing import Any, Dict, Optional

# Suppress openpyxl warnings early
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Add parent directory to path for package imports
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from parser_improve import ImprovedExcelParser
from parser_improve.config import db_config
from parser_improve.database import (
    BatteryPackNotFoundError,
    DatabaseError,
    DatabaseManager,
    TransactionError,
    get_database_manager,
)
from parser_improve.utils import JSONFileUtils


class InputValidator:
    """Input validation utilities."""

    @staticmethod
    def validate_file_path(file_path: str) -> bool:
        """
        Validate file path format and existence.

        Args:
            file_path: Path to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        if not file_path or not isinstance(file_path, str):
            return False

        # Check if file exists and has valid extension
        path = Path(file_path)
        if not path.exists():
            return False

        # Validate file extension
        valid_extensions = {".xlsx", ".xls"}
        if path.suffix.lower() not in valid_extensions:
            return False

        return True

    @staticmethod
    def validate_battery_pack_id(battery_pack_id: str) -> bool:
        """
        Validate battery pack ID format.

        Args:
            battery_pack_id: ID to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        if not battery_pack_id or not isinstance(battery_pack_id, str):
            return False

        # Remove whitespace
        battery_pack_id = battery_pack_id.strip()

        # Check for empty string
        if not battery_pack_id:
            return False

        # Check for basic format (alphanumeric, hyphens, underscores)
        if not re.match(r"^[a-zA-Z0-9_-]+$", battery_pack_id):
            return False

        # Check reasonable length
        if len(battery_pack_id) > 100:
            return False

        return True

    @staticmethod
    def sanitize_input(value: str) -> str:
        """
        Sanitize input string to prevent injection attacks.

        Args:
            value: Input string to sanitize.

        Returns:
            str: Sanitized string.
        """
        if not isinstance(value, str):
            return str(value)

        # Remove null bytes and control characters
        sanitized = value.replace("\x00", "").strip()

        # Limit length to prevent buffer overflow attacks
        if len(sanitized) > 1000:
            sanitized = sanitized[:1000]

        return sanitized


def create_response(
    status: str,
    message: str,
    file_path: Optional[str] = None,
    json_path: Optional[str] = None,
    data: Optional[Dict] = None,
    metadata: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Create standardized response format for JavaScript consumption.

    Args:
        status: Either "SUCCESS" or "ERROR"
        message: Descriptive message
        file_path: Path to the file (optional)
        data: Parsed data (optional)
        metadata: Metadata information (optional)

    Returns:
        Dict: Standardized response format
    """
    return {
        "status": status,
        "message": message,
        "file_path": file_path,
        "json_path": json_path,
        "data": data or {},
        "metadata": metadata or {},
        "timestamp": Path(__file__).stat().st_mtime if file_path else None,
    }


def parse_file_safely(file_path: str, save_json: bool = True) -> Dict[str, Any]:
    """
    Parse Excel file with comprehensive error handling.

    Args:
        file_path: Path to the Excel file to parse.
        save_json: Whether to save JSON file (default True for compatibility).

    Returns:
        Dict: Standardized response with status, message, data, and metadata.
    """
    try:
        # Validate file path
        if not InputValidator.validate_file_path(file_path):
            return create_response(
                "ERROR",
                f"Invalid file path or file does not exist: {file_path}",
                file_path,
            )

        # Initialize parser with default settings
        parser = ImprovedExcelParser()

        # Parse the file
        result = parser.parse_file(file_path)

        # Check if parsing failed
        if "error" in result:
            return create_response(
                "ERROR", f"Parsing failed: {result['error']}", file_path
            )

        json_path = None
        if save_json:
            # Save standardized response to JSON file for review
            input_filename = Path(file_path).stem

            # Generate a unique filename for the response
            uuid_str = str(uuid.uuid4())
            first_section = uuid_str.split("-")[0]
            response_filename = f"{input_filename}_{first_section}_response.json"
            response_output_path = Path("result") / response_filename

            # Ensure result directory exists
            response_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(response_output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=0, ensure_ascii=False)

            json_path = str(response_output_path.absolute())

        # Extract data and metadata from result
        data = result.get("data", {})
        metadata = result.get("metadata", {})

        # Calculate summary statistics for message
        sheets_parsed = len(metadata.get("sheets_parsed", []))
        total_records = sum(
            len(sheet_data)
            for sheet_data in data.values()
            if isinstance(sheet_data, list)
        )

        success_message = f"Successfully parsed {sheets_parsed} sheets with {total_records:,} total records"

        return create_response(
            "SUCCESS",
            success_message,
            file_path,
            json_path,
            data,
            metadata,
        )

    except Exception as e:
        return create_response("ERROR", f"Unexpected error: {str(e)}", file_path)


def process_battery_pack_test(
    file_path: str, battery_pack_id: str, db_manager: DatabaseManager
) -> Dict[str, Any]:
    """
    Process battery pack test data with database integration and performance optimization.

    Args:
        file_path: Path to the Excel file.
        battery_pack_id: Battery pack identifier.
        db_manager: Database manager instance.

    Returns:
        Dict: Processing result.
    """
    try:
        start_time = time.time()

        # Validate and sanitize inputs
        if not InputValidator.validate_battery_pack_id(battery_pack_id):
            return create_response("ERROR", "Invalid battery pack ID format", file_path)

        battery_pack_id = InputValidator.sanitize_input(battery_pack_id)

        # Verify battery pack exists in database
        battery_pack, error = db_manager.verify_battery_pack_exists(battery_pack_id)
        if error:
            return create_response("ERROR", error, file_path)

        # Parse the file (skip JSON generation for faster processing)
        parse_start = time.time()
        parse_result = parse_file_safely(file_path)
        parse_time = time.time() - parse_start

        if parse_result["status"] == "ERROR":
            return create_response("ERROR", parse_result["message"], file_path)

        # Database operations with timing
        db_start = time.time()

        # Insert test data into database
        test_id, error = db_manager.insert_test_data(
            battery_pack["id"], parse_result["data"]
        )
        if error:
            return create_response("ERROR", error, file_path)

        # Insert metadata tables (smaller datasets, can be done sequentially)
        unit_id, error = db_manager.insert_unit_data(test_id, parse_result["data"])
        if error:
            return create_response("ERROR", error, file_path)

        cycle_id, error = db_manager.insert_cycle_data(test_id, parse_result["data"])
        if error:
            return create_response("ERROR", error, file_path)

        step_id, error = db_manager.insert_steps_data(test_id, parse_result["data"])
        if error:
            return create_response("ERROR", error, file_path)

        # Process large datasets with bulk operations
        bulk_start = time.time()

        # Insert records data (largest dataset - use optimized bulk insert)
        record_id, error = db_manager.insert_records_data(test_id, parse_result["data"])
        if error:
            return create_response("ERROR", error, file_path)

        # Insert logs data
        log_id, error = db_manager.insert_logs_data(test_id, parse_result["data"])
        if error:
            return create_response("ERROR", error, file_path)

        # Insert aux_dbc data (second largest dataset - use optimized bulk insert)
        aux_id, error = db_manager.insert_aux_dbc_data(test_id, parse_result["data"])
        if error:
            return create_response("ERROR", error, file_path)

        bulk_time = time.time() - bulk_start
        db_time = time.time() - db_start
        total_time = time.time() - start_time

        # Calculate data volumes for performance reporting
        data = parse_result["data"]
        record_count = len(data.get("record", []))
        aux_count = len(data.get("aux_dbc", []))
        total_inserts = record_count + aux_count

        # Enhanced success message with performance metrics
        success_message = (
            f"{parse_result.get('message', 'Processing completed')} | "
            f"Performance: Total={total_time:.2f}s, Parse={parse_time:.2f}s, "
            f"DB={db_time:.2f}s, Bulk={bulk_time:.2f}s | "
            f"Inserted: {total_inserts:,} records ({record_count:,} records + {aux_count:,} aux_dbc)"
        )

        return create_response(
            "SUCCESS",
            success_message,
            file_path,
            parse_result.get("json_path"),
            {
                "test_id": test_id,
                "performance": {
                    "total_time_seconds": round(total_time, 3),
                    "parse_time_seconds": round(parse_time, 3),
                    "database_time_seconds": round(db_time, 3),
                    "bulk_insert_time_seconds": round(bulk_time, 3),
                    "records_per_second": (
                        round(total_inserts / total_time, 0) if total_time > 0 else 0
                    ),
                },
                "data_summary": {
                    "record_count": record_count,
                    "aux_dbc_count": aux_count,
                    "total_inserts": total_inserts,
                },
            },
        )

    except Exception as e:
        return create_response("ERROR", f"Processing failed: {str(e)}", file_path)


def main():
    """
    Main function with comprehensive error handling and input validation.
    """
    try:
        # Validate command line arguments
        if len(sys.argv) != 3:
            response = create_response(
                "ERROR",
                "Usage: python main_db.py <file_path> <battery_pack_id>",
                None,
            )
            print(json.dumps(response, indent=0, ensure_ascii=False))
            sys.exit(0)

        file_path = InputValidator.sanitize_input(sys.argv[1])
        battery_pack_id = InputValidator.sanitize_input(sys.argv[2])

        # Perform database health check
        db_manager = get_database_manager()
        health_status = db_manager.health_check()

        if health_status["status"] != "healthy":
            response = create_response(
                "ERROR",
                f"Database health check failed: {health_status.get('error', 'Unknown error')}",
                file_path,
            )
            print(json.dumps(response, indent=0, ensure_ascii=False))
            sys.exit(0)

        # Process the battery pack test
        result = process_battery_pack_test(file_path, battery_pack_id, db_manager)
        print(json.dumps(result, indent=0, ensure_ascii=False))
        # Exit with appropriate code
        sys.exit(0 if result["status"] == "SUCCESS" else 1)

    except KeyboardInterrupt:
        response = create_response("ERROR", "Process interrupted by user", None)
        print(json.dumps(response, indent=0, ensure_ascii=False))
        sys.exit(130)

    except Exception as e:
        response = create_response("ERROR", f"System error: {str(e)}", None)
        print(json.dumps(response, indent=0, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
