#!/usr/bin/env python3
"""
Main entry point for the parser improvement package with advanced database integration.
Simple interface with standardized JSON response format for JavaScript integration.
"""

import sys
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
import re

# Add parent directory to path for package imports
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from parser_improve import ImprovedExcelParser
from parser_improve.utils import JSONFileUtils
from parser_improve.database import (
    DatabaseManager,
    get_database_manager,
    DatabaseError,
    BatteryPackNotFoundError,
    TransactionError,
)
from parser_improve.config import db_config

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
        "data": data or {},
        "metadata": metadata or {},
        "timestamp": Path(__file__).stat().st_mtime if file_path else None,
    }


def parse_file_safely(file_path: str) -> Dict[str, Any]:
    """
    Parse Excel file with comprehensive error handling.

    Args:
        file_path: Path to the Excel file to parse.

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

        logger.info(f"Starting to parse file: {file_path}")

        # Initialize parser with default settings
        parser = ImprovedExcelParser()

        # Parse the file
        result = parser.parse_file(file_path)

        # Check if parsing failed
        if "error" in result:
            logger.error(f"Parsing failed for {file_path}: {result['error']}")
            return create_response(
                "ERROR", f"Parsing failed: {result['error']}", file_path
            )

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

        logger.info(f"File parsing completed successfully: {success_message}")
        return create_response("SUCCESS", success_message, file_path, data, metadata)

    except Exception as e:
        logger.error(f"Unexpected error during file parsing: {e}", exc_info=True)
        return create_response("ERROR", f"Unexpected error: {str(e)}", file_path)


def process_battery_pack_test(
    file_path: str, battery_pack_id: str, db_manager: DatabaseManager
) -> Dict[str, Any]:
    """
    Process battery pack test data with database integration.

    Args:
        file_path: Path to the Excel file.
        battery_pack_id: Battery pack identifier.
        db_manager: Database manager instance.

    Returns:
        Dict: Processing result.
    """
    try:
        # Validate and sanitize inputs
        if not InputValidator.validate_battery_pack_id(battery_pack_id):
            return create_response("ERROR", "Invalid battery pack ID format", file_path)

        battery_pack_id = InputValidator.sanitize_input(battery_pack_id)
        logger.info(f"Processing battery pack test: {battery_pack_id}")

        # Verify battery pack exists in database
        battery_pack, error = db_manager.verify_battery_pack_exists(battery_pack_id)
        if error:
            logger.error(f"Battery pack verification failed: {error}")
            return create_response("ERROR", error, file_path)

        # Parse the file
        parse_result = parse_file_safely(file_path)
        if parse_result["status"] == "ERROR":
            logger.error(f"File parsing failed: {parse_result['message']}")
            return create_response("ERROR", parse_result["message"], file_path)

        # Insert test data into database
        test_id, error = db_manager.insert_test_data(
            battery_pack["id"], parse_result["data"]
        )
        if error:
            logger.error(f"Database error during test data insertion: {error}")
            return create_response("ERROR", error, file_path)

        logger.info(f"Test data inserted successfully with ID: {test_id}")

        unit_id, error = db_manager.insert_unit_data(test_id, parse_result["data"])
        if error:
            logger.error(f"Database error during unit data insertion: {error}")
            return create_response("ERROR", error, file_path)

        logger.info(f"Unit data inserted successfully with ID: {unit_id}")

        cycle_id, error = db_manager.insert_cycle_data(test_id, parse_result["data"])
        if error:
            logger.error(f"Database error during cycle data insertion: {error}")
            return create_response("ERROR", error, file_path)

        logger.info(f"Cycle data inserted successfully with ID: {cycle_id}")

        return create_response(
            "SUCCESS",
            parse_result["message"],
            file_path,
        )

    except Exception as e:
        logger.error(
            f"Unexpected error in process_battery_pack_test: {e}", exc_info=True
        )
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

        logger.info(
            f"Starting main process with file: {file_path}, battery_pack_id: {battery_pack_id}"
        )

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
            sys.exit(1)

        logger.info(
            f"Database health check passed in {health_status['response_time_ms']}ms"
        )

        # Process the battery pack test
        result = process_battery_pack_test(file_path, battery_pack_id, db_manager)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        # Exit with appropriate code
        sys.exit(0 if result["status"] == "SUCCESS" else 1)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        response = create_response("ERROR", "Process interrupted by user", None)
        print(json.dumps(response, indent=0, ensure_ascii=False))
        sys.exit(130)

    except Exception as e:
        logger.error(f"Unexpected error in main function: {e}", exc_info=True)
        response = create_response("ERROR", f"System error: {str(e)}", None)
        print(json.dumps(response, indent=0, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
