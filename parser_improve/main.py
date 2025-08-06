#!/usr/bin/env python3
"""
Main entry point for the parser improvement package.
Simple interface with standardized JSON response format for JavaScript integration.
"""

import sys
import json
from pathlib import Path

# Add parent directory to path for package imports
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from parser_improve import ImprovedExcelParser
from parser_improve.utils import JSONFileUtils


def create_response(status, message, file_path=None, data=None, metadata=None):
    """
    Create standardized response format for JavaScript consumption.

    Args:
        status (str): Either "SUCCESS" or "ERROR"
        message (str): Descriptive message
        file_path (str, optional): Path to the file
        data (dict, optional): Parsed data
        metadata (dict, optional): Metadata information

    Returns:
        dict: Standardized response format
    """
    return {
        "status": status,
        "message": message,
        "file_path": file_path,
        "data": data or {},
        "metadata": metadata or {},
    }


def main(file_path):
    """
    Main function to parse Excel file and return standardized response.

    Args:
        file_path (str): Path to the Excel file to parse

    Returns:
        dict: Standardized response with status, message, data, and metadata
    """
    try:
        # Validate file path
        if not file_path:
            return create_response("ERROR", "File path is required", None)

        # Check if file exists
        if not Path(file_path).exists():
            return create_response("ERROR", f"File not found: {file_path}", file_path)

        # Initialize parser with default settings
        parser = ImprovedExcelParser()

        # Parse the file
        result = parser.parse_file(file_path)

        # Check if parsing failed
        if "error" in result:
            return create_response(
                "ERROR", f"Parsing failed: {result['error']}", file_path
            )

        # Generate output filename based on input filename
        output_path = JSONFileUtils.generate_output_filename(file_path)

        # Write result to JSON file
        json_success = JSONFileUtils.write_json_result(result, output_path)

        # Extract data and metadata from result
        data = result.get("data", {})
        metadata = result.get("metadata", {})

        # Add JSON output info to metadata
        if json_success:
            metadata["json_output"] = {"file_path": output_path, "status": "saved"}
        else:
            metadata["json_output"] = {
                "file_path": output_path,
                "status": "failed_to_save",
            }

        # Calculate summary statistics for message
        sheets_parsed = len(metadata.get("sheets_parsed", []))
        total_records = sum(
            len(sheet_data)
            for sheet_data in data.values()
            if isinstance(sheet_data, list)
        )

        success_message = f"Successfully parsed {sheets_parsed} sheets with {total_records:,} total records"
        if json_success:
            success_message += f". JSON saved to {output_path}"

        return create_response("SUCCESS", success_message, file_path, data, metadata)

    except Exception as e:
        return create_response("ERROR", f"Unexpected error: {str(e)}", file_path)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        response = create_response("ERROR", "Usage: python main.py <file_path>", None)
        print(json.dumps(response, indent=2, ensure_ascii=False))
        sys.exit(1)

    file_path = sys.argv[1]
    result = main(file_path)

    # Save standardized response to JSON file for review
    input_filename = Path(file_path).stem
    response_filename = f"{input_filename}_response.json"
    response_output_path = Path("result") / response_filename

    # Ensure result directory exists
    response_output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write response to JSON file
    with open(response_output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"Response saved to: {response_output_path}")
    sys.exit(0)

    # Also output JSON response to console for immediate review
    # print(json.dumps(result, indent=2, ensure_ascii=False))

    # Exit with error code if failed
    # if result["status"] == "ERROR":
    # sys.exit(1)
