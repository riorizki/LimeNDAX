"""
Improved Excel parser with proper architecture and configuration.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from .utils import ExcelLoader, ValidationUtils
from .base_parsers import (
    StandardSheetParser,
    TestSheetParser,
    UnitSheetParser,
    SheetConfig,
)

logger = logging.getLogger(__name__)


class ExcelParserConfig:
    """Configuration for Excel parsing operations."""

    # Sheet configurations
    SHEET_CONFIGS = {
        "auxDBC": SheetConfig(header_row=1, drop_no_column=True, max_records=1),
        "cycle": SheetConfig(header_row=0, drop_no_column=False, max_records=1),
        "idle": SheetConfig(header_row=0, drop_no_column=True, max_records=1),
        "log": SheetConfig(header_row=0, drop_no_column=True, max_records=1),
        "record": SheetConfig(header_row=0, drop_no_column=False, max_records=1),
        "step": SheetConfig(header_row=0, drop_no_column=False, max_records=1),
        "others": SheetConfig(header_row=1, drop_no_column=True),
    }

    # Result key mappings
    RESULT_KEY_MAP = {
        "auxDBC": "aux_dbc",
        "cycle": "cycle",
        "idle": "idle",
        "log": "log",
        "record": "record",
        "step": "step",
        "test": "test",
        "unit": "unit",
    }


class ImprovedExcelParser:
    """Main Excel parser with improved architecture."""

    def __init__(self, config: Optional[ExcelParserConfig] = None):
        self.config = config or ExcelParserConfig()
        self.loader = ExcelLoader()

        # Initialize parsers
        self.parsers = {
            "test": TestSheetParser(),
            "unit": UnitSheetParser(),
        }

        # Add standard parsers for other sheets
        for sheet_name, sheet_config in self.config.SHEET_CONFIGS.items():
            self.parsers[sheet_name] = StandardSheetParser(sheet_config)

    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """
        Parse entire Excel file.

        Args:
            file_path: Path to Excel file

        Returns:
            Parsed data structure
        """
        if not ValidationUtils.validate_file_path(file_path):
            logger.error(f"File not found or not accessible: {file_path}")
            return {"error": "File not found", "file_path": file_path}

        try:
            # Load all sheets
            sheets = self.loader.load_workbook(file_path)
            if not sheets:
                return {"error": "Failed to load workbook", "file_path": file_path}

            # Parse each sheet
            parsed_data = {}
            for sheet_name, parser in self.parsers.items():
                result_key = self.config.RESULT_KEY_MAP.get(sheet_name, sheet_name)

                try:
                    df = sheets.get(sheet_name)
                    if df is not None:
                        parsed_data[result_key] = parser.parse(df)
                    else:
                        # Initialize with appropriate empty structure
                        if isinstance(parser, (TestSheetParser, UnitSheetParser)):
                            parsed_data[result_key] = {}
                        else:
                            parsed_data[result_key] = []
                        logger.warning(f"Sheet '{sheet_name}' not found in workbook")

                except Exception as e:
                    logger.error(f"Error parsing sheet '{sheet_name}': {e}")
                    # Initialize with appropriate empty structure
                    if isinstance(parser, (TestSheetParser, UnitSheetParser)):
                        parsed_data[result_key] = {}
                    else:
                        parsed_data[result_key] = []

            return {
                "file_path": file_path,
                "data": parsed_data,
                "metadata": {
                    "sheets_found": list(sheets.keys()),
                    "sheets_parsed": list(parsed_data.keys()),
                },
            }

        except Exception as e:
            logger.error(f"Unexpected error parsing file '{file_path}': {e}")
            return {"error": str(e), "file_path": file_path}

    def parse_sheet(self, file_path: str, sheet_name: str) -> Any:
        """
        Parse specific sheet from Excel file.

        Args:
            file_path: Path to Excel file
            sheet_name: Name of sheet to parse

        Returns:
            Parsed sheet data
        """
        if not ValidationUtils.validate_file_path(file_path):
            logger.error(f"File not found: {file_path}")
            return None

        try:
            sheets = self.loader.load_workbook(file_path)
            if not sheets:
                return None

            # Normalize sheet name for lookup
            sheet_key = sheet_name.lower().replace(" ", "_")

            # Find parser
            parser = self.parsers.get(sheet_key)
            if not parser:
                # Use standard parser with others config
                parser = StandardSheetParser(self.config.SHEET_CONFIGS.get("others"))

            # Get DataFrame
            df = sheets.get(sheet_name) or sheets.get(sheet_key)
            if df is None:
                logger.warning(f"Sheet '{sheet_name}' not found")
                return (
                    {} if isinstance(parser, (TestSheetParser, UnitSheetParser)) else []
                )

            return parser.parse(df)

        except Exception as e:
            logger.error(f"Error parsing sheet '{sheet_name}' from '{file_path}': {e}")
            return None


def main():
    """Main execution function with improved error handling."""
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        description="Parse Excel files with improved parser"
    )
    parser.add_argument("file_path", help="Path to Excel file")
    parser.add_argument("--sheet", help="Specific sheet to parse (optional)")
    parser.add_argument(
        "--output", help="Output file path (default: result/parsed_output.json)"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    # Configure logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Initialize parser
    excel_parser = ImprovedExcelParser()

    try:
        if args.sheet:
            # Parse specific sheet
            result = excel_parser.parse_sheet(args.file_path, args.sheet)
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            # Parse entire file
            result = excel_parser.parse_file(args.file_path)

            # Save to file
            output_path = args.output or "result/parsed_output.json"
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)

            print(f"Parsed data written to {output_file}")

            # Print summary
            if "data" in result:
                print(f"\nParsing Summary:")
                print(f"File: {result['file_path']}")
                if "metadata" in result:
                    print(f"Sheets found: {result['metadata']['sheets_found']}")
                    print(f"Sheets parsed: {result['metadata']['sheets_parsed']}")

                for key, value in result["data"].items():
                    if isinstance(value, list):
                        print(f"  {key}: {len(value)} records")
                    elif isinstance(value, dict):
                        print(f"  {key}: dictionary with {len(value)} keys")

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
