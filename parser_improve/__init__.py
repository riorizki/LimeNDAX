"""
Parser Improvement Package

This package contains the improved Excel parser architecture with:
- Centralized utilities for common operations
- Base parser classes for extensibility
- Configuration management
- Enhanced error handling and logging
- Type safety and validation
- High-performance parsing for large files
- Intelligent caching and streaming

Usage:
    # Standard parser
    from parser_improve.improved_parser import ImprovedExcelParser
    parser = ImprovedExcelParser()
    result = parser.parse_file("file.xlsx")

    # High-performance parser for large files
    from parser_improve.high_performance_parser import HighPerformanceParser
    hp_parser = HighPerformanceParser(use_cache=True, chunk_size=5000)
    result = hp_parser.parse_file("large_file.xlsx")
"""

from .base_parsers import (
    BaseSheetParser,
    StandardSheetParser,
    TestSheetParser,
    UnitSheetParser,
)
from .config import ParserConfig

# Import main classes for easy access
from .improved_parser import ImprovedExcelParser
from .utils import DataFrameProcessor, ExcelLoader, HeaderNormalizer, ValidationUtils

__all__ = [
    "ImprovedExcelParser",
    "HeaderNormalizer",
    "DataFrameProcessor",
    "ExcelLoader",
    "ValidationUtils",
    "BaseSheetParser",
    "StandardSheetParser",
    "TestSheetParser",
    "UnitSheetParser",
    "ParserConfig",
]
