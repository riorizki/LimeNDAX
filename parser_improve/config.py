"""
Configuration settings for Excel parsing operations.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
import os


@dataclass
class DatabaseConfig:
    """Configuration for database connection."""

    host: str = "localhost"
    user: str = "root"
    password: str = ""
    database: str = "battery_line_pack"
    port: int = 3306
    charset: str = "utf8mb4"

    # Connection pool settings
    pool_name: str = "battery_pool"
    pool_size: int = 5
    pool_reset_session: bool = True

    # Connection timeout settings
    connection_timeout: int = 10
    autocommit: bool = False

    # SSL settings
    use_ssl: bool = False
    ssl_disabled: bool = True

    @classmethod
    def from_environment(cls) -> "DatabaseConfig":
        """Create database configuration from environment variables."""
        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", "battery_line_pack"),
            port=int(os.getenv("DB_PORT", "3306")),
            charset=os.getenv("DB_CHARSET", "utf8mb4"),
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            connection_timeout=int(os.getenv("DB_TIMEOUT", "10")),
            autocommit=os.getenv("DB_AUTOCOMMIT", "false").lower() == "true",
            use_ssl=os.getenv("DB_USE_SSL", "false").lower() == "true",
        )

    def to_connection_dict(self) -> Dict:
        """Convert to dictionary format for mysql.connector.connect()."""
        config = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "port": self.port,
            "charset": self.charset,
            "connection_timeout": self.connection_timeout,
            "autocommit": self.autocommit,
            "ssl_disabled": self.ssl_disabled,
        }

        if self.use_ssl:
            config["ssl_disabled"] = False

        return config

    def to_pool_dict(self) -> Dict:
        """Convert to dictionary format for connection pooling."""
        config = self.to_connection_dict()
        config.update(
            {
                "pool_name": self.pool_name,
                "pool_size": self.pool_size,
                "pool_reset_session": self.pool_reset_session,
            }
        )
        return config


@dataclass
class SheetParsingConfig:
    """Configuration for individual sheet parsing."""

    header_row: int = 0  # Which row contains headers (0-based)
    drop_no_column: bool = True  # Whether to drop 'No.' columns
    max_records: Optional[int] = None  # Maximum records to return
    required_columns: Optional[List[str]] = None  # Required columns (normalized names)
    return_single_record: bool = False  # Return only first record for some sheets


class ParserConfig:
    """Global configuration for parser behavior."""

    # Default file path for development
    DEFAULT_FILE_PATH = "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/data/LG_2_EOL_test_15-1-4-20250428125222.xlsx"

    # Output configuration
    DEFAULT_OUTPUT_DIR = "result"
    DEFAULT_OUTPUT_FILE = "parsed_output.json"

    # Logging configuration
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Sheet-specific configurations
    SHEET_CONFIGS: Dict[str, SheetParsingConfig] = {
        "auxDBC": SheetParsingConfig(
            header_row=1, drop_no_column=True, return_single_record=True
        ),
        "cycle": SheetParsingConfig(
            header_row=0, drop_no_column=False, return_single_record=True
        ),
        "idle": SheetParsingConfig(
            header_row=0, drop_no_column=True, return_single_record=True
        ),
        "log": SheetParsingConfig(
            header_row=0, drop_no_column=True, return_single_record=True
        ),
        "record": SheetParsingConfig(
            header_row=0, drop_no_column=False, return_single_record=True
        ),
        "step": SheetParsingConfig(
            header_row=0, drop_no_column=False, return_single_record=True
        ),
    }

    # Result key mappings for consistency
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

    # Ordered list of sheets to parse (controls output order)
    SHEET_PARSE_ORDER = [
        "unit",
        "test",
        "cycle",
        "step",
        "record",
        "log",
        "idle",
        "auxDBC",
    ]

    # Known sheet types that return dictionaries instead of lists
    DICT_RETURN_SHEETS = {"test", "unit"}

    # Test sheet field mappings
    TEST_FIELD_MAP = {
        "start step id": "start_step_id",
        "cycle count": "cycle_count",
        "record settings": "record_settings",
        "voltage range": "voltage_range",
        "current range": "current_range",
        "active material": "active_material",
        "volt. upper": "volt_upper",
        "volt upper": "volt_upper",
        "volt. lower": "volt_lower",
        "volt lower": "volt_lower",
        "curr. upper": "curr_upper",
        "curr upper": "curr_upper",
        "curr. lower": "curr_lower",
        "curr lower": "curr_lower",
        "start time": "start_time",
        "nominal capacity": "nominal_capacity",
        "p/n": "p_n",
        "builder": "builder",
        "remarks": "remarks",
        "barcode": "barcode",
    }


# Global configuration instance
config = ParserConfig()

# Database configuration instance
db_config = DatabaseConfig.from_environment()
