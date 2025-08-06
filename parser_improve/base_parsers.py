"""
Base parser classes and configuration for Excel sheet parsing.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any, Union
import pandas as pd
import logging
import warnings
from .utils import DataFrameProcessor, HeaderNormalizer, ValidationUtils

# Suppress openpyxl warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

logger = logging.getLogger(__name__)


class SheetConfig:
    """Configuration for sheet parsing behavior."""

    def __init__(
        self,
        header_row: int = 0,
        drop_no_column: bool = True,
        return_type: str = "list",  # "list" or "dict"
        max_records: Optional[int] = None,
        required_columns: Optional[List[str]] = None,
    ):
        self.header_row = header_row
        self.drop_no_column = drop_no_column
        self.return_type = return_type
        self.max_records = max_records
        self.required_columns = required_columns or []


class BaseSheetParser(ABC):
    """Base class for all sheet parsers."""

    def __init__(self, config: Optional[SheetConfig] = None):
        self.config = config or SheetConfig()
        self.processor = DataFrameProcessor()

    @abstractmethod
    def parse(
        self, df: Optional[pd.DataFrame]
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Parse the DataFrame and return structured data."""
        pass

    def validate_input(self, df: Optional[pd.DataFrame]) -> bool:
        """Validate input DataFrame."""
        if df is None or df.empty:
            return False

        # Check required columns if specified
        if self.config.required_columns:
            available_cols = [
                HeaderNormalizer.normalize_header(col) for col in df.columns
            ]
            for req_col in self.config.required_columns:
                if req_col not in available_cols:
                    logger.warning(f"Required column '{req_col}' not found")
                    return False

        return True

    def preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimized preprocessing for DataFrames."""
        # Extract headers if needed with optimizations
        if self.config.header_row > 0:
            df = self.processor.extract_header_from_row(df, self.config.header_row)
        elif self.config.header_row == 0:
            # Faster header extraction
            df.columns = df.iloc[0].values
            df = df.iloc[1:].reset_index(drop=True)

        # Clean the DataFrame with optimizations
        df = self.processor.clean_dataframe(
            df, drop_no_column=self.config.drop_no_column
        )

        return df

    def postprocess_data(
        self, data: List[Dict[str, Any]]
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Optimized post-processing of parsed data."""
        # Apply record limit before sanitization for better performance
        if self.config.max_records:
            data = data[: self.config.max_records]

        # Use fast sanitization for large datasets
        if len(data) > 1000:
            data = ValidationUtils.fast_sanitize_records(data)
        else:
            data = ValidationUtils.sanitize_data_for_json(data)

        # Return appropriate format
        if self.config.return_type == "dict" and data:
            return data[0] if len(data) == 1 else {"records": data}

        return data


class StandardSheetParser(BaseSheetParser):
    """Optimized standard parser for most sheet types."""

    def parse(self, df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
        """Parse standard sheet format with optimizations."""
        if not self.validate_input(df):
            return []

        try:
            df = self.preprocess_dataframe(df)

            if df.empty:
                return []

            # Optimized conversion to records for large DataFrames
            if len(df) > 10000:
                # For large DataFrames, process in chunks to avoid memory issues
                chunk_size = 5000
                all_records = []

                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i : i + chunk_size]
                    chunk_records = chunk.to_dict(orient="records")
                    all_records.extend(chunk_records)

                data = all_records
            else:
                # For smaller DataFrames, use standard conversion
                data = df.to_dict(orient="records")

            return self.postprocess_data(data)

        except Exception as e:
            logger.error(f"Error parsing standard sheet: {e}")
            return []


class TestSheetParser(BaseSheetParser):
    """Specialized parser for test sheet with complex structure."""

    def __init__(self):
        super().__init__(SheetConfig(return_type="dict"))
        self.field_map = {
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

    def parse(self, df: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Parse test sheet with specific structure."""
        if not self.validate_input(df):
            return {}

        try:
            test_info = self._extract_test_information(df)
            step_plan = self._extract_step_plan(df)

            return {"test_information": test_info, "step_plan": step_plan}

        except Exception as e:
            logger.error(f"Error parsing test sheet: {e}")
            return {}

    def _extract_test_information(self, df: pd.DataFrame) -> Dict[str, str]:
        """Extract test information from specific cells."""
        test_info = {v: "" for v in self.field_map.values()}

        # Search for test information in rows 1-6
        for i in range(1, min(7, df.shape[0])):
            for j in range(0, df.shape[1], 3):
                if j >= df.shape[1]:
                    continue

                field_cell = (
                    str(df.iloc[i, j]).strip().lower()
                    if pd.notnull(df.iloc[i, j])
                    else ""
                )

                value_col = j + 2
                if value_col < df.shape[1]:
                    value_cell = (
                        str(df.iloc[i, value_col]).strip()
                        if pd.notnull(df.iloc[i, value_col])
                        else ""
                    )

                    if field_cell in self.field_map:
                        key = self.field_map[field_cell]
                        test_info[key] = value_cell

        return test_info

    def _extract_step_plan(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Extract step plan from DataFrame."""
        # Find step plan header
        step_plan_header_idx = None
        for idx in range(df.shape[0]):
            if (
                pd.notnull(df.iloc[idx, 0])
                and str(df.iloc[idx, 0]).lower().strip() == "step index"
            ):
                step_plan_header_idx = idx
                break

        if step_plan_header_idx is None:
            return []

        # Extract step plan data
        step_headers = df.iloc[step_plan_header_idx, :].tolist()
        step_headers_norm = [HeaderNormalizer.normalize_header(h) for h in step_headers]

        step_plan = []
        for i in range(step_plan_header_idx + 1, df.shape[0]):
            row = df.iloc[i, :].tolist()

            # Skip empty rows
            if (
                pd.isnull(row[0])
                or str(row[0]).strip() == ""
                or str(row[0]).lower().startswith("nan")
            ):
                continue

            step_dict = {}
            for k, v in zip(step_headers_norm, row):
                if k and k != "nan":
                    step_dict[k] = v if pd.notnull(v) else ""

            step_plan.append(step_dict)

        return step_plan


class UnitSheetParser(BaseSheetParser):
    """Specialized parser for unit sheet."""

    def __init__(self):
        super().__init__(SheetConfig(return_type="dict"))

    def parse(self, df: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Parse unit sheet with specific structure."""
        if not self.validate_input(df):
            return {}

        try:
            # Extract device information
            device_parts = []
            if df.shape[0] > 1:
                for i in range(1, min(4, df.shape[1])):
                    if i < df.shape[1] and pd.notnull(df.iloc[1, i]):
                        device_parts.append(str(int(df.iloc[1, i])))

            device = " ".join(device_parts) if device_parts else ""

            # Extract timestamps
            start_time = (
                str(df.iloc[2, 2])
                if (df.shape[0] > 2 and df.shape[1] > 2 and pd.notnull(df.iloc[2, 2]))
                else ""
            )

            end_time = (
                str(df.iloc[2, 6])
                if (df.shape[0] > 2 and df.shape[1] > 6 and pd.notnull(df.iloc[2, 6]))
                else ""
            )

            # Extract unit plans
            list_of_unit_plans = {}
            if df.shape[0] > 6:
                headers = df.iloc[5, :].tolist() if df.shape[0] > 5 else []
                units = df.iloc[6, :].tolist() if df.shape[0] > 6 else []

                for h, u in zip(headers, units):
                    if pd.notnull(h) and pd.notnull(u):
                        key = str(h).strip().lower().replace(" ", "_")
                        value = str(u).strip()
                        list_of_unit_plans[key] = value

            result = {
                "device": device,
                "start_time": start_time,
                "end_time": end_time,
                "list_of_unit_plans": list_of_unit_plans,
            }

            # Return empty dict if all values are empty
            if not any([device, start_time, end_time, list_of_unit_plans]):
                return {}

            return result

        except Exception as e:
            logger.error(f"Error parsing unit sheet: {e}")
            return {}
