"""
Advanced database management utilities with connection pooling, error handling, and transactions.
"""

import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Tuple, Union
from datetime import datetime
import uuid

import mysql.connector
from mysql.connector import pooling, Error as MySQLError
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor

from .config import DatabaseConfig, db_config


class DatabaseError(Exception):
    """Custom database error class."""

    pass


class ConnectionPoolError(DatabaseError):
    """Connection pool related errors."""

    pass


class TransactionError(DatabaseError):
    """Transaction related errors."""

    pass


class BatteryPackNotFoundError(DatabaseError):
    """Battery pack not found in database."""

    pass


class DatabaseManager:
    """
    Advanced database manager with connection pooling, error handling, and transaction management.
    """

    def __init__(self, config: DatabaseConfig = None):
        """
        Initialize database manager.

        Args:
            config: Database configuration. Uses default if None.
        """
        self.config = config or db_config
        self._pool: Optional[pooling.MySQLConnectionPool] = None
        self._setup_connection_pool()

    def _setup_connection_pool(self) -> None:
        """Setup MySQL connection pool."""
        try:
            pool_config = self.config.to_pool_dict()
            self._pool = pooling.MySQLConnectionPool(**pool_config)
        except MySQLError as e:
            error_msg = f"Failed to create connection pool: {e}"
            raise ConnectionPoolError(error_msg) from e

    def get_connection(self) -> MySQLConnection:
        """
        Get connection from pool with retry logic.

        Returns:
            MySQLConnection: Active database connection.

        Raises:
            ConnectionPoolError: If unable to get connection after retries.
        """
        max_retries = 3
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
                if not self._pool:
                    self._setup_connection_pool()

                connection = self._pool.get_connection()

                # Verify connection is alive
                if not connection.is_connected():
                    connection.reconnect()

                return connection

            except MySQLError as e:
                if attempt == max_retries - 1:
                    error_msg = (
                        f"Failed to get connection after {max_retries} attempts: {e}"
                    )
                    raise ConnectionPoolError(error_msg) from e

                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

    @contextmanager
    def get_connection_context(self) -> Generator[MySQLConnection, None, None]:
        """
        Context manager for database connections with automatic cleanup.

        Yields:
            MySQLConnection: Active database connection.
        """
        connection = None
        try:
            connection = self.get_connection()
            yield connection
        except Exception as e:
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()

    @contextmanager
    def get_cursor_context(
        self, connection: MySQLConnection = None, dictionary: bool = False
    ) -> Generator[MySQLCursor, None, None]:
        """
        Context manager for database cursors with automatic cleanup.

        Args:
            connection: Existing connection. If None, gets new connection.
            dictionary: Whether to return results as dictionaries.

        Yields:
            MySQLCursor: Active database cursor.
        """
        cursor = None
        connection_created = False

        try:
            if connection is None:
                connection = self.get_connection()
                connection_created = True

            cursor = connection.cursor(dictionary=dictionary, buffered=True)
            yield cursor

        except Exception as e:
            raise
        finally:
            if cursor:
                cursor.close()

            if connection_created and connection and connection.is_connected():
                connection.close()

    @contextmanager
    def transaction_context(
        self, connection: MySQLConnection = None
    ) -> Generator[MySQLConnection, None, None]:
        """
        Context manager for database transactions with automatic rollback on error.

        Args:
            connection: Existing connection. If None, gets new connection.

        Yields:
            MySQLConnection: Active database connection in transaction.
        """
        connection_created = False
        transaction_started = False

        try:
            if connection is None:
                connection = self.get_connection()
                connection_created = True

            # Start transaction
            connection.start_transaction()
            transaction_started = True

            yield connection

            # Commit transaction
            connection.commit()

        except Exception as e:
            if transaction_started and connection and connection.is_connected():
                try:
                    connection.rollback()
                except MySQLError as rollback_error:
                    pass
            raise TransactionError(f"Transaction failed: {e}") from e
        finally:
            if connection_created and connection and connection.is_connected():
                connection.close()

    def execute_query(
        self,
        query: str,
        params: Tuple = None,
        fetch_one: bool = False,
        fetch_all: bool = True,
        dictionary: bool = False,
    ) -> Optional[Union[List, Tuple, Dict]]:
        """
        Execute a query with proper error handling.

        Args:
            query: SQL query to execute.
            params: Query parameters.
            fetch_one: Whether to fetch only one result.
            fetch_all: Whether to fetch all results.
            dictionary: Whether to return results as dictionaries.

        Returns:
            Query results or None.
        """
        try:
            with self.get_connection_context() as connection:
                with self.get_cursor_context(
                    connection, dictionary=dictionary
                ) as cursor:
                    cursor.execute(query, params or ())

                    if fetch_one:
                        result = cursor.fetchone()
                        return result
                    elif fetch_all:
                        result = cursor.fetchall()
                        return result
                    else:
                        return None

        except MySQLError as e:
            raise DatabaseError(f"Query execution failed: {e}") from e

    def execute_transaction(
        self, queries: List[Tuple[str, Tuple]], return_results: bool = False
    ) -> Optional[List]:
        """
        Execute multiple queries in a single transaction.

        Args:
            queries: List of (query, params) tuples.
            return_results: Whether to return results from queries.

        Returns:
            List of results if return_results=True, None otherwise.
        """
        results = []

        try:
            with self.transaction_context() as connection:
                with self.get_cursor_context(connection) as cursor:
                    for query, params in queries:
                        cursor.execute(query, params or ())

                        if return_results:
                            try:
                                result = cursor.fetchall()
                                results.append(result)
                            except MySQLError:
                                # Some queries don't return results (INSERT, UPDATE, DELETE)
                                results.append(None)

            return results if return_results else None

        except Exception as e:
            raise TransactionError(f"Transaction execution failed: {e}") from e

    def execute_bulk_insert(self, query, data, batch_size=1000):
        """Execute bulk insert with chunking to prevent MySQL timeout/memory issues."""
        import time
        from mysql.connector import errors as mysql_errors

        if not data:
            return

        total_batches = (len(data) + batch_size - 1) // batch_size

        connection = None
        cursor = None

        try:
            # Get connection from pool
            connection = self.get_connection()
            cursor = connection.cursor()

            # Optimize MySQL session for bulk operations (MUST be done before transaction)
            original_autocommit = connection.autocommit
            connection.autocommit = False

            # Store original values to restore later
            cursor.execute("SELECT @@unique_checks")
            original_unique_checks = cursor.fetchone()[0]

            cursor.execute("SELECT @@foreign_key_checks")
            original_foreign_key_checks = cursor.fetchone()[0]

            # Set optimization values (outside transaction)
            cursor.execute("SET unique_checks = 0")
            cursor.execute("SET foreign_key_checks = 0")

            # Try to set sql_log_bin only if not in transaction and we have permission
            try:
                cursor.execute("SELECT @@sql_log_bin")
                original_sql_log_bin = cursor.fetchone()[0]
                cursor.execute("SET sql_log_bin = 0")
                sql_log_bin_modified = True
            except mysql_errors.Error:
                # Skip sql_log_bin if we can't modify it (replication not used or no permission)
                sql_log_bin_modified = False
                original_sql_log_bin = None

            # Start transaction after setting session variables
            connection.start_transaction()

            start_time = time.time()

            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                batch_num = (i // batch_size) + 1

                try:
                    cursor.executemany(query, batch)

                except mysql_errors.OperationalError as e:
                    if "Lost connection" in str(e):
                        # Get a new connection and cursor
                        connection.close()
                        connection = self.get_connection()
                        cursor = connection.cursor()
                        connection.start_transaction()
                        # Retry the batch
                        cursor.executemany(query, batch)
                    else:
                        raise

                except Exception as e:
                    raise

            # Commit transaction
            connection.commit()

            total_time = time.time() - start_time

        except Exception as e:
            # Rollback transaction on error
            if connection and connection.is_connected():
                try:
                    connection.rollback()
                except:
                    pass
            raise
        finally:
            # Restore original settings
            if cursor and connection and connection.is_connected():
                try:
                    cursor.execute(f"SET unique_checks = {original_unique_checks}")
                    cursor.execute(
                        f"SET foreign_key_checks = {original_foreign_key_checks}"
                    )
                    if sql_log_bin_modified and original_sql_log_bin is not None:
                        cursor.execute(f"SET sql_log_bin = {original_sql_log_bin}")
                    connection.autocommit = original_autocommit
                except:
                    pass

            # Clean up resources
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()

    def verify_battery_pack_exists(self, battery_pack_id):
        query = "SELECT * FROM battery_packs WHERE id = %s LIMIT 1"

        try:
            result = self.execute_query(
                query, (battery_pack_id,), fetch_one=True, dictionary=True
            )

            if not result:
                return None, f"Battery pack ID not found: {battery_pack_id}"

            return result, None

        except DatabaseError:
            return (
                None,
                f"Database error while verifying battery pack {battery_pack_id}",
            )
        except Exception as e:
            return None, f"Failed to verify battery pack: {e}"

    def insert_test_data(self, battery_pack_id, test_data):
        test_id = str(uuid.uuid4())
        current_time = datetime.now().isoformat()

        # Extract test information and step plan
        test_info = test_data.get("test", {}).get("test_information", {})
        step_plan = test_data.get("test", {}).get("step_plan", [])

        # Prepare the main test insert query
        test_insert_query = """
            INSERT INTO battery_pack_cycle_csv_tests (
                id, start_step_id, cycle_count, record_settings, voltage_range,
                current_range, active_material, volt_upper, volt_lower,
                current_upper, current_lower, start_time, nominal_capacity,
                pn, builder, remarks, barcode, battery_pack_id, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        test_params = (
            test_id,
            test_info.get("start_step_id"),
            test_info.get("cycle_count"),
            test_info.get("record_settings"),
            test_info.get("voltage_range"),
            test_info.get("current_range"),
            test_info.get("active_material"),
            test_info.get("volt_upper"),
            test_info.get("volt_lower"),
            test_info.get("current_upper"),
            test_info.get("current_lower"),
            test_info.get("start_time"),
            test_info.get("nominal_capacity"),
            test_info.get("pn"),
            test_info.get("builder"),
            test_info.get("remarks"),
            test_info.get("barcode"),
            battery_pack_id,
            current_time,
            current_time,
        )

        # Prepare step plan insert queries if needed
        queries = [(test_insert_query, test_params)]

        # insert step plan if it exists
        if step_plan:
            step_insert_query = """
                INSERT INTO battery_pack_cycle_csv_test_step_plans (
                    id, step_index, step_name, step_time, voltage_v, c_rate_c,
                    current_a, cut_off_voltage_v, cut_off_c_rate_c, cut_off_curr_a,
                    energy_wh, v_v, power_w, resistance_m, capacity_ah, record_settings,
                    aux_ch_recording_condition, max_vi_v, min_vi_v, max_ti, min_ti,
                    segment_record1, segment_record2, current_range_a,
                    battery_pack_cycle_csv_test_id, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s)
            """

            for step in step_plan:
                step_params = (
                    str(uuid.uuid4()),
                    step.get("step_index"),
                    step.get("step_name"),
                    step.get("step_time_hh:mm:ss_ms"),
                    step.get("voltage_v"),
                    step.get("c_rate_c"),
                    step.get("current_a"),
                    step.get("cut_off_voltage_v"),
                    step.get("cut_off_c_rate_c"),
                    step.get("cut_off_curr_a"),
                    step.get("energy_wh"),
                    step.get("v_v"),
                    step.get("power_w"),
                    step.get("resistance_m"),
                    step.get("capacity_ah"),
                    step.get("record_settings"),
                    step.get("aux_ch_recording_condition"),
                    step.get("max_vi_v"),
                    step.get("min_vi_v"),
                    step.get("max_ti"),
                    step.get("min_ti"),
                    step.get("segment_record1"),
                    step.get("segment_record2"),
                    step.get("current_range_a"),
                    test_id,
                    current_time,
                    current_time,
                )
                queries.append((step_insert_query, step_params))

        try:
            self.execute_transaction(queries)
            return test_id, None

        except Exception as e:
            return None, f"Failed to insert test data: {e}"

    def insert_unit_data(self, test_id, unit_data):
        current_time = datetime.now().isoformat()

        unit_id = str(uuid.uuid4())

        # Extract test information and step plan
        unit = unit_data.get("unit", {})
        plans = unit.get("list_of_unit_plans", {})

        # Prepare the main test insert query
        unit_query = """
            INSERT INTO battery_pack_cycle_csv_test_units (
                id, device, start_time, end_time, battery_pack_cycle_csv_test_id, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        test_params = (
            unit_id,
            unit.get("device"),
            unit.get("start_time"),
            unit.get("end_time"),
            test_id,
            current_time,
            current_time,
        )

        # Prepare step plan insert queries if needed
        queries = [(unit_query, test_params)]

        # insert step plan if it exists
        if plans:
            plans_insert_queryy = """
                INSERT INTO battery_pack_cycle_csv_test_unit_plans (
                   id, time, current, voltage, capacity, energy, power,
                   internal_resistance, temperature, air_pressure,
                   battery_pack_cycle_csv_test_unit_id, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            plans_params = (
                str(uuid.uuid4()),
                plans.get("time"),
                plans.get("current"),
                plans.get("voltage"),
                plans.get("capacity"),
                plans.get("energy"),
                plans.get("power"),
                plans.get("internal_resistance"),
                plans.get("temperature"),
                plans.get("air_pressure"),
                unit_id,
                current_time,
                current_time,
            )
            queries.append((plans_insert_queryy, plans_params))

        try:
            self.execute_transaction(queries)
            return test_id, None

        except Exception as e:
            return None, f"Failed to insert unit data: {e}"

    def insert_cycle_data(self, test_id, test_data):
        cycle_id = str(uuid.uuid4())
        current_time = datetime.now().isoformat()

        # Extract test information and step plan
        cycle = test_data.get("cycle", [])

        queries = []

        # insert step plan if it exists
        if cycle:
            cycle_insert_query = """
                INSERT INTO battery_pack_cycle_csv_test_cycles (
                   id, cycle_index, chg_cap_ah, dchg_cap_ah,
                   chg_dchg_eff_percent, chg_energy_wh, dchg_energy_wh,
                   chg_time, dchg_time, battery_pack_cycle_csv_test_id,
                   created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for c in cycle:
                cycle_params = (
                    str(uuid.uuid4()),
                    c.get("cycle_index"),
                    c.get("chg_cap_ah"),
                    c.get("dchg_cap_ah"),
                    c.get("chg_dchg_eff_percent"),
                    c.get("chg_energy_wh"),
                    c.get("dchg_energy_wh"),
                    c.get("chg_time"),
                    c.get("dchg_time"),
                    test_id,
                    current_time,
                    current_time,
                )
                queries.append((cycle_insert_query, cycle_params))

        try:
            self.execute_transaction(queries)
            return test_id, None

        except Exception as e:
            return None, f"Failed to insert cycle data: {e}"

    def insert_steps_data(self, test_id, test_data):
        current_time = datetime.now().isoformat()

        # Extract test information and step plan
        steps = test_data.get("step", [])

        queries = []

        # insert step plan if it exists
        if steps:
            steps_insert_query = """
                INSERT INTO battery_pack_cycle_csv_test_steps (
                  id, cycle_index, step_index, step_number,
                  step_type, step_time, oneset_date, end_date,
                  capacity_ah, energy_wh, oneset_volt_v, end_voltage_v,
                  battery_pack_cycle_csv_test_id, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for step in steps:
                step_params = (
                    str(uuid.uuid4()),
                    step.get("cycle_index"),
                    step.get("step_index"),
                    step.get("step_number"),
                    step.get("step_type"),
                    step.get("step_time"),
                    step.get("oneset_date"),
                    step.get("end_date"),
                    step.get("capacity_ah"),
                    step.get("energy_wh"),
                    step.get("oneset_volt_v"),
                    step.get("end_voltage_v"),
                    test_id,
                    current_time,
                    current_time,
                )
                queries.append((steps_insert_query, step_params))

        try:
            self.execute_transaction(queries)
            return test_id, None

        except Exception as e:
            return None, f"Failed to insert step data: {e}"

    def insert_records_data(self, test_id, test_data):
        current_time = datetime.now().isoformat()

        # Extract test information and step plan
        records = test_data.get("record", [])

        # insert step plan if it exists
        if records:
            insert_query = """
                INSERT INTO battery_pack_cycle_csv_test_records (
                  id, datapoint, step_type, time, total_time,
                  current_a, voltage_v, capacity_ah, energy_wh,
                  date, power_w, battery_pack_cycle_csv_test_id,
                  created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Prepare all data for bulk insert
            bulk_data = []
            for e in records:
                params = (
                    str(uuid.uuid4()),
                    e.get("datapoint"),
                    e.get("step_type"),
                    e.get("time"),
                    e.get("total_time"),
                    e.get("current_a"),
                    e.get("voltage_v"),
                    e.get("capacity_ah"),
                    e.get("energy_wh"),
                    e.get("date"),
                    e.get("power_w"),
                    test_id,
                    current_time,
                    current_time,
                )
                bulk_data.append(params)

        try:
            if records:
                self.execute_bulk_insert(insert_query, bulk_data)
            return test_id, None

        except Exception as e:
            return None, f"Failed to insert records: {e}"

    def insert_logs_data(self, test_id, test_data):
        current_time = datetime.now().isoformat()

        # Extract test information and step plan
        logs = test_data.get("log", [])

        # insert step plan if it exists
        if logs:
            insert_query = """
                INSERT INTO battery_pack_cycle_csv_test_logs (
                  id, datapoint, time, class, event, detailed_log_description, battery_pack_cycle_csv_test_id, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Prepare all data for bulk insert
            bulk_data = []
            for e in logs:
                params = (
                    str(uuid.uuid4()),
                    e.get("datapoint"),
                    e.get("time"),
                    e.get("class"),
                    e.get("event"),
                    e.get("detailed_log_description"),
                    test_id,
                    current_time,
                    current_time,
                )
                bulk_data.append(params)

        try:
            if logs:
                self.execute_bulk_insert(insert_query, bulk_data)
            return test_id, None

        except Exception as e:
            return None, f"Failed to insert logs: {e}"

    def insert_aux_dbc_data(self, test_id, test_data):
        current_time = datetime.now().isoformat()

        # Extract test information and step plan
        aux = test_data.get("aux_dbc", [])

        # insert step plan if it exists
        if aux:
            insert_query = """
                INSERT INTO battery_pack_cycle_csv_test_aux_dbcs (
                  id, datapoint, date, obc_error5, obc_error4, obc_error3, obc_error2, obc_error1,
                  obc_temperature, obc_present_votage_v, obc_present_current_a, obc_input_voltage_v,
                  obc_activation, fault_code, warning_code, soc_pct, bms_obc_en, max_chg_volt_v_v,
                  max_chg_current_a_a, balancing_circuit_temp, mos_temp, bms_temp_8_c, bms_temp_7_c,
                  bms_temp_4_c, bms_temp_3_c, bms_temp_1_c, bms_temp_2_c, cell_volt_mv_20_mv, cell_volt_mv_19_mv,
                  cell_volt_mv_18_mv, cell_volt_mv_17_mv, cell_volt_mv_16_mv, cell_volt_mv_15_mv, cell_volt_mv_14_mv,
                  cell_volt_mv_13_mv, cell_volt_mv_9_mv, cell_volt_mv_12_mv, cell_volt_mv_11_mv,
                  cell_volt_mv_10_mv, cell_volt_mv_8_mv, cell_volt_mv_7_mv, cell_volt_mv_6_mv,
                  cell_volt_mv_5_mv, cell_volt_mv_4_mv, cell_volt_mv_3_mv, cell_volt_mv_2_mv,
                  cell_volt_mv_1_mv, bms_warn_22, bms_warn_21, bms_warn_20, bms_warn_19, bms_warn_18,
                  bms_warn_17, bms_warn_16, bms_warn_15, bms_warn_14, bms_warn_13, bms_warn_12, bms_warn_11,
                  bms_warn_10, bms_warn_9, bms_warn_8, bms_warn_7, bms_warn_6, bms_warn_5, bms_warn_4,
                  bms_warn_3, bms_warn_2, bms_warn_1, bms_err_22, bms_err_21, bms_err_20, bms_err_19,
                  bms_err_18, bms_err_17, bms_err_16, bms_err_15, bms_err_14, bms_err_13, bms_err_12,
                  bms_err_11, bms_err_10, bms_err_9, bms_err_8, bms_err_7, bms_err_6, bms_err_5, bms_err_4,
                  bms_err_3, bms_err_2, bms_err_1, err_lvl, cycles, ascii_coded_hex_revision,
                  ascii_coded_hex_minjorversion, ascii_coded_hex_majorversion, bms_serial_num_17,
                  bms_serial_num_16, bms_serial_num_15, bms_serial_num_14, bms_serial_num_13,
                  bms_serial_num_12, bms_serial_num_11, bms_serial_num_10, bms_serial_num_9, bms_serial_num_8,
                  bms_serial_num_1, bms_serial_num_7, bms_serial_num_6, bms_serial_num_5, bms_serial_num_4,
                  bms_serial_num_3, bms_serial_num_2, lowest_cell_volt_mv_mv, highest_cell_volt_mv_mv,
                  max_regen_current_a_a, max_dschg_current_a_a, dcdc_mos_status, bms_current_a_a,
                  bms_alive_counter, bms_volt_v_v, bms_soh_pct, bms_soc_pct, bms_charger_bool,
                  chg_relay_bool, dschg_relay_bool, pre_dschg_bool, bms_status, dcdc_en, bms_charge_en,
                  disable_insulation_detection_en, bms_discharge_en, battery_pack_cycle_csv_test_id, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Prepare all data for bulk insert
            bulk_data = []
            for e in aux:
                params = (
                    str(uuid.uuid4()),
                    e.get("datapoint"),
                    e.get("date"),
                    e.get("obc_error5"),
                    e.get("obc_error4"),
                    e.get("obc_error3"),
                    e.get("obc_error2"),
                    e.get("obc_error1"),
                    e.get("obc_temperature"),
                    e.get("obc_present_votage_v"),
                    e.get("obc_present_current_a"),
                    e.get("obc_input_voltage_v"),
                    e.get("obc_activation"),
                    e.get("fault_code"),
                    e.get("warning_code"),
                    e.get("soc_pct"),
                    e.get("bms_obc_en"),
                    e.get("max_chg_volt_v_v"),
                    e.get("max_chg_current_a_a"),
                    e.get("balancing_circuit_temp"),
                    e.get("mos_temp"),
                    e.get("bms_temp_8_c"),
                    e.get("bms_temp_7_c"),
                    e.get("bms_temp_4_c"),
                    e.get("bms_temp_3_c"),
                    e.get("bms_temp_1_c"),
                    e.get("bms_temp_2_c"),
                    e.get("cell_volt_mv_20_mv"),
                    e.get("cell_volt_mv_19_mv"),
                    e.get("cell_volt_mv_18_mv"),
                    e.get("cell_volt_mv_17_mv"),
                    e.get("cell_volt_mv_16_mv"),
                    e.get("cell_volt_mv_15_mv"),
                    e.get("cell_volt_mv_14_mv"),
                    e.get("cell_volt_mv_13_mv"),
                    e.get("cell_volt_mv_9_mv"),
                    e.get("cell_volt_mv_12_mv"),
                    e.get("cell_volt_mv_11_mv"),
                    e.get("cell_volt_mv_10_mv"),
                    e.get("cell_volt_mv_8_mv"),
                    e.get("cell_volt_mv_7_mv"),
                    e.get("cell_volt_mv_6_mv"),
                    e.get("cell_volt_mv_5_mv"),
                    e.get("cell_volt_mv_4_mv"),
                    e.get("cell_volt_mv_3_mv"),
                    e.get("cell_volt_mv_2_mv"),
                    e.get("cell_volt_mv_1_mv"),
                    e.get("bms_warn_22"),
                    e.get("bms_warn_21"),
                    e.get("bms_warn_20"),
                    e.get("bms_warn_19"),
                    e.get("bms_warn_18"),
                    e.get("bms_warn_17"),
                    e.get("bms_warn_16"),
                    e.get("bms_warn_15"),
                    e.get("bms_warn_14"),
                    e.get("bms_warn_13"),
                    e.get("bms_warn_12"),
                    e.get("bms_warn_11"),
                    e.get("bms_warn_10"),
                    e.get("bms_warn_9"),
                    e.get("bms_warn_8"),
                    e.get("bms_warn_7"),
                    e.get("bms_warn_6"),
                    e.get("bms_warn_5"),
                    e.get("bms_warn_4"),
                    e.get("bms_warn_3"),
                    e.get("bms_warn_2"),
                    e.get("bms_warn_1"),
                    e.get("bms_err_22"),
                    e.get("bms_err_21"),
                    e.get("bms_err_20"),
                    e.get("bms_err_19"),
                    e.get("bms_err_18"),
                    e.get("bms_err_17"),
                    e.get("bms_err_16"),
                    e.get("bms_err_15"),
                    e.get("bms_err_14"),
                    e.get("bms_err_13"),
                    e.get("bms_err_12"),
                    e.get("bms_err_11"),
                    e.get("bms_err_10"),
                    e.get("bms_err_9"),
                    e.get("bms_err_8"),
                    e.get("bms_err_7"),
                    e.get("bms_err_6"),
                    e.get("bms_err_5"),
                    e.get("bms_err_4"),
                    e.get("bms_err_3"),
                    e.get("bms_err_2"),
                    e.get("bms_err_1"),
                    e.get("err_lvl"),
                    e.get("cycles"),
                    e.get("ascii_coded_hex_revision"),
                    e.get("ascii_coded_hex_minjorversion"),
                    e.get("ascii_coded_hex_majorversion"),
                    e.get("bms_serial_num_17"),
                    e.get("bms_serial_num_16"),
                    e.get("bms_serial_num_15"),
                    e.get("bms_serial_num_14"),
                    e.get("bms_serial_num_13"),
                    e.get("bms_serial_num_12"),
                    e.get("bms_serial_num_11"),
                    e.get("bms_serial_num_10"),
                    e.get("bms_serial_num_9"),
                    e.get("bms_serial_num_8"),
                    e.get("bms_serial_num_1"),
                    e.get("bms_serial_num_7"),
                    e.get("bms_serial_num_6"),
                    e.get("bms_serial_num_5"),
                    e.get("bms_serial_num_4"),
                    e.get("bms_serial_num_3"),
                    e.get("bms_serial_num_2"),
                    e.get("lowest_cell_volt_mv_mv"),
                    e.get("highest_cell_volt_mv_mv"),
                    e.get("max_regen_current_a_a"),
                    e.get("max_dschg_current_a_a"),
                    e.get("dcdc_mos_status"),
                    e.get("bms_current_a_a"),
                    e.get("bms_alive_counter"),
                    e.get("bms_volt_v_v"),
                    e.get("bms_soh_pct"),
                    e.get("bms_soc_pct"),
                    e.get("bms_charger_bool"),
                    e.get("chg_relay_bool"),
                    e.get("dschg_relay_bool"),
                    e.get("pre_dschg_bool"),
                    e.get("bms_status"),
                    e.get("dcdc_en"),
                    e.get("bms_charge_en"),
                    e.get("disable_insulation_detection_en"),
                    e.get("bms_discharge_en"),
                    test_id,
                    current_time,
                    current_time,
                )
                bulk_data.append(params)

        try:
            if aux:
                # Use smaller batch size for aux_dbc due to large number of columns (130+ columns)
                self.execute_bulk_insert(insert_query, bulk_data, batch_size=500)
            return test_id, None

        except Exception as e:
            return None, f"Failed to insert aux: {e}"

    def health_check(self) -> Dict[str, Any]:
        """
        Perform database health check.

        Returns:
            Dict containing health check results.
        """
        try:
            start_time = time.time()

            # Test basic connectivity
            result = self.execute_query("SELECT 1 as health_check", fetch_one=True)

            end_time = time.time()
            response_time = round((end_time - start_time) * 1000, 2)

            health_status = {
                "status": "healthy" if result and result[0] == 1 else "unhealthy",
                "response_time_ms": response_time,
                "pool_size": self.config.pool_size if self._pool else 0,
                "timestamp": datetime.now().isoformat(),
            }
            return health_status

        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

    def close_pool(self) -> None:
        """Close the connection pool."""
        if self._pool:
            try:
                # MySQL Connector/Python doesn't have a direct close method for pools
                # The pool will be garbage collected
                self._pool = None
            except Exception as e:
                pass


# Global database manager instance
db_manager = DatabaseManager()


def get_database_manager() -> DatabaseManager:
    """
    Get the global database manager instance.

    Returns:
        DatabaseManager: The global database manager.
    """
    return db_manager


# Convenience functions for backward compatibility
def setup_database_connection() -> MySQLConnection:
    """
    Legacy function for backward compatibility.

    Returns:
        MySQLConnection: Database connection.
    """
    return db_manager.get_connection()
