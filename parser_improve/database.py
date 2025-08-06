"""
Advanced database management utilities with connection pooling, error handling, and transactions.
"""

import logging
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


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
            logger.info(
                f"Connection pool '{self.config.pool_name}' created with {self.config.pool_size} connections"
            )
        except MySQLError as e:
            error_msg = f"Failed to create connection pool: {e}"
            logger.error(error_msg)
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

                logger.debug(f"Connection obtained from pool (attempt {attempt + 1})")
                return connection

            except MySQLError as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    error_msg = (
                        f"Failed to get connection after {max_retries} attempts: {e}"
                    )
                    logger.error(error_msg)
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
            logger.debug("Database connection acquired")
            yield connection
        except Exception as e:
            logger.error(f"Error in connection context: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
                logger.debug("Database connection released")

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
            logger.debug("Database cursor acquired")
            yield cursor

        except Exception as e:
            logger.error(f"Error in cursor context: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
                logger.debug("Database cursor closed")

            if connection_created and connection and connection.is_connected():
                connection.close()
                logger.debug("Database connection closed")

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
            logger.debug("Database transaction started")

            yield connection

            # Commit transaction
            connection.commit()
            logger.debug("Database transaction committed")

        except Exception as e:
            if transaction_started and connection and connection.is_connected():
                try:
                    connection.rollback()
                    logger.warning(
                        f"Database transaction rolled back due to error: {e}"
                    )
                except MySQLError as rollback_error:
                    logger.error(f"Failed to rollback transaction: {rollback_error}")
            raise TransactionError(f"Transaction failed: {e}") from e
        finally:
            if connection_created and connection and connection.is_connected():
                connection.close()
                logger.debug("Database connection closed")

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
                        logger.debug(
                            f"Query executed, fetched one result: {bool(result)}"
                        )
                        return result
                    elif fetch_all:
                        result = cursor.fetchall()
                        logger.debug(f"Query executed, fetched {len(result)} results")
                        return result
                    else:
                        logger.debug("Query executed, no fetch requested")
                        return None

        except MySQLError as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
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

                    logger.info(
                        f"Transaction completed successfully with {len(queries)} queries"
                    )

            return results if return_results else None

        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            raise TransactionError(f"Transaction execution failed: {e}") from e

    def verify_battery_pack_exists(self, battery_pack_id):
        query = "SELECT * FROM battery_packs WHERE id = %s LIMIT 1"

        try:
            result = self.execute_query(
                query, (battery_pack_id,), fetch_one=True, dictionary=True
            )

            if not result:
                return None, f"Battery pack ID not found: {battery_pack_id}"

            logger.info(f"Battery pack {battery_pack_id} verified successfully")
            return result, None

        except DatabaseError:
            return (
                None,
                f"Database error while verifying battery pack {battery_pack_id}",
            )
        except Exception as e:
            logger.error(f"Error verifying battery pack {battery_pack_id}: {e}")
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
            logger.info(f"Test data inserted successfully with ID: {test_id}")
            return test_id, None

        except Exception as e:
            logger.error(f"Failed to insert test data: {e}")
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
            logger.info(f"unit and plans inserted successfully with ID: {unit_id}")
            return test_id, None

        except Exception as e:
            logger.error(f"Failed to insert unit and plans: {e}")
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
            logger.info(f"cycle data inserted successfully with ID: {test_id}")
            return test_id, None

        except Exception as e:
            logger.error(f"Failed to insert cycle data: {e}")
            return None, f"Failed to insert cycle data: {e}"

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
            response_time = round(
                (end_time - start_time) * 1000, 2
            )  # Convert to milliseconds

            health_status = {
                "status": "healthy" if result and result[0] == 1 else "unhealthy",
                "response_time_ms": response_time,
                "pool_size": self.config.pool_size if self._pool else 0,
                "timestamp": datetime.now().isoformat(),
            }

            logger.info(f"Database health check completed: {health_status['status']}")
            return health_status

        except Exception as e:
            logger.error(f"Database health check failed: {e}")
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
                logger.info("Connection pool closed")
            except Exception as e:
                logger.error(f"Error closing connection pool: {e}")


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
    logger.warning(
        "setup_database_connection() is deprecated. Use DatabaseManager instead."
    )
    return db_manager.get_connection()
