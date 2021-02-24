import logging
import os
from psycopg2.extras import RealDictCursor
from functools import wraps
from typing import *
from threading import Thread
from queue import Queue
import databases

# Configure the logging tool in the AWS Lambda function.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize constants with parameters to configure.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]

# The connection to the database will be created the first time the AWS Lambda function is called.
# Any subsequent call to the function will use the same database connection until the container stops.
POSTGRESQL_CONNECTION = None


def run_multithreading_tasks(functions: List[Dict[AnyStr, Union[Callable, Dict[AnyStr, Any]]]]) -> Dict[AnyStr, Any]:
    # Create the empty list to save all parallel threads.
    threads = []

    # Create the queue to store all results of functions.
    queue = Queue()

    # Create the thread for each function.
    for function in functions:
        # Check whether the input arguments have keys in their dictionaries.
        try:
            function_object = function["function_object"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        try:
            function_arguments = function["function_arguments"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)

        # Add the instance of the queue to the list of function arguments.
        function_arguments["queue"] = queue

        # Create the thread.
        thread = Thread(target=function_object, kwargs=function_arguments)
        threads.append(thread)

    # Start all parallel threads.
    for thread in threads:
        thread.start()

    # Wait until all parallel threads are finished.
    for thread in threads:
        thread.join()

    # Get the results of all threads.
    results = {}
    while not queue.empty():
        results = {**results, **queue.get()}

    # Return the results of all threads.
    return results


def check_input_arguments(**kwargs) -> None:
    # Make sure that all the necessary arguments for the AWS Lambda function are present.
    try:
        input_arguments = kwargs["event"]["arguments"]["input"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the format and values of required arguments in the list of input arguments.
    required_arguments = ["usersIds"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(argument_name))
        if argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(argument_name))
        if not isinstance(argument_value, list):
            raise Exception("The data type of the argument '{0}' is incorrect".format(argument_name))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "users_ids": input_arguments["usersIds"]
        }
    })

    # Return nothing.
    return None


def reuse_or_recreate_postgresql_connection(queue: Queue) -> None:
    global POSTGRESQL_CONNECTION
    if not POSTGRESQL_CONNECTION:
        try:
            POSTGRESQL_CONNECTION = databases.create_postgresql_connection(
                POSTGRESQL_USERNAME,
                POSTGRESQL_PASSWORD,
                POSTGRESQL_HOST,
                POSTGRESQL_PORT,
                POSTGRESQL_DB_NAME
            )
        except Exception as error:
            logger.error(error)
            raise Exception("Unable to connect to the PostgreSQL database.")
    queue.put({"postgresql_connection": POSTGRESQL_CONNECTION})
    return None


def postgresql_wrapper(function):
    @wraps(function)
    def wrapper(**kwargs):
        try:
            postgresql_connection = kwargs["postgresql_connection"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)
        kwargs["cursor"] = cursor
        result = function(**kwargs)
        cursor.close()
        return result
    return wrapper


@postgresql_wrapper
def get_aggregated_data(**kwargs) -> Dict[AnyStr, Any]:
    # Check if the input dictionary has all the necessary keys.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        sql_arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the SQL request that returns the aggregated list of IDs by user type.
    sql_statement = """
    select
        array_remove(array_agg(distinct internal_user_id), null)::text[] as internal_users_ids
    from
        users
    where user_id in %(users_ids)s;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the aggregated data.
    return cursor.fetchone()


@postgresql_wrapper
def update_internal_users(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        sql_arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        internal_users_ids = sql_arguments["internal_users_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Execute the SQL query only if the list is not empty.
    if internal_users_ids:
        # Put a tag for deletion for the internal users.
        sql_statement = """
        update
            internal_users
        set
            entry_deleted_date_time = null
        where
            internal_user_id in %(internal_users_ids)s;
        """

        # Execute the SQL query dynamically, in a convenient and safe way.
        try:
            cursor.execute(sql_statement, sql_arguments)
        except Exception as error:
            logger.error(error)
            raise Exception(error)

    # Return nothing.
    return None


@postgresql_wrapper
def update_users(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        sql_arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        users_ids = sql_arguments["users_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Execute the SQL query only if the list is not empty.
    if users_ids:
        # Put a tag for deletion for the users.
        sql_statement = """
        update
            users
        set
            entry_deleted_date_time = null
        where
            user_id in %(users_ids)s;
        """

        # Execute the SQL query dynamically, in a convenient and safe way.
        try:
            cursor.execute(sql_statement, sql_arguments)
        except Exception as error:
            logger.error(error)
            raise Exception(error)

    # Return nothing.
    return None


def lambda_handler(event, context):
    """
    :param event: The AWS Lambda function uses this parameter to pass in event data to the handler.
    :param context: The AWS Lambda function uses this parameter to provide runtime information to your handler.
    """
    # Run several initialization functions in parallel.
    results_of_tasks = run_multithreading_tasks([
        {
            "function_object": check_input_arguments,
            "function_arguments": {
                "event": event
            }
        },
        {
            "function_object": reuse_or_recreate_postgresql_connection,
            "function_arguments": {}
        }
    ])

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]
    users_ids = input_arguments["users_ids"]

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Get the aggregated data.
    aggregated_data = get_aggregated_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "users_ids": users_ids
        }
    )

    # Define several variables that will be used in the future.
    internal_users_ids = aggregated_data.get("internal_users_ids", [])

    # Run several initialization functions in parallel.
    run_multithreading_tasks([
        {
            "function_object": update_internal_users,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {
                    "internal_users_ids": internal_users_ids
                }
            }
        },
        {
            "function_object": update_users,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {
                    "users_ids": users_ids
                }
            }
        }
    ])

    # Return the list of user ids as the response.
    return users_ids
