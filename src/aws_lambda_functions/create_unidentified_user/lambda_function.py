import logging
import os
import json
from psycopg2.extras import RealDictCursor
from functools import wraps
from typing import *
from threading import Thread
from queue import Queue
import databases
import utils

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
    required_arguments = ["metadata"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(argument_name))
        if argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(argument_name))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "metadata": json.dumps(input_arguments["metadata"])
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
def create_unidentified_user(**kwargs) -> AnyStr:
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

    # Prepare the SQL request that creates the new created unidentified user.
    sql_statement = """
    insert into unidentified_users (
        metadata
    ) values (
        %(metadata)s
    ) returning
        unidentified_user_id::text;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Define the id of the new created unidentified user.
    try:
        unidentified_user_id = cursor.fetchone()["unidentified_user_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Add the id of the new created unidentified user to the map of the sql arguments.
    sql_arguments["unidentified_user_id"] = unidentified_user_id

    # Prepare the SQL request that creates the new created user.
    sql_statement = """
    insert into users (
        unidentified_user_id
    ) values (
        %(unidentified_user_id)s
    ) returning
        user_id::text;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Define the id of the new created user.
    try:
        user_id = cursor.fetchone()["user_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Return the id of the new user.
    return user_id


@postgresql_wrapper
def get_unidentified_user_data(**kwargs) -> Any:
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

    # Prepare the SQL request that return information about new created unidentified user.
    sql_statement = """
    select
        users.user_id::text,
        users.user_nickname::text,
        users.user_profile_photo_url:text,
        unidentified_users.metadata::text
    from
        users
    left join unidentified_users on
        users.unidentified_user_id = unidentified_users.unidentified_user_id
    where
        users.user_id = %(user_id)s
    limit 1;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the information of the new created unidentified user.
    return cursor.fetchone()


def analyze_and_format_unidentified_user_data(**kwargs) -> Any:
    # Check if the input dictionary has all the necessary keys.
    try:
        unidentified_user_data = kwargs["unidentified_user_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the unidentified user data.
    unidentified_user = {}
    if unidentified_user_data:
        for key, value in unidentified_user_data.items():
            unidentified_user[utils.camel_case(key)] = value

    # Return the information of the new created unidentified user.
    return unidentified_user


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

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Create the new unidentified user.
    user_id = create_unidentified_user(postgresql_connection=postgresql_connection, sql_arguments=input_arguments)

    # Get information of the new created unidentified user.
    unidentified_user_data = get_unidentified_user_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "user_id": user_id
        }
    )

    # Define variables that stores formatted information about unidentified user.
    unidentified_user = analyze_and_format_unidentified_user_data(unidentified_user_data=unidentified_user_data)

    # Return the information of the new created unidentified user.
    return unidentified_user
