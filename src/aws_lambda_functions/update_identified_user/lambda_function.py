import logging
import os
import uuid
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from functools import wraps
from typing import *
from threading import Thread
from queue import Queue
import databases
import utils
import re

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
    required_arguments = ["userId"]
    modified_arguments = [
        "userFirstName",
        "userLastName",
        "userMiddleName",
        "userPrimaryEmail",
        "userSecondaryEmail",
        "userPrimaryPhoneNumber",
        "userSecondaryPhoneNumber"
    ]
    formatted_arguments = {}
    for argument_name, argument_value in input_arguments.items():
        if argument_name == "userId" and argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(argument_name))
        if argument_name == "userId":
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '{0}' argument format is not UUID.".format(argument_name))
        if argument_name in modified_arguments:
            formatted_arguments["identified_{0}".format(utils.snake_case(argument_name))] = input_arguments[argument_name]
        else:
            formatted_arguments[utils.snake_case(argument_name)] = input_arguments[argument_name]

    # Validation primary and secondary phone numbers.
    if formatted_arguments.get("identified_user_primary_phone_number", None) is not None:
        formatted_arguments["identified_user_primary_phone_number"] = re.sub(
            "[^0-9+]",
            "",
            formatted_arguments["identified_user_primary_phone_number"]
        )
    if formatted_arguments.get("identified_user_secondary_phone_number", None) is not None:
        formatted_arguments["identified_user_secondary_phone_number"] = [
            re.sub(
                "[^0-9+]",
                "",
                phone_number
            ) for phone_number in formatted_arguments["identified_user_secondary_phone_number"]
        ]

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": formatted_arguments
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
def update_user_profile_photo_url(**kwargs) -> None:
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

    # Prepare the SQL request that updates the value of user's profile photo url.
    sql_statement = """
    update
        users
    set
        user_profile_photo_url = %(user_profile_photo_url)s
    where
        user_id = %(user_id)s;
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
def update_identified_user(**kwargs) -> None:
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

    # Generate SQL dynamically, in a convenient and safe way: "https://www.psycopg.org/docs/sql.html".
    removed_arguments = ["user_id", "user_profile_photo_url"]
    sql_statement_arguments = [argument for argument in list(sql_arguments.keys()) if argument not in removed_arguments]
    if sql_statement_arguments:
        sql_statement = sql.SQL("""
        update
            identified_users
        set
            {fields}
        where
            identified_user_id = (
                select
                    identified_user_id
                from
                    users
                where
                    user_id = {user_id}
                and
                    identified_user_id is not null
                limit 1
            );
        """).format(
            fields=sql.SQL(', ').join(
                sql.Composed([
                    sql.Identifier(argument_name),
                    sql.SQL(" = "),
                    sql.Placeholder(argument_name)
                ]) for argument_name in sql_statement_arguments
            ),
            user_id=sql.Placeholder("user_id")
        )

        # Execute the SQL query dynamically, in a convenient and safe way.
        try:
            cursor.execute(sql_statement, sql_arguments)
        except Exception as error:
            logger.error(error)
            raise Exception(error)

    # Return nothing.
    return None


@postgresql_wrapper
def get_identified_user_data(**kwargs) -> Any:
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

    # Prepare the SQL request that return information about new created identified user.
    sql_statement = """
    select
        users.user_id::text,
        users.user_nickname::text,
        users.user_profile_photo_url::text,
        identified_users.identified_user_first_name::text as user_first_name,
        identified_users.identified_user_last_name::text as user_last_name,
        identified_users.identified_user_middle_name::text as user_middle_name,
        identified_users.identified_user_primary_email::text as user_primary_email,
        identified_users.identified_user_secondary_email::text[] as user_secondary_email,
        identified_users.identified_user_primary_phone_number::text as user_primary_phone_number,
        identified_users.identified_user_secondary_phone_number::text[] as user_secondary_phone_number,
        genders.gender_id::text,
        genders.gender_technical_name::text,
        genders.gender_public_name::text,
        identified_users.metadata::text,
        identified_users.telegram_username::text,
        identified_users.whatsapp_profile::text,
        identified_users.whatsapp_username::text,
        identified_users.instagram_private_username::text
    from
        users
    left join identified_users on
        users.identified_user_id = identified_users.identified_user_id
    left join genders on
        identified_users.gender_id = genders.gender_id
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

    # Return the information of the new created identified user.
    return cursor.fetchone()


def analyze_and_format_identified_user_data(**kwargs) -> Any:
    # Check if the input dictionary has all the necessary keys.
    try:
        identified_user_data = kwargs["identified_user_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the identified user data.
    identified_user = {}
    if identified_user_data:
        gender = {}
        for key, value in identified_user_data.items():
            if key.startswith("gender_"):
                gender[utils.camel_case(key)] = value
            else:
                identified_user[utils.camel_case(key)] = value
        identified_user["gender"] = gender

    # Return the information of the new created identified user.
    return identified_user


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
    user_id = input_arguments["user_id"]
    user_profile_photo_url = input_arguments.get("user_profile_photo_url", None)

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Change the user's profile photo url.
    if user_profile_photo_url:
        update_user_profile_photo_url(
            postgresql_connection=postgresql_connection,
            sql_arguments={
                "user_id": user_id,
                "user_profile_photo_url": user_profile_photo_url
            }
        )

    # Update identified user's main information.
    update_identified_user(postgresql_connection=postgresql_connection, sql_arguments=input_arguments)

    # Get information of the identified user.
    identified_user_data = get_identified_user_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "user_id": user_id
        }
    )

    # Define variable that stores formatted information about identified user.
    identified_user = analyze_and_format_identified_user_data(identified_user_data=identified_user_data)

    # Return the information of the identified user.
    return identified_user
