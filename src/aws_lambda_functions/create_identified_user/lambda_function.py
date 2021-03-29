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
    required_arguments = ["metadata"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name == "metadata" and argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(argument_name))
        if argument_name == "metadata" and argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(argument_name))

    # Validation primary and secondary phone numbers.
    if input_arguments.get("userPrimaryPhoneNumber", None) is not None:
        input_arguments["userPrimaryPhoneNumber"] = re.sub(
            "[^0-9+]",
            "",
            input_arguments["userPrimaryPhoneNumber"]
        )
    if input_arguments.get("userSecondaryPhoneNumber", None) is not None:
        input_arguments["userSecondaryPhoneNumber"] = [
            re.sub(
                "[^0-9+]",
                "",
                phone_number
            ) for phone_number in input_arguments["userSecondaryPhoneNumber"]
        ]

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "user_profile_photo_url": input_arguments.get("userProfilePhotoUrl", None),
            "identified_user_first_name": input_arguments.get("userFirstName", None),
            "identified_user_last_name": input_arguments.get("userLastName", None),
            "identified_user_middle_name": input_arguments.get("userMiddleName", None),
            "identified_user_primary_email": input_arguments.get("userPrimaryEmail", None),
            "identified_user_secondary_email": input_arguments.get("userSecondaryEmail", None),
            "identified_user_primary_phone_number": input_arguments.get("userPrimaryPhoneNumber", None),
            "identified_user_secondary_phone_number": input_arguments.get("userSecondaryPhoneNumber", None),
            "gender_id": input_arguments.get("genderId", None),
            "metadata": json.dumps(input_arguments["metadata"]),
            "telegram_username": input_arguments.get("telegramUsername", None),
            "whatsapp_profile": input_arguments.get("whatsappProfile", None),
            "whatsapp_username": input_arguments.get("whatsappUsername", None),
            "instagram_private_username": input_arguments.get("instagramPrivateUsername", None)
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
def create_identified_user(**kwargs) -> AnyStr:
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

    # Prepare the SQL request that creates the new created identified user.
    sql_statement = """
    insert into identified_users (
        identified_user_first_name,
        identified_user_last_name,
        identified_user_middle_name,
        identified_user_primary_email,
        identified_user_secondary_email,
        identified_user_primary_phone_number,
        identified_user_secondary_phone_number,
        gender_id,
        metadata,
        telegram_username,
        whatsapp_profile,
        whatsapp_username,
        instagram_private_username
    ) values (
        %(identified_user_first_name)s,
        %(identified_user_last_name)s,
        %(identified_user_middle_name)s,
        %(identified_user_primary_email)s,
        %(identified_user_secondary_email)s,
        %(identified_user_primary_phone_number)s,
        %(identified_user_secondary_phone_number)s,
        %(gender_id)s,
        %(metadata)s,
        %(telegram_username)s,
        %(whatsapp_profile)s,
        %(whatsapp_username)s,
        %(instagram_private_username)s
    ) returning
        identified_user_id::text;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Define the id of the new created identified user.
    try:
        identified_user_id = cursor.fetchone()["identified_user_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Add the id of the new created identified user to the map of the sql arguments.
    sql_arguments["identified_user_id"] = identified_user_id

    # Prepare the SQL request that creates the new created user.
    sql_statement = """
    insert into users (
        identified_user_id,
        user_profile_photo_url
    ) values (
        %(identified_user_id)s,
        %(user_profile_photo_url)s
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

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Create the new identified user.
    user_id = create_identified_user(postgresql_connection=postgresql_connection, sql_arguments=input_arguments)

    # Get information of the new created identified user.
    identified_user_data = get_identified_user_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "user_id": user_id
        }
    )

    # Define variable that stores formatted information about identified user.
    identified_user = analyze_and_format_identified_user_data(identified_user_data=identified_user_data)

    # Return the information of the new created identified user.
    return identified_user
