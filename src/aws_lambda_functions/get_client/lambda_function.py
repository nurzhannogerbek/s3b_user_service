import logging
import os
import uuid
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
        input_arguments = kwargs["event"]["arguments"]
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
    for argument_name, argument_value in input_arguments.items():
        if argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(argument_name))
        if argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(argument_name))
        if argument_name == "userId":
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '{0}' argument format is not UUID.".format(argument_name))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "user_id": input_arguments["userId"]
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
def get_client_data(**kwargs) -> Any:
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

    # Prepare the SQL request that return information about the client.
    sql_statement = """
    select
        users.user_id::text,
        users.user_nickname::text,
        users.user_profile_photo_url::text,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then 'identified_user'::text
            else 'unidentified_user'::text
        end as user_type,
        users.entry_created_date_time::text as created_date_time,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_first_name::text
            else null
        end as user_first_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_last_name::text
            else null
        end as user_last_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_middle_name::text
            else null
        end as user_middle_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_primary_email::text
            else null
        end as user_primary_email,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_secondary_email::text[]
            else null
        end as user_secondary_email,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_primary_phone_number::text
            else null
        end as user_primary_phone_number,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_secondary_phone_number::text[]
            else null
        end as user_secondary_phone_number,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.metadata::text
            else unidentified_users.metadata::text
        end as metadata,
        case
            when users.identified_user_id is not null
            and users.unidentified_user_id is null then identified_users.telegram_username::text
            else null
        end as telegram_username,
        case
            when users.identified_user_id is not null
            and users.unidentified_user_id is null then identified_users.whatsapp_profile::text
            else null
        end as whatsapp_profile,
        case
            when users.identified_user_id is not null
            and users.unidentified_user_id is null then identified_users.whatsapp_username::text
            else null
        end as whatsapp_username,
        case
            when users.identified_user_id is not null
            and users.unidentified_user_id is null then identified_users.instagram_private_username::text
            else null
        end as instagram_private_username,
        case
            when users.identified_user_id is not null
            and users.unidentified_user_id is null then identified_users.vk_user_id::text
            else null
        end as vk_user_id,
        case
            when users.identified_user_id is not null
            and users.unidentified_user_id is null then identified_users.instagram_profile::text
            else null
        end as instagram_profile,
        genders.gender_id::text,
        genders.gender_technical_name::text,
        genders.gender_public_name::text
    from
        users
    left join identified_users on
        users.identified_user_id = identified_users.identified_user_id
    left join unidentified_users on
        users.unidentified_user_id = unidentified_users.unidentified_user_id
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

    # Return the information of the client.
    return cursor.fetchone()


def analyze_and_format_client_data(**kwargs) -> Any:
    # Check if the input dictionary has all the necessary keys.
    try:
        client_data = kwargs["client_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the client data.
    client = {}
    if client_data is not None:
        gender = {}
        for key, value in client_data.items():
            if key.startswith("gender_"):
                gender[utils.camel_case(key)] = value
            else:
                client[utils.camel_case(key)] = value
        client["gender"] = gender

    # Return the information of the client.
    return client


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

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Get information of the client.
    client_data = get_client_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "user_id": user_id
        }
    )

    # Define variable that stores formatted information about client.
    client = analyze_and_format_client_data(client_data=client_data)

    # Return the information of the client.
    return client
