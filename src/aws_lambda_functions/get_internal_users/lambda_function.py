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
    required_arguments = ["rootOrganizationId", "itemsCountPerPage", "currentPageNumber"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(argument_name))
        if argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(argument_name))
        if argument_name == "rootOrganizationId":
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '{0}' argument format is not UUID.".format(argument_name))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "root_organization_id": input_arguments["rootOrganizationId"],
            "items_count_per_page": input_arguments["itemsCountPerPage"],
            "current_page_number": input_arguments["currentPageNumber"]
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
def get_internal_users_data(**kwargs) -> List[Dict[AnyStr, Any]]:
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

    # Prepare the SQL request that returns the list of internal users who have interacted with the company.
    sql_statement = """
    select
        count(*) over() as total_items_count,
        internal_users.auth0_user_id::text,
        internal_users.auth0_metadata::text,
        users.user_id::text,
        users.user_nickname::text,
        users.user_profile_photo_url::text,
        internal_users.internal_user_first_name::text as user_first_name,
        internal_users.internal_user_last_name::text as user_last_name,
        internal_users.internal_user_middle_name::text as user_middle_name,
        internal_users.internal_user_primary_email::text as user_primary_email,
        internal_users.internal_user_secondary_email::text[] as user_secondary_email,
        internal_users.internal_user_primary_phone_number::text as user_primary_phone_number,
        internal_users.internal_user_secondary_phone_number::text[] as user_secondary_phone_number,
        internal_users.internal_user_position_name::text as user_position_name,
        genders.gender_id::text,
        genders.gender_technical_name::text,
        genders.gender_public_name::text,
        roles.role_id::text,
        roles.role_technical_name::text,
        roles.role_public_name::text,
        roles.role_description::text,
        organizations.organization_id::text,
        organizations.organization_name::text,
        organizations.organization_level::smallint,
        organizations.parent_organization_id::text,
        organizations.parent_organization_name::text,
        organizations.parent_organization_level::smallint,
        organizations.root_organization_id::text,
        organizations.root_organization_name::text,
        organizations.root_organization_level::smallint,
        organizations.tree_organization_id::text,
        organizations.tree_organization_name::text
    from
        users
    left join internal_users on
        users.internal_user_id = internal_users.internal_user_id
    left join genders on
        internal_users.gender_id = genders.gender_id
    left join roles on
        internal_users.role_id = roles.role_id
    left join organizations on
        internal_users.organization_id = organizations.organization_id
    where
        users.internal_user_id is not null
    and
        users.entry_deleted_date_time is null
    and
        organizations.root_organization_id = %(root_organization_id)s
    offset %(offset)s limit %(limit)s;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the list of internal users who have interacted with the company.
    return cursor.fetchall()


def analyze_and_format_internal_users_data(**kwargs) -> Any:
    # Check if the input dictionary has all the necessary keys.
    try:
        internal_users_data = kwargs["internal_users_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the internal users data.
    internal_users = []
    total_items_count = 0
    if internal_users_data is not None:
        for index, record in enumerate(internal_users_data):
            internal_user, gender, role, organization = {}, {}, {}, {}
            for key, value in record.items():
                if key.startswith("gender_"):
                    gender[utils.camel_case(key)] = value
                elif key.startswith("role_"):
                    role[utils.camel_case(key)] = value
                elif key.startswith("organization_"):
                    organization[utils.camel_case(key)] = value
                elif key == "total_number_of_users":
                    break
                else:
                    internal_user[utils.camel_case(key)] = value
            internal_user["gender"] = gender
            internal_user["role"] = role
            internal_user["organization"] = organization
            internal_users.append(internal_user)
            if index == 0:
                total_items_count = record["total_items_count"]

    # Return the internal users and the total count of items.
    return internal_users, total_items_count


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
    root_organization_id = input_arguments["root_organization_id"]
    items_count_per_page = input_arguments["items_count_per_page"]
    current_page_number = input_arguments["current_page_number"]

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Get a list of internal users who have interacted with the company.
    internal_users_data = get_internal_users_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "root_organization_id": root_organization_id,
            "limit": items_count_per_page,
            "offset": (current_page_number - 1) * items_count_per_page
        }
    )

    # Define variables that stores formatted information.
    internal_users, total_items_count = analyze_and_format_internal_users_data(internal_users_data=internal_users_data)

    # Return the full information about the internal users as the response.
    return {
        "internalUsers": internal_users,
        "pageInformation": {
            "currentPageNumber": current_page_number,
            "itemsCountPerPage": items_count_per_page,
            "totalItemsCount": total_items_count
        }
    }
