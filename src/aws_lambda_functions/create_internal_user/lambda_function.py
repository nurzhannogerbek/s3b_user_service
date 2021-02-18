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
import requests

# Configure the logging tool in the AWS Lambda function.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize constants with parameters to configure.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]
AUTH0_DOMAIN = os.environ["AUTH0_DOMAIN"]
AUTH0_CLIENT_ID = os.environ["AUTH0_CLIENT_ID"]
AUTH0_CLIENT_SECRET = os.environ["AUTH0_CLIENT_SECRET"]

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
    required_arguments = ["userPrimaryEmail", "password"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(argument_name))
        if argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(argument_name))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "auth0_user_id": input_arguments.get("auth0UserId", None),
            "auth0_metadata": json.dumps(input_arguments.get("auth0Metadata", None)),
            "user_profile_photo_url": input_arguments.get("userProfilePhotoUrl", None),
            "internal_user_first_name": input_arguments.get("userFirstName", None),
            "internal_user_last_name": input_arguments.get("userLastName", None),
            "internal_user_middle_name": input_arguments.get("userMiddleName", None),
            "internal_user_primary_email": input_arguments["userPrimaryEmail"],
            "internal_user_secondary_email": input_arguments.get("userSecondaryEmail", None),
            "internal_user_primary_phone_number": input_arguments.get("userPrimaryPhoneNumber", None),
            "internal_user_secondary_phone_number": input_arguments.get("userSecondaryPhoneNumber", None),
            "internal_user_position_name": input_arguments.get("userPositionName", None),
            "gender_id": input_arguments.get("genderId", None),
            "role_id": input_arguments.get("roleId", None),
            "organization_id": input_arguments.get("organizationId", None),
            "password": input_arguments["password"]
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


def get_access_token_from_auth0(**kwargs) -> None:
    # Make sure that all the necessary arguments for the AWS Lambda function are present.
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    request_url = "{0}/oauth/token".format(AUTH0_DOMAIN)

    # Define the headers.
    headers = {
        "Content-Type": "application/json"
    }

    # Define the JSON object body of the POST request.
    data = {
        "client_id": AUTH0_CLIENT_ID,
        "client_secret": AUTH0_CLIENT_SECRET,
        "audience": "{0}/api/v2/".format(AUTH0_DOMAIN),
        "grant_type": "client_credentials"
    }

    # Execute the POST request.
    try:
        response = requests.post(request_url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Put the result of the function in the queue.
    queue.put({
        "access_token": response.json().get("access_token", None)
    })

    # Return nothing.
    return None


def create_user_in_auth0(**kwargs) -> Any:
    # Make sure that all the necessary arguments for the AWS Lambda function are present.
    try:
        access_token = kwargs["access_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        internal_user_first_name = kwargs["internal_user_first_name"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        internal_user_last_name = kwargs["internal_user_last_name"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        internal_user_primary_email = kwargs["internal_user_primary_email"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        password = kwargs["password"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    request_url = "{0}/api/v2/users".format(AUTH0_DOMAIN)

    # Define the headers.
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {0}".format(access_token)
    }

    # Define the JSON object body of the POST request.
    data = {
        "connection": "Username-Password-Authentication",
        "email": internal_user_primary_email,
        "email_verified": False,
        "verify_email": False,
        "blocked": False,
        "password": password
    }

    # Check the first and last name of the internal user.
    if internal_user_first_name is not None:
        data["given_name"] = internal_user_first_name
    if internal_user_last_name is not None:
        data["family_name"] = internal_user_last_name

    # Execute the POST request.
    try:
        response = requests.post(request_url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the Auth0 metadata about the new created user.
    return response.json()


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
def create_internal_user(**kwargs) -> AnyStr:
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

    # Prepare the SQL request that creates the new created internal user.
    sql_statement = """
    insert into internal_users (
        internal_user_first_name,
        internal_user_last_name,
        internal_user_middle_name,
        internal_user_primary_email,
        internal_user_secondary_email,
        internal_user_primary_phone_number,
        internal_user_secondary_phone_number,
        gender_id,
        internal_user_position_name,
        role_id,
        organization_id,
        auth0_user_id,
        auth0_metadata
    ) values (
        %(internal_user_first_name)s,
        %(internal_user_last_name)s,
        %(internal_user_middle_name)s,
        %(internal_user_primary_email)s,
        %(internal_user_secondary_email)s,
        %(internal_user_primary_phone_number)s,
        %(internal_user_secondary_phone_number)s,
        %(gender_id)s,
        %(internal_user_position_name)s,
        %(role_id)s,
        %(organization_id)s,
        %(auth0_user_id)s,
        %(auth0_metadata)s
    ) returning
        internal_user_id::text;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Define the id of the new created internal user.
    try:
        internal_user_id = cursor.fetchone()["internal_user_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Add the id of the new created internal user to the map of the sql arguments.
    sql_arguments["internal_user_id"] = internal_user_id

    # Prepare the SQL request that creates the new created user.
    sql_statement = """
    insert into users (
        internal_user_id
    ) values (
        %(internal_user_id)s
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
def get_internal_user_data(**kwargs) -> Any:
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

    # Prepare the SQL request that return information about new created internal user.
    sql_statement = """
    select
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
        users.user_id = %(user_id)s
    and
        users.internal_user_id is not null
    limit 1;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the information of the new created internal user.
    return cursor.fetchone()


def analyze_and_format_internal_user_data(**kwargs) -> Any:
    # Check if the input dictionary has all the necessary keys.
    try:
        internal_user_data = kwargs["internal_user_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the internal user data.
    internal_user = {}
    if internal_user_data is not None:
        gender, role, organization = {}, {}, {}
        for key, value in internal_user_data.items():
            if key.startswith("gender_"):
                gender[utils.camel_case(key)] = value
            elif key.startswith("role_"):
                role[utils.camel_case(key)] = value
            elif "organization_" in key:
                organization[utils.camel_case(key)] = value
            else:
                internal_user[utils.camel_case(key)] = value
        internal_user["gender"] = gender
        internal_user["role"] = role
        internal_user["organization"] = organization

    # Return the information of the new created internal user.
    return internal_user


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
        },
        {
            "function_object": get_access_token_from_auth0,
            "function_arguments": {}
        }
    ])

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]

    # Define several variables that will be used in the future.
    auth0_user_id = input_arguments["auth0_user_id"]
    internal_user_first_name = input_arguments["internal_user_first_name"]
    internal_user_last_name = input_arguments["internal_user_last_name"]
    internal_user_primary_email = input_arguments["internal_user_primary_email"]
    password = input_arguments["password"]

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Define the value of the access token.
    access_token = results_of_tasks["access_token"]

    # Check the value of the user id in the Auth0.
    if auth0_user_id:
        # Create the new user in the Auth0.
        input_arguments["auth0_metadata"] = create_user_in_auth0(
            access_token=access_token,
            internal_user_first_name=internal_user_first_name,
            internal_user_last_name=internal_user_last_name,
            internal_user_primary_email=internal_user_primary_email,
            password=password
        )
        input_arguments["auth0_user_id"] = input_arguments["auth0_metadata"]

    # Create the new internal user.
    user_id = create_internal_user(postgresql_connection=postgresql_connection, sql_arguments=input_arguments)

    # Get information of the new created internal user.
    internal_user_data = get_internal_user_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "user_id": user_id
        }
    )

    # Define variables that stores formatted information about internal user.
    internal_user = analyze_and_format_internal_user_data(internal_user_data=internal_user_data)

    # Return the information of the new created internal user.
    return internal_user
