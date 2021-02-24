import logging
import os
import json
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
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
    required_arguments = ["auth0UserId"]
    modified_arguments = [
        "userFirstName",
        "userLastName",
        "userMiddleName",
        "userPrimaryEmail",
        "userSecondaryEmail",
        "userPrimaryPhoneNumber",
        "userSecondaryPhoneNumber",
        "userPositionName"
    ]
    for argument_name, argument_value in input_arguments.items():
        if argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(argument_name))
        if argument_name in modified_arguments:
            input_arguments["internal_{0}".format(utils.snake_case(argument_name))] = input_arguments.pop(argument_name)
        else:
            input_arguments[utils.camel_case(argument_name)] = input_arguments.pop(argument_name)

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": input_arguments
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


def get_access_token_from_auth0() -> AnyStr:
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

    # Return access token from the Auth0.
    return response.json().get("access_token", None)


def change_user_password_in_auth0(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        access_token = kwargs["access_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        auth0_user_id = kwargs["auth0_user_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        password = kwargs["password"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    request_url = "{0}/api/v2/users/{1}".format(AUTH0_DOMAIN, auth0_user_id)

    # Define the headers.
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {0}".format(access_token)
    }

    # Define the JSON object body of the PATCH request.
    data = {
        "password": password,
        "connection": "Username-Password-Authentication"
    }

    # Execute the PATCH request.
    try:
        response = requests.patch(request_url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


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
        internal_user_id = (
            select
                internal_user_id
            from
                internal_users
            where
                auth0_user_id = %(auth0_user_id)s
            limit 1
        );
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
def update_internal_user(**kwargs) -> None:
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
    removed_arguments = ["auth0_user_id", "password", "user_profile_photo_url"]
    sql_statement_arguments = [argument for argument in list(sql_arguments.keys()) if argument not in removed_arguments]
    if sql_statement_arguments:
        sql_statement = sql.SQL("update internal_users set {fields} where auth0_user_id = {auth0_user_id};").format(
            fields=sql.SQL(', ').join(
                sql.Composed([
                    sql.Identifier(argument_name),
                    sql.SQL(" = "),
                    sql.Placeholder(argument_name)
                ]) for argument_name in sql_statement_arguments
            ),
            auth0_user_id=sql.Placeholder("auth0_user_id")
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

    # Prepare the SQL request that return information about the internal user.
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
        internal_users.auth0_user_id = %(auth0_user_id)s
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

    # Return the information of the internal user.
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

    # Return the information of the internal user.
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
        }
    ])

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]
    auth0_user_id = input_arguments["auth0_user_id"]
    password = input_arguments.get("password", None)
    user_profile_photo_url = input_arguments.get("user_profile_photo_url", None)

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Change the user's password in the Auth0.
    if password:
        access_token = get_access_token_from_auth0()
        change_user_password_in_auth0(access_token=access_token, auth0_user_id=auth0_user_id, password=password)

    # Change the user's profile photo url.
    if user_profile_photo_url:
        update_user_profile_photo_url(
            postgresql_connection=postgresql_connection,
            sql_arguments={
                "auth0_user_id": auth0_user_id,
                "user_profile_photo_url": user_profile_photo_url
            }
        )

    # Update internal user's main information.
    update_internal_user(postgresql_connection=postgresql_connection, sql_arguments=input_arguments)

    # Get information of the internal user.
    internal_user_data = get_internal_user_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "auth0_user_id": auth0_user_id
        }
    )

    # Define variable that stores formatted information about internal user.
    internal_user = analyze_and_format_internal_user_data(internal_user_data=internal_user_data)

    # Return the information of the internal user.
    return internal_user
