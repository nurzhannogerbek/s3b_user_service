import logging
import os
from psycopg2.extras import RealDictCursor
from functools import wraps
from typing import *
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


def reuse_or_recreate_postgresql_connection():
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
    return POSTGRESQL_CONNECTION


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
def get_roles_data(**kwargs) -> List[Dict[AnyStr, Any]]:
    # Check if the input dictionary has all the necessary keys.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the SQL request that returns the list of roles.
    sql_statement = """
    select
        role_id::text,
        role_technical_name::text,
        role_public_name::text,
        role_description::text
    from
        roles;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the list of roles.
    return cursor.fetchall()


def analyze_and_format_roles_data(**kwargs) -> Any:
    # Check if the input dictionary has all the necessary keys.
    try:
        roles_data = kwargs["roles_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the roles data.
    roles = []
    if roles_data is not None:
        for record in roles_data:
            role = {}
            for key, value in record.items():
                role[utils.camel_case(key)] = value
            roles.append(role)

    # Return the roles.
    return roles


def lambda_handler(event, context):
    # Define the instances of the database connections.
    postgresql_connection = reuse_or_recreate_postgresql_connection()

    # Get the list of roles.
    roles_data = get_roles_data(postgresql_connection=postgresql_connection)

    # Define variable that stores formatted information.
    roles = analyze_and_format_roles_data(roles_data=roles_data)

    # Return the list of roles as the response.
    return roles
