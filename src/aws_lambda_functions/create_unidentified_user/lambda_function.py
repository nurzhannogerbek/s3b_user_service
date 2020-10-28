import databases
import utils
import logging
import sys
import os
from psycopg2.extras import RealDictCursor

"""
Define the connection to the database outside of the "lambda_handler" function.
The connection to the database will be created the first time the function is called.
Any subsequent function call will use the same database connection.
"""
postgresql_connection = None

# Define databases settings parameters.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]

logger = logging.getLogger(__name__)  # Create the logger with the specified name.
logger.setLevel(logging.WARNING)  # Set the logging level of the logger.


def lambda_handler(event, context):
    """
    :argument event: The AWS Lambda uses this parameter to pass in event data to the handler.
    :argument context: The AWS Lambda uses this parameter to provide runtime information to your handler.
    """
    # Since the connection with the database were defined outside of the function, we create global variable.
    global postgresql_connection
    if not postgresql_connection:
        try:
            postgresql_connection = databases.create_postgresql_connection(
                POSTGRESQL_USERNAME,
                POSTGRESQL_PASSWORD,
                POSTGRESQL_HOST,
                POSTGRESQL_PORT,
                POSTGRESQL_DB_NAME
            )
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # Define the values of the data passed to the function.
    metadata = event["arguments"]["input"]["metadata"]

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that creates the new unidentified user.
    statement = """
    insert into unidentified_users (
        metadata
    ) values (
        {0}
    ) returning
        unidentified_user_id;
    """.format("'{0}'".format(metadata.replace("'", "''")))

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Define the id of the new unidentified user.
    unidentified_user_id = cursor.fetchone()["unidentified_user_id"]

    # Prepare the SQL request that creates the new user.
    statement = """
    insert into users (
        unidentified_user_id
    ) values (
        {0}
    ) returning
        user_id;
    """.format("'{0}'".format(unidentified_user_id))

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Define the id of the new user.
    user_id = cursor.fetchone()["user_id"]

    # Prepare the SQL request that returns information about new created unidentified user.
    statement = """
    select
        users.user_id,
        unidentified_users.metadata::text
    from
        users
    left join unidentified_users on
        users.unidentified_user_id = unidentified_users.unidentified_user_id
    where
        users.user_id = '{0}'
    limit 1;
    """.format(user_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Fetch the next row of a query result set.
    unidentified_user_entry = cursor.fetchone()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about unidentified user received from the database.
    unidentified_user = dict()
    if unidentified_user_entry is not None:
        for key, value in unidentified_user_entry.items():
            if "_id" in key and value is not None:
                value = str(value)
            unidentified_user[utils.camel_case(key)] = value

    # Return the full information of the new created user as the response.
    return unidentified_user
