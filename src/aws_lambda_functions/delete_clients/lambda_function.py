import databases
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
    # Since the connection with the database were defined outside of the function, we create global variables.
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
    users_ids = event["arguments"]["input"]["usersIds"]

    # Convert string array with users' ids to the string data type.
    converted_users_ids = ", ".join("'{0}'".format(user_id) for user_id in users_ids)

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that get an aggregated list of IDs by user type.
    statement = """
    select
        array_remove(array_agg(distinct identified_user_id), null)::text[] as identified_users_ids,
        array_remove(array_agg(distinct unidentified_user_id), null)::text[] as unidentified_users_ids
    from
        users
    where user_id in ({0});
    """.format(converted_users_ids)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Define the ids.
    aggregated_data = cursor.fetchone()
    try:
        identified_users_ids = aggregated_data["identified_users_ids"]
    except KeyError:
        identified_users_ids = None
    try:
        unidentified_users_ids = aggregated_data["unidentified_users_ids"]
    except KeyError:
        unidentified_users_ids = None

    print("identified_users_ids")
    print(type(identified_users_ids))
    print("unidentified_users_ids")
    print(type(unidentified_users_ids))

    if identified_users_ids is not None:
        # Convert string array with identified users' ids to the string data type.
        converted_identified_users_ids = ", ".join("'{0}'".format(user_id) for user_id in identified_users_ids)

        # Put a tag for deletion for an identified user.
        statement = """
        update
            identified_users
        set
            entry_deleted_date_time = now()
        where
            identified_user_id in ({0});
        """.format(converted_identified_users_ids)

        # Execute a previously prepared SQL query.
        try:
            cursor.execute(statement)
        except Exception as error:
            logger.error(error)
            sys.exit(1)

        # After the successful execution of the query commit your changes to the database.
        postgresql_connection.commit()

    if unidentified_users_ids is not None:
        # Convert string array with unidentified users' ids to the string data type.
        converted_unidentified_users_ids = ", ".join("'{0}'".format(user_id) for user_id in unidentified_users_ids)

        # Put a tag for deletion for an unidentified user.
        statement = """
        update
            unidentified_users
        set
            entry_deleted_date_time = now()
        where
            unidentified_user_id in ({0});
        """.format(converted_unidentified_users_ids)

        # Execute a previously prepared SQL query.
        try:
            cursor.execute(statement)
        except Exception as error:
            logger.error(error)
            sys.exit(1)

        # After the successful execution of the query commit your changes to the database.
        postgresql_connection.commit()

    if users_ids is not None:
        # Put a tag for deletion for an user.
        statement = """
        update
            users
        set
            entry_deleted_date_time = now()
        where
            user_id in ({0});
        """.format(converted_users_ids)

        # Execute a previously prepared SQL query.
        try:
            cursor.execute(statement)
        except Exception as error:
            logger.error(error)
            sys.exit(1)

        # After the successful execution of the query commit your changes to the database.
        postgresql_connection.commit()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Return the full information of the new created user as the response.
    return users_ids
