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
    # Define the values of the data passed to the function.
    user_id = event["arguments"]["input"]["userId"]
    try:
        identified_user_first_name = event["arguments"]["input"]["userFirstName"]
    except KeyError:
        identified_user_first_name = None
    try:
        identified_user_last_name = event["arguments"]["input"]["userLastName"]
    except KeyError:
        identified_user_last_name = None
    try:
        identified_user_middle_name = event["arguments"]["input"]["userMiddleName"]
    except KeyError:
        identified_user_middle_name = None
    try:
        identified_user_primary_email = event["arguments"]["input"]["userPrimaryEmail"]
    except KeyError:
        identified_user_primary_email = None
    try:
        identified_user_secondary_email = event["arguments"]["input"]["userSecondaryEmail"]
    except KeyError:
        identified_user_secondary_email = None
    try:
        identified_user_primary_phone_number = event["arguments"]["input"]["userPrimaryPhoneNumber"]
    except KeyError:
        identified_user_primary_phone_number = None
    try:
        identified_user_secondary_phone_number = event["arguments"]["input"]["userSecondaryPhoneNumber"]
    except KeyError:
        identified_user_secondary_phone_number = None
    try:
        identified_user_profile_photo_url = event["arguments"]["input"]["userProfilePhotoUrl"]
    except KeyError:
        identified_user_profile_photo_url = None
    try:
        gender_id = event["arguments"]["input"]["genderId"]
    except KeyError:
        gender_id = None
    metadata = event["arguments"]["input"]["metadata"]
    try:
        telegram_username = event["arguments"]["input"]["telegramUsername"]
    except KeyError:
        telegram_username = None
    try:
        whatsapp_profile = event["arguments"]["input"]["whatsappProfile"]
    except KeyError:
        whatsapp_profile = None
    try:
        whatsapp_username = event["arguments"]["input"]["whatsappUsername"]
    except KeyError:
        whatsapp_username = None

    # Since the connection with the database were defined outside of the function, we create the global variable.
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

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that creates the new identified user.
    statement = """
    insert into identified_users (
        identified_user_first_name,
        identified_user_last_name,
        identified_user_middle_name,
        identified_user_primary_email,
        identified_user_secondary_email,
        identified_user_primary_phone_number,
        identified_user_secondary_phone_number,
        identified_user_profile_photo_url,
        gender_id,
        metadata,
        telegram_username,
        whatsapp_profile,
        whatsapp_username
    ) values (
        {0},
        {1},
        {2},
        {3},
        {4},
        {5},
        {6},
        {7},
        {8},
        {9},
        {10},
        {11},
        {12}
    ) returning
        identified_user_id;
    """.format(
        'null' if identified_user_first_name is None or len(identified_user_first_name) == 0
        else "'{0}'".format(identified_user_first_name),
        'null' if identified_user_last_name is None or len(identified_user_last_name) == 0
        else "'{0}'".format(identified_user_last_name),
        'null' if identified_user_middle_name is None or len(identified_user_middle_name) == 0
        else "'{0}'".format(identified_user_middle_name),
        'null' if identified_user_primary_email is None or len(identified_user_primary_email) == 0
        else "'{0}'".format(identified_user_primary_email),
        'null' if identified_user_secondary_email is None or len(identified_user_secondary_email) == 0
        else "'{0}'".format(identified_user_secondary_email),
        'null' if identified_user_primary_phone_number is None or len(identified_user_primary_phone_number) == 0
        else "'{0}'".format(identified_user_primary_phone_number),
        'null' if identified_user_secondary_phone_number is None or len(identified_user_secondary_phone_number) == 0
        else "'{0}'".format(identified_user_secondary_phone_number),
        'null' if identified_user_profile_photo_url is None or len(identified_user_profile_photo_url) == 0
        else "'{0}'".format(identified_user_profile_photo_url),
        'null' if gender_id is None or len(gender_id) == 0
        else "'{0}'".format(gender_id),
        "'{0}'".format(metadata.replace("'", "''")),
        'null' if telegram_username is None or len(telegram_username) == 0
        else "'{0}'".format(telegram_username),
        'null' if whatsapp_profile is None or len(whatsapp_profile) == 0
        else "'{0}'".format(whatsapp_profile),
        'null' if whatsapp_username is None or len(whatsapp_username) == 0
        else "'{0}'".format(whatsapp_username)
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Define the id of the new identified user.
    identified_user_id = cursor.fetchone()["identified_user_id"]

    # Update the user type.
    statement = """
    update
        users x
    set
        identified_user_id = '{0}',
        unidentified_user_id = null
    from (
        select
            *
        from
            users
        where
            user_id = '{1}'
        for update
    ) y
    where
        x.user_id = y.user_id
    returning
        y.unidentified_user_id;
    """.format(
        identified_user_id,
        user_id
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Define the id of the new identified user.
    unidentified_user_id = cursor.fetchone()["unidentified_user_id"]

    # Put a tag for deletion for an unidentified user.
    statement = """
    update
        unidentified_users
    set
        entry_deleted_date_time = now()
    where
        unidentified_user_id = '{0}';
    """.format(unidentified_user_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Prepare the SQL request that returns the information of the specific client.
    statement = """
    select
        users.user_id,
        users.entry_created_date_time as created_date_time,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_first_name
            else null
        end as user_first_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_last_name
            else null
        end as user_last_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_middle_name
            else null
        end as user_middle_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_primary_email
            else null
        end as user_primary_email,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_secondary_email
            else null
        end as user_secondary_email,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_primary_phone_number
            else null
        end as user_primary_phone_number,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_secondary_phone_number
            else null
        end as user_secondary_phone_number,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_profile_photo_url
            else null
        end as user_profile_photo_url,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then 'identified_user'
            else 'unidentified_user'
        end as user_type,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.metadata::text
            else unidentified_users.metadata::text
        end as metadata,
        case
            when users.identified_user_id is not null
            and users.unidentified_user_id is null then identified_users.telegram_username
            else null
        end as telegram_username,
        case
            when users.identified_user_id is not null
            and users.unidentified_user_id is null then identified_users.whatsapp_profile
            else null
        end as whatsapp_profile,
        case
            when users.identified_user_id is not null
            and users.unidentified_user_id is null then identified_users.whatsapp_username
            else null
        end as whatsapp_username,
        genders.gender_id,
        genders.gender_technical_name,
        genders.gender_public_name
    from
        users
    left join identified_users on
        users.identified_user_id = identified_users.identified_user_id
    left join unidentified_users on
        users.unidentified_user_id = unidentified_users.unidentified_user_id
    left join genders on
        identified_users.gender_id = genders.gender_id
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

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Fetch the next row of a query result set.
    client_entry = cursor.fetchone()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about client received from the database.
    client = dict()
    if client_entry is not None:
        gender = dict()
        for key, value in client_entry.items():
            if ("_id" in key or "_date_time" in key) and value is not None:
                value = str(value)
            if "gender_" in key:
                gender[utils.camel_case(key)] = value
            else:
                client[utils.camel_case(key)] = value
        client["gender"] = gender

    # Return the list of roles as the response.
    return client
