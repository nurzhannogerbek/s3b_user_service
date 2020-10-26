import databases
import utils
import sys
import logging
from psycopg2.extras import RealDictCursor

"""
Define the connection to the database outside of the "lambda_handler" function.
The connection to the database will be created the first time the function is called.
Any subsequent function call will use the same database connection.
"""
postgresql_connection = None

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
            postgresql_connection = databases.create_postgresql_connection()
        except Exception as error:
            logger.error(error)
            sys.exit(1)

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
    try:
        country_id = event["arguments"]["input"]["countryId"]
    except KeyError:
        country_id = None
    metadata = event["arguments"]["input"]["metadata"]

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that updates information of the specific identified user.
    statement = """
    update
        identified_users
    set
        identified_user_first_name = {0},
        identified_user_last_name = {1},
        identified_user_middle_name = {2},
        identified_user_primary_email = {3},
        identified_user_secondary_email = {4},
        identified_user_primary_phone_number = {5},
        identified_user_secondary_phone_number = {6},
        identified_user_profile_photo_url = {7},
        gender_id = {8},
        country_id = {9},
        metadata = {10}
    where
        identified_user_id = (
            select
                identified_user_id
            from
                users
            where
                user_id = {11}
            and
                identified_user_id is not null
            limit 1
        );
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
        'null' if country_id is None or len(country_id) == 0
        else "'{0}'".format(country_id),
        "'{0}'".format(metadata.replace("'", "''")),
        "'{0}'".format(user_id)
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Prepare the SQL request that returns information of the user.
    statement = """
    select
        users.user_id,
        identified_users.identified_user_first_name as user_first_name,
        identified_users.identified_user_last_name as user_last_name,
        identified_users.identified_user_middle_name as user_middle_name,
        identified_users.identified_user_primary_email as user_primary_email,
        identified_users.identified_user_secondary_email as user_secondary_email,
        identified_users.identified_user_primary_phone_number as user_primary_phone_number,
        identified_users.identified_user_secondary_phone_number as user_secondary_phone_number,
        identified_users.identified_user_profile_photo_url as user_profile_photo_url,
        genders.gender_id,
        genders.gender_technical_name,
        genders.gender_public_name,
        countries.country_id,
        countries.country_short_name,
        countries.country_official_name,
        countries.country_alpha_2_code,
        countries.country_alpha_3_code,
        countries.country_numeric_code,
        countries.country_code_top_level_domain,
        identified_users.metadata::text
    from
        users
    left join identified_users on
        users.identified_user_id = identified_users.identified_user_id
    left join genders on
        identified_users.gender_id = genders.gender_id
    left join countries on
        identified_users.country_id = countries.country_id
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
    identified_user_entry = cursor.fetchone()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about identified user received from the database.
    identified_user = dict()
    if identified_user_entry is not None:
        gender = dict()
        country = dict()
        for key, value in identified_user_entry.items():
            if "_id" in key and value is not None:
                value = str(value)
            if "gender_" in key:
                gender[utils.camel_case(key)] = value
            elif "country_" in key:
                country[utils.camel_case(key)] = value
            else:
                identified_user[utils.camel_case(key)] = value
        identified_user["gender"] = gender
        identified_user["country"] = country

    # Return the full information of the new created user as the response.
    return identified_user
