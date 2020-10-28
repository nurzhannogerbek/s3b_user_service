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
    auth0_user_id = event["arguments"]["input"]["auth0UserId"]
    try:
        auth0_metadata = event["arguments"]["input"]["auth0Metadata"]
    except KeyError:
        auth0_metadata = None
    try:
        internal_user_first_name = event["arguments"]["input"]["userFirstName"]
    except KeyError:
        internal_user_first_name = None
    try:
        internal_user_last_name = event["arguments"]["input"]["userLastName"]
    except KeyError:
        internal_user_last_name = None
    try:
        internal_user_middle_name = event["arguments"]["input"]["userMiddleName"]
    except KeyError:
        internal_user_middle_name = None
    try:
        internal_user_primary_email = event["arguments"]["input"]["userPrimaryEmail"]
    except KeyError:
        internal_user_primary_email = None
    try:
        internal_user_secondary_email = event["arguments"]["input"]["userSecondaryEmail"]
    except KeyError:
        internal_user_secondary_email = None
    try:
        internal_user_primary_phone_number = event["arguments"]["input"]["userPrimaryPhoneNumber"]
    except KeyError:
        internal_user_primary_phone_number = None
    try:
        internal_user_secondary_phone_number = event["arguments"]["input"]["userSecondaryPhoneNumber"]
    except KeyError:
        internal_user_secondary_phone_number = None
    try:
        internal_user_profile_photo_url = event["arguments"]["input"]["userProfilePhotoUrl"]
    except KeyError:
        internal_user_profile_photo_url = None
    try:
        internal_user_position_name = event["arguments"]["input"]["userPositionName"]
    except KeyError:
        internal_user_position_name = None
    try:
        gender_id = event["arguments"]["input"]["genderId"]
    except KeyError:
        gender_id = None
    try:
        country_id = event["arguments"]["input"]["countryId"]
    except KeyError:
        country_id = None
    try:
        role_id = event["arguments"]["input"]["roleId"]
    except KeyError:
        role_id = None
    try:
        organization_id = event["arguments"]["input"]["organizationId"]
    except KeyError:
        organization_id = None

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that creates the new internal user.
    statement = """
    insert into internal_users (
        internal_user_first_name,
        internal_user_last_name,
        internal_user_middle_name,
        internal_user_primary_email,
        internal_user_secondary_email,
        internal_user_primary_phone_number,
        internal_user_secondary_phone_number,
        internal_user_profile_photo_url,
        internal_user_position_name,
        gender_id,
        country_id,
        role_id,
        organization_id,
        auth0_user_id,
        auth0_metadata
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
        {12},
        {13},
        {14}
    ) returning
        internal_user_id;
    """.format(
        'null' if internal_user_first_name is None or len(internal_user_first_name) == 0
        else "'{0}'".format(internal_user_first_name),
        'null' if internal_user_last_name is None or len(internal_user_last_name) == 0
        else "'{0}'".format(internal_user_last_name),
        'null' if internal_user_middle_name is None or len(internal_user_middle_name) == 0
        else "'{0}'".format(internal_user_middle_name),
        'null' if internal_user_primary_email is None or len(internal_user_primary_email) == 0
        else "'{0}'".format(internal_user_primary_email),
        'null' if internal_user_secondary_email is None or len(internal_user_secondary_email) == 0
        else "'{0}'".format(internal_user_secondary_email),
        'null' if internal_user_primary_phone_number is None or len(internal_user_primary_phone_number) == 0
        else "'{0}'".format(internal_user_primary_phone_number),
        'null' if internal_user_secondary_phone_number is None or len(internal_user_secondary_phone_number) == 0
        else "'{0}'".format(internal_user_secondary_phone_number),
        'null' if internal_user_profile_photo_url is None or len(internal_user_profile_photo_url) == 0
        else "'{0}'".format(internal_user_profile_photo_url),
        'null' if internal_user_position_name is None or len(internal_user_position_name) == 0
        else "'{0}'".format(internal_user_position_name),
        'null' if gender_id is None or len(gender_id) == 0
        else "'{0}'".format(gender_id),
        'null' if country_id is None or len(country_id) == 0
        else "'{0}'".format(country_id),
        'null' if role_id is None or len(role_id) == 0
        else "'{0}'".format(role_id),
        'null' if organization_id is None or len(organization_id) == 0
        else "'{0}'".format(organization_id),
        "'{0}'".format(auth0_user_id),
        'null' if auth0_metadata is None or len(auth0_metadata) == 0
        else "'{0}'".format(auth0_metadata.replace("'", "''")),
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Define the id of the new internal user.
    internal_user_id = cursor.fetchone()["internal_user_id"]

    # Prepare the SQL request that creates the new user.
    statement = """
    insert into users (
        internal_user_id
    ) values (
        {0}
    ) returning
        user_id;
    """.format("'{0}'".format(internal_user_id))

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Define the id of the new user.
    user_id = str(cursor.fetchone()["user_id"])

    # Prepare the SQL request that returns all detailed information of the specific internal user.
    statement = """
    select
        internal_users.auth0_user_id,
        internal_users.auth0_metadata::text,
        users.user_id,
        internal_users.internal_user_first_name as user_first_name,
        internal_users.internal_user_last_name as user_last_name,
        internal_users.internal_user_middle_name as user_middle_name,
        internal_users.internal_user_primary_email as user_primary_email,
        internal_users.internal_user_secondary_email as user_secondary_email,
        internal_users.internal_user_primary_phone_number as user_primary_phone_number,
        internal_users.internal_user_secondary_phone_number as user_secondary_phone_number,
        internal_users.internal_user_profile_photo_url as user_profile_photo_url,
        internal_users.internal_user_position_name as user_position_name,
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
        roles.role_id,
        roles.role_technical_name,
        roles.role_public_name,
        roles.role_description,
        organizations.organization_id,
        organizations.organization_name,
        organizations.organization_description,
        organizations.parent_organization_id,
        organizations.parent_organization_name,
        organizations.parent_organization_description,
        organizations.root_organization_id,
        organizations.root_organization_name,
        organizations.root_organization_description
    from
        users
    left join internal_users on
        users.internal_user_id = internal_users.internal_user_id
    left join genders on
        internal_users.gender_id = genders.gender_id
    left join countries on
        internal_users.country_id = countries.country_id
    left join roles on
        internal_users.role_id = roles.role_id
    left join organizations on
        internal_users.organization_id = organizations.organization_id
    where
        users.user_id = '{0}'
    and
        users.internal_user_id is not null
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
    internal_user_entry = cursor.fetchone()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about internal user received from the database.
    internal_user = dict()
    if internal_user_entry is not None:
        gender = dict()
        country = dict()
        role = dict()
        organization = dict()
        for key, value in internal_user_entry.items():
            if "_id" in key and value is not None:
                value = str(value)
            if "gender_" in key:
                gender[utils.camel_case(key)] = value
            elif "country_" in key:
                country[utils.camel_case(key)] = value
            elif "role_" in key:
                role[utils.camel_case(key)] = value
            elif "organization_" in key:
                organization[utils.camel_case(key)] = value
            else:
                internal_user[utils.camel_case(key)] = value
        internal_user["gender"] = gender
        internal_user["country"] = country
        internal_user["role"] = role
        internal_user["organization"] = organization

    # Return the full information about the internal user as the response.
    return internal_user
