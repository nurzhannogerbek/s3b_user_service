import databases
import utils
import requests
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
AUTH0_DOMAIN = os.environ["AUTH0_DOMAIN"]
AUTH0_CLIENT_ID = os.environ["AUTH0_CLIENT_ID"]
AUTH0_CLIENT_SECRET = os.environ["AUTH0_CLIENT_SECRET"]

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
        role_id = event["arguments"]["input"]["roleId"]
    except KeyError:
        role_id = None
    try:
        organization_id = event["arguments"]["input"]["organizationId"]
    except KeyError:
        organization_id = None
    try:
        password = event["arguments"]["input"]["password"]
    except KeyError:
        password = None

    # Change the user password in the Auth0.
    if auth0_user_id is not None and password is not None:
        access_token = get_access_token_from_auth0()
        if access_token is not None:
            change_user_password_in_auth0(access_token, auth0_user_id, password)

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that updates information of the specific internal user.
    statement = """
    update
        internal_users
    set
        internal_user_first_name = {0},
        internal_user_last_name = {1},
        internal_user_middle_name = {2},
        internal_user_primary_email = {3},
        internal_user_secondary_email = {4},
        internal_user_primary_phone_number = {5},
        internal_user_secondary_phone_number = {6},
        internal_user_profile_photo_url = {7},
        internal_user_position_name = {8},
        gender_id = {9},
        role_id = {10},
        organization_id = {11},
        auth0_metadata = {12}
    where
        auth0_user_id = {13};
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
        'null' if role_id is None or len(role_id) == 0
        else "'{0}'".format(role_id),
        'null' if organization_id is None or len(organization_id) == 0
        else "'{0}'".format(organization_id),
        'null' if auth0_metadata is None or len(auth0_metadata) == 0
        else "'{0}'".format(auth0_metadata.replace("'", "''")),
        "'{0}'".format(auth0_user_id)
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

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
    left join roles on
        internal_users.role_id = roles.role_id
    left join organizations on
        internal_users.organization_id = organizations.organization_id
    where
        internal_users.auth0_user_id = '{0}'
    and
        users.internal_user_id is not null
    limit 1;
    """.format(auth0_user_id)

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
        role = dict()
        organization = dict()
        for key, value in internal_user_entry.items():
            if "_id" in key and value is not None:
                value = str(value)
            if "gender_" in key:
                gender[utils.camel_case(key)] = value
            elif "role_" in key:
                role[utils.camel_case(key)] = value
            elif "organization_" in key:
                organization[utils.camel_case(key)] = value
            else:
                internal_user[utils.camel_case(key)] = value
        internal_user["gender"] = gender
        internal_user["role"] = role
        internal_user["organization"] = organization

    # Return the full information about the internal user as the response.
    return internal_user


def get_access_token_from_auth0():
    """
    Function name:
    get_access_token_from_auth0

    Function description:
    The main task of this function is to get access token from Auth0.
    """
    try:
        # Make the POST request to the Auth0.
        request_url = "{0}/oauth/token".format(AUTH0_DOMAIN)
        headers = {
            "Content-Type": "application/json"
        }
        payload = {
            "client_id": AUTH0_CLIENT_ID,
            "client_secret": AUTH0_CLIENT_SECRET,
            "audience": "{0}/api/v2/".format(AUTH0_DOMAIN),
            "grant_type": "client_credentials"
        }
        response = requests.post(
            request_url,
            json=payload,
            headers=headers
        )
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Determine the value of the access token.
    try:
        access_token = response.json()["access_token"]
    except KeyError:
        access_token = None

    # Return the access token.
    return access_token


def change_user_password_in_auth0(access_token, auth0_user_id, password):
    """
    Function name:
    change_user_password_in_auth0

    Function description:
    The main task of this function is to change the password of the specific user in the database of the Auth0.
    """
    try:
        # Make the POST request to the Auth0.
        request_url = "{0}/api/v2/users/{1}".format(AUTH0_DOMAIN, auth0_user_id)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {0}".format(access_token)
        }
        payload = {
            "password": password,
            "connection": "Username-Password-Authentication"
        }
        response = requests.patch(
            request_url,
            data=payload,
            headers=headers
        )
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        sys.exit(1)
    return None
