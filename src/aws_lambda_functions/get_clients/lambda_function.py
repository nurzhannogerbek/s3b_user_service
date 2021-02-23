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
def get_clients_data(**kwargs) -> List[Dict[AnyStr, Any]]:
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

    # Prepare the SQL request that returns the list of customers who have interacted with the company.
    sql_statement = """
    select
        count(*) over() as total_items_count,
        aggregated_data.*
    from (
        select
            distinct users.user_id::text,
            users.user_nickname::text,
            users.user_profile_photo_url::text,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then 'identified_user'::text
                else 'unidentified_user'::text
            end as user_type,
            users.entry_created_date_time::text as created_date_time,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.identified_user_first_name::text
                else null
            end as user_first_name,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.identified_user_last_name::text
                else null
            end as user_last_name,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.identified_user_middle_name::text
                else null
            end as user_middle_name,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.identified_user_primary_email::text
                else null
            end as user_primary_email,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.identified_user_secondary_email::text[]
                else null
            end as user_secondary_email,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.identified_user_primary_phone_number::text
                else null
            end as user_primary_phone_number,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.identified_user_secondary_phone_number::text[]
                else null
            end as user_secondary_phone_number,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.metadata::text
                else unidentified_users.metadata::text
            end as metadata,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.telegram_username::text
                else null
            end as telegram_username,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.whatsapp_profile::text
                else null
            end as whatsapp_profile,
            case
                when users.identified_user_id is not null and users.unidentified_user_id is null
                then identified_users.whatsapp_username::text
                else null
            end as whatsapp_username,
            genders.gender_id::text,
            genders.gender_technical_name::text,
            genders.gender_public_name::text
        from
            chat_rooms_users_relationship
        left join users on
            chat_rooms_users_relationship.user_id = users.user_id
        left join identified_users on
            users.identified_user_id = identified_users.identified_user_id
        left join unidentified_users on
            users.unidentified_user_id = unidentified_users.unidentified_user_id
        left join genders on
            identified_users.gender_id = genders.gender_id
        where
            users.entry_deleted_date_time is null
        and
            users.internal_user_id is null
        and 
            (users.unidentified_user_id is not null or users.identified_user_id is not null)
        and
            chat_rooms_users_relationship.chat_room_id in (
                select
                    chat_rooms.chat_room_id
                from
                    chat_rooms
                where
                    chat_rooms.channel_id in (
                        select
                            channels_organizations_relationship.channel_id
                        from
                            channels_organizations_relationship
                        left join organizations on
                            channels_organizations_relationship.organization_id = organizations.organization_id
                        where
                            organizations.organization_id = %(root_organization_id)s
                        or
                            organizations.root_organization_id = %(root_organization_id)s
                    )
            )
        order by
            users.user_id::text
    ) as aggregated_data offset %(offset)s limit %(limit)s;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the list of customers who have interacted with the company.
    return cursor.fetchall()


def analyze_and_format_clients_data(**kwargs) -> Any:
    # Check if the input dictionary has all the necessary keys.
    try:
        clients_data = kwargs["clients_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the clients data.
    clients = []
    total_items_count = 0
    if clients_data is not None:
        for index, record in enumerate(clients_data):
            client, gender = {}, {}
            for key, value in record.items():
                if key.startswith("gender_"):
                    gender[utils.camel_case(key)] = value
                elif key == "total_number_of_users":
                    break
                else:
                    client[utils.camel_case(key)] = value
            client["gender"] = gender
            clients.append(client)
            if index == 0:
                total_items_count = record["total_number_of_users"]

    # Return the clients and the total count of items.
    return clients, total_items_count


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

    # Get a list of customers who have interacted with the company.
    clients_data = get_clients_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "root_organization_id": root_organization_id,
            "limit": items_count_per_page,
            "offset": (current_page_number - 1) * items_count_per_page
        }
    )

    # Define variables that stores formatted information.
    clients, total_items_count = analyze_and_format_clients_data(clients_data=clients_data)

    # Return the full information about the clients as the response.
    return {
        "clients": clients,
        "pageInformation": {
            "currentPageNumber": current_page_number,
            "itemsCountPerPage": items_count_per_page,
            "totalItemsCount": total_items_count
        }
    }
