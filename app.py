from flask import Flask, request, jsonify, make_response, Response
from data_processing.main import *
from threading import Thread
from typing import Union, Callable
import yaml
from data_processing.postgres_handler import get_db_all_users, init_db, drop_users_table

docker_compose = yaml.load(open('docker-compose.yml'))

IP = '127.0.0.1'
PORT = 3001
PERIODIC_TIME = 10 * 60
app = Flask(__name__)

minio_info = {
    'endpoint': os.getenv("Minio_host", "localhost"),
    'access_key': docker_compose['services']['minio']['environment'][0].split('=')[-1],
    'secret_key': docker_compose['services']['minio']['environment'][1].split('=')[-1],
}

minio_client = Minio(
    endpoint=minio_info['endpoint'],
    access_key=minio_info['access_key'],
    secret_key=minio_info['secret_key'],
    secure=False
)

db_info = {
    'db_name': docker_compose['services']['db']['environment']['POSTGRES_DB'],
    'db_user': docker_compose['services']['db']['environment']['POSTGRES_USER'],
    'db_password': docker_compose['services']['db']['environment']['POSTGRES_PASSWORD'],
    'db_host': os.getenv("DB_host", "localhost")
}



def fix_values_data_get(is_image_exists: Union[str, None], min_age: Union[str, None], max_age: Union[str, None]) \
        -> Tuple[Union[bool, None], Union[float, None], Union[float, None]]:
    """
    A method to fix the values received in a /data get request as strings (or Nones). The None values will stay None,
    while the valid values will convert to their corresponding types. An error will occur if the types do not match
    their conditions.
    :param is_image_exists: a string representing a boolean flag to filter the data returned according to the existence
    images in their data.
    :param min_age: a string representing a float representing the minimum age of the data returned.
    :param max_age: a string representing a float representing the maximum age of the data returned.
    :return: the non-None data converted to their correct types and values.
    """
    if is_image_exists is not None:
        is_image_exists = str(is_image_exists)
        assert is_image_exists in ['True', 'False']
        is_image_exists = is_image_exists == 'True'
    if min_age is not None:
        min_age = float(min_age)
    if max_age is not None:
        max_age = float(max_age)
    return is_image_exists, min_age, max_age


def generate_conditions_get(is_image_exists: Union[bool, None], min_age: Union[float, None],
                            max_age: Union[float, None]) -> List[Callable]:
    """
    A method to generate the conditions of the returned value in the /data get request according the value of its
    parameters. The conditions are for the 'value' elements in the dictionary.
    :param is_image_exists: a boolean flag to filter the data returned according to the existence images in their data.
    :param min_age: a string representing a float representing the minimum age of the data returned.
    :param max_age: a string representing a float representing the maximum age of the data returned.
    :return: a list of conditions.
    """
    ret = []
    if is_image_exists is not None:
        ret.append(lambda arg: 'img_path' in arg.keys() and (arg['img_path'] != '') == is_image_exists)
    if min_age is not None:
        min_age_ms = years_to_ms(min_age)
        ret.append(lambda arg: 'birthts' in arg.keys() and get_ms_age(float(arg['birthts'])) >= min_age_ms)
    if max_age is not None:
        max_age_ms = years_to_ms(max_age)
        ret.append(lambda arg: 'birthts' in arg.keys() and get_ms_age(float(arg['birthts'])) <= max_age_ms)
    return ret


def handle_data_get_request(args: dict) -> Response:
    """
    A method to handle /data get requests. It includes checking if the arguments are valid, construct the proper
    conditions, and finally returns the response that meets the arguments conditions.
    :param args: a dictionary containing the arguments passed to the get request.
    :return: the proper response to the get request.
    """
    is_image_exists = args.get('is_image_exists', None)
    min_age = args.get('min_age', None)
    max_age = args.get('max_age', None)
    try:
        is_image_exists, min_age, max_age = fix_values_data_get(is_image_exists, min_age, max_age)
    except:
        return make_response(400)
    conditions = generate_conditions_get(is_image_exists, min_age, max_age)
    res_dict = get_db_all_users(db_info)
    res_dict = {key: val for key, val in res_dict.items() if all(condition(val) for condition in conditions)}
    res = make_response(jsonify(res_dict), 200)
    return res


def handle_data_post_request() -> Response:
    """
    A method to handle the /data post request. It is responsible for manually running the data processing from the src
    to the output.
    :return: a message showing how files were successfully processed.
    """
    success, all_files = process_all_data_minio(db_info, minio_client, True)
    return make_response(f"Out of {all_files} files for the users, {success} were successfully processed.", 200)


@app.route("/data", methods=['GET', 'POST'])
def handle_data_requests() -> Response:
    """
    A method for handling requests on /data. It is either used for obtaining some users' data with (possibly) some
    conditions, or manually running the data processing.
    :return: the proper response depending on the type of the request.
    """
    if request.method == 'GET':
        return handle_data_get_request(request.args.to_dict())
    if request.method == 'POST':
        return handle_data_post_request()
    else:
        return make_response("No such request is available", 404)


def periodic_update() -> None:
    """
    A method for running periodic data processing in the background.
    :return: None.
    """
    while True:
        process_all_data_minio(db_info, minio_client)
        time.sleep(PERIODIC_TIME)


if __name__ == '__main__':
    create_bucket_minio(minio_client, SRC_DATA_BUCKET)
    create_bucket_minio(minio_client, PROCESSED_DATA_BUCKET)
    init_db(db_info)
    periodic_process = Thread(target=periodic_update)
    periodic_process.start()
    app.run(host=IP, port=PORT)
