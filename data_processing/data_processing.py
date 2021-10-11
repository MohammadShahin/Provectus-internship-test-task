import warnings
from typing import Tuple
import pandas as pd
from data_processing.helpers import *


SRC_DATA_PATH = '/home/mohammad/internship/src_data/'
PROCESSED_DATA_PATH = '/home/mohammad/internship/processed_data/'
OUTPUT_FILE_PATH = PROCESSED_DATA_PATH + 'output.csv'
ORG_HEADERS = ['first_name', 'last_name', 'birthts']
ORG_HEADERS_CONDITIONS = [lambda x: x.strip() != '', lambda x: x.strip() != '', lambda x: x != '' and int(x)]
OUTPUT_HEADERS = ['user_id', 'first_name', 'last_name', 'birthts', 'img_path']
IMG_EXTENSION = '.png'


def is_valid_headers_src(headers: List[str]) -> bool:
    """
    A method to check if given headers match ORG_HEADERS. Additional spaces in the beginning and ending of each column
    are discarded.
    :param headers: the headers which need to be checked.
    :return: a boolean representing if the headers are valid.
    """
    strip_headers = [header.strip() for header in headers]
    return strip_headers == ORG_HEADERS


def check_values_types_src(values: List[str]) -> Tuple[bool, str]:
    """
    A method to check if the values' row in the original directory (src) is valid, i.e. it matches the conditions and
    patterns in ORG_HEADERS_CONDITIONS.
    :param values: a list of strings, representing the values in some original csv file.
    :return: a tuple of two elements. The first is
    """
    assert len(values) == len(ORG_HEADERS)
    for i in range(len(values)):
        try:
            assert ORG_HEADERS_CONDITIONS[i](values[i])
        except Exception:
            return False, f"The value {values[i]} does not follow {ORG_HEADERS[i]}'s condition"
    return True, ''


def check_csv_rows_src(file_rows: List[List[str]]) -> Tuple[bool, str]:
    """
    A method to check if a given csv file's rows are valid in the sense the file has exactly two rows, and the they
    follow the patterns and types in ORG_HEADERS, ORG_HEADERS_CONDITIONS.
    :param file_rows: The rows in the csv file, including the headers.
    :return: A tuple of two elements; the first indicates whether the csv file is valid, and the other represents why it
    is invalid if the first element is False, and empty string otherwise.
    """
    if len(file_rows) != 2:
        return False, f"The file contains {len(file_rows)} rows. It should contain exactly two rows."
    headers, values = file_rows[0], file_rows[1]
    if not is_valid_headers_src(headers):
        return False, f"The headers {headers} do not match the required headers {ORG_HEADERS}."
    if len(values) != len(headers):
        return False, f"The number of elements in the values row does not match the header."
    values_types_is_valid, msg = check_values_types_src(values)
    if not values_types_is_valid:
        return False, msg
    return True, ''


def create_output() -> None:
    """
    A method to create the output csv in OUTPUT_FILE_PATH file in case it does not exist. And if it does exist, then it
    clears all its contents, and all the columns' labels row.
    :return: None
    """
    if os.path.isfile(OUTPUT_FILE_PATH):
        f = open(OUTPUT_FILE_PATH, "w+")
        f.close()
    df = pd.DataFrame(columns=OUTPUT_HEADERS)
    df.to_csv(OUTPUT_FILE_PATH, index=False)


def update_output_existing_row(user_id: str, row_index: int, org_values: List[Tuple[str, str]], img_path: str) -> Tuple[bool, str]:
    """
    This method is to update an existing row in the output csv file. Given the row index, it updates the original (src)
    values and also updates the image path.
    :param user_id: the user id as a string.
    :param row_index: the row index in the csv file to be updated.
    :param org_values: the new values which were in the original (src) file. It a list of tuples. Each tuple contains
    exactly two elements. where the first element is the column name, while the second element is the corresponding
    value.
    :param img_path: the new image path.
    :return: a Tuple of two elements, the first is a boolean representing if the file was updated. The second is for the
    error message.
    """
    try:
        df = pd.read_csv(OUTPUT_FILE_PATH)
        assert str(df.loc[row_index, OUTPUT_HEADERS[0]]).strip() == user_id
        for column, value in org_values:
            df.loc[row_index, column] = value
        df.loc[row_index, OUTPUT_HEADERS[-1]] = img_path
        df.to_csv(OUTPUT_FILE_PATH, index=False)
    except Exception as e:
        return False, str(e)
    return True, f'Updated the existing row for {user_id}.'


def update_output_new_row(user_id: str, org_values: List[Tuple[str, str]], img_path: str) -> Tuple[bool, str]:
    """
    A method to add a new row in the output file.
    :param user_id: the user id as a string.
    :param org_values: the new values which were in the original (src) file. It's a list of tuples. Each tuple contains
    exactly two elements. where the first element is the column name, while the second element is the corresponding
    value.
    :param img_path: the new image path.
    :return: a Tuple of two elements, the first is a boolean representing if the file was updated. The second is for the
    error message.
    """
    try:
        with open(OUTPUT_FILE_PATH, 'a') as f_object:
            writer = csv.writer(f_object)
            new_row = [user_id] + [org_value[1] for org_value in org_values] + [img_path]
            writer.writerow(new_row)
            f_object.close()
    except Exception as e:
        return False, str(e)
    return True, f'Added a new row for {user_id}'


def update_output(user_id: str, org_values: List[Tuple[str, str]], img_path: str) -> Tuple[bool, str]:
    """
    A method to update the output file given its rows by either updating an existing row or by adding a new row.
    :param user_id: the user id as a string.
    :param org_values: the new values which were in the original (src) file. It's a list of tuples. Each tuple contains
    exactly two elements. where the first element is the column name, while the second element is the corresponding
    value.
    :param img_path: the new image path.
    :return: a Tuple of two elements, the first is a boolean representing if the file was updated. The second is for the
    error message.
    """
    file_rows = get_rows_csv(OUTPUT_FILE_PATH)
    for row in range(1, len(file_rows)):
        if user_id == file_rows[row][0].strip():
            return update_output_existing_row(user_id, row - 1, org_values, img_path)
    else:
        return update_output_new_row(user_id, org_values, img_path)


def proc_csv_file(csv_file: str) -> Tuple[bool, str]:
    """
    The main method to process some csv file. It checks if the file is valid, then updates the output file with the info
    obtained from the csv file.
    :param csv_file: the path to the targeted csv file.
    :return: a Tuple of two elements, the first is a boolean representing if the file was processed. The second is for
    the error message.
    """
    assert get_extension(csv_file) == '.csv'
    file_rows = get_rows_csv(csv_file)
    csv_is_valid, msg = check_csv_rows_src(file_rows)
    if not csv_is_valid:
        return False, msg
    user_id = get_filename(csv_file)
    img_path = SRC_DATA_PATH + user_id + IMG_EXTENSION
    if not os.path.isfile(img_path):
        warn_msg = f'Could not find an image for the user with id {user_id}.'
        warnings.warn(warn_msg)
        img_path = ''
    _, values = file_rows
    org_values = [(column, value) for column, value in zip(ORG_HEADERS, values)]
    return update_output(user_id, org_values, img_path)


def process_all_data(with_print: bool = False) -> Tuple[int, int]:
    """
    The main method for running the whole data processing from src to output.csv.
    :return: None.
    """
    create_output()
    src_csv_files = get_files_with_extension(SRC_DATA_PATH, '.csv')
    success = 0
    for csv_file in src_csv_files:
        processed, msg = proc_csv_file(csv_file)
        if processed:
            success += 1
            if with_print:
                print(f"The file {csv_file} was successfully processed.", msg)
        else:
            if with_print:
                print(f"The file {csv_file} was not processed. The following error occurred: {msg}")
    return success, len(src_csv_files)
