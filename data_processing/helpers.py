import csv
import os
import time
from typing import List
from datetime import timezone
import datetime


def get_filename(file: str) -> str:
    """
    A method to get the filename (without the extension) of a given file. E.g. if file == 'dir/text.txt', the returned
    value is 'text'.
    :param file: the path of the file.
    :return: the file's name (without the extension) as a string .
    """
    if type(file) != str:
        raise TypeError("The file should be of type string.")
    file = file.split('/')[-1]
    return os.path.splitext(file)[0]


def get_extension(file: str) -> str:
    """
    A method to get the extension of a given file. E.g. if file == 'text.txt', the returned value is '.txt'
    :param file: the name of the file.
    :return: the file's extension as a string .
    """
    if type(file) != str:
        raise TypeError("The file should be of type string.")
    return os.path.splitext(file)[1]


def get_files_with_extension(dir_path: str, extension: str) -> List[str]:
    """
    A method to get all the files in a given directory with the given extension.
    :param dir_path: the path to the targeted directory.
    :param extension: the desired extension of the files.
    :return: a list of strings containing the global paths of the files in the given directory which match the extension.
    """
    csv_files = [file for file in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, file))]
    csv_files = [os.path.join(dir_path, file) for file in csv_files if get_extension(file) == extension]
    return csv_files


def get_rows_csv(file_path: str) -> List[List[str]]:
    """
    A method to get the contents of a csv file.
    :param file_path: the path to the targeted csv file.
    :return: a list of the file's rows in order (including the headers). Each row is a list itself of the row items.
    """
    extension = get_extension(file_path)
    if extension != '.csv':
        raise Exception(f"The file's extension is {extension}, while it must be .csv")
    csv_file = open(file_path)
    csv_reader = csv.reader(csv_file)
    rows = []
    for row in csv_reader:
        rows.append(row)
    csv_file.close()
    return rows


def csv_to_dict(csv_file_path: str, primary_key) -> dict:
    """
    A method to convert a csv file (given its path) to a dictionary with a primary key. For example, if the dictionary
    has the following columns [[user_id, age, type_work], [10, 20, sailor], [82, 30, teacher], [90, 25, developer]] and
    primary_key == 'user_id', the returned dictionary will contain the value of user_id as a key and other columns as a
    values, i.e. the returned value = {'10': {'age': '20', 'type_work': 'sailor'}, '82': {'age': '30',
    'type_work': 'teacher'}, '90': {'age': 25, 'type_work': 'developer'}}.
    :param csv_file_path: the path of the targeted csv file.
    :param primary_key: the primary key in the csv file as a string.
    :return: a dictionary in the format previously explained.
    """
    data = {}
    with open(csv_file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            key = row.get(primary_key, None)
            if key is None:
                print(f'The row {row} does not contain the primary key {primary_key}.')
                continue
            row_filtered = {col.strip(): val.strip() for col, val in row.items() if col.strip() != primary_key}
            data[key] = row_filtered
    return data


def get_ms_age(birth_date_ms: float) -> float:
    """
    A method to get the age of somebody in milliseconds timestamp given their birth date as a milliseconds timestamp.
    :param birth_date_ms: the birth date in milliseconds timestamp.
    :return: the age of that person in milliseconds timestamp.
    """
    return time.time() * 1000 - birth_date_ms


def years_to_ms(years: float) -> float:
    """
    A method to convert the number of years to the number of milliseconds. Approximately a year has 31556952000
    milliseconds.
    :param years: the number of years.
    :return: the equivalent period as milliseconds.
    """
    assert float(years) >= 0
    return 31556952000 * years
