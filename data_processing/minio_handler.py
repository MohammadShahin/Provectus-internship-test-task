import io
from typing import List

from data_processing.helpers import get_extension
from minio import Minio


def is_file_exist_minio(minio_client: Minio, minio_bucket: str, minio_object: str) -> bool:
    """
    a helper method to check if an object exist in a MinIO bucket.
    :param minio_client: the MinIO client which reads the data.
    :param minio_bucket: the MinIO bucket name.
    :param minio_object: the MinIO object name
    :return: a boolean value representing the object exists or not.
    """
    response = None
    try:
        response = minio_client.get_object(minio_bucket, minio_object)
    except:
        if response is not None:
            response.close()
            response.release_conn()
        return False
    response.close()
    response.release_conn()
    return True


def read_file_object_minio(minio_client: Minio, minio_bucket: str, minio_object: str) -> str:
    """
    A helper method to read the contents of an object in MinIO and return them as a string.
    :param minio_client: the MinIO client which reads the data.
    :param minio_bucket: the MinIO bucket name.
    :param minio_object: the MinIO object name
    :return: if the object exists, the returned value is the contents of the object as a string. Otherwise it is an
    empty string.
    """
    response = None
    try:
        response = minio_client.get_object(minio_bucket, minio_object)
    except:
        if response is not None:
            response.close()
            response.release_conn()
        return ''
    res = response.data.decode()
    response.close()
    response.release_conn()
    return res


def get_files_with_extension_minio(minio_client: Minio, bucket: str, extension: str) -> List[str]:
    """
    A method to get all the files in a given bucket with the given extension.
    :param minio_client: the MinIO client which reads the data.
    :param bucket: the targeted bucket.
    :param extension: the desired extension of the files.
    :return: a list of strings containing the object names of the files in the given bucket which match the extension.
    """
    try:
        all_files = list(minio_client.list_objects(bucket))
    except:
        return []
    files_with_extension = []
    for file in all_files:
        if get_extension(file.object_name) == extension:
            files_with_extension.append(file.object_name)
    return files_with_extension


def create_bucket_minio(minio_client: Minio, bucket: str) -> None:
    """
    a helper method for creating a bucket in MinIO given the bucket name and the client.
    :param minio_client: the MinIO client which creates the bucket.
    :param bucket: the name of the new bucket.
    :return: None.
    """
    try:
        minio_client.make_bucket(bucket)
    except:
        pass


def create_file_minio(minio_client: Minio, bucket: str, file_name: str, file_contents) -> None:
    """
    a helper method for creating an object (file) in MinIO.
    :param minio_client: the MinIO client which creates the file.
    :param bucket: the name of the bucket where the file needs to be created.
    :param file_name: the new file's name.
    :param file_contents: the contents of the file.
    :return: None.
    """
    try:
        minio_client.put_object(bucket, file_name, io.BytesIO(file_contents.encode()), len(file_contents))
    except:
        pass


def get_rows_csv_minio(minio_client: Minio, bucket: str, file_name: str) -> List[List[str]]:
    """
    A method to get the contents of a csv file in MinIO.
    :param minio_client: the MinIO client which reads the data.
    :param bucket: the bucket where the file object is located.
    :param file_name: the name of the object/file.
    :return: a list of the file's rows in order (including the headers). Each row is a list itself of the row items.
    """
    file_lines = read_file_object_minio(minio_client, bucket, file_name).split('\n')
    rows = []
    for line in file_lines:
        if line == '':
            continue
        temp = line.split(',')
        for i in range(len(temp)):
            temp[i] = temp[i].strip()
        rows.append(temp)
    return rows


# if __name__ == '__main__':
#     minio = Minio('localhost:9000', access_key='minio-access-key', secret_key='minio-secret-key', secure=False)
#     create_bucket_minio(minio, 'srcdata')
#     create_bucket_minio(minio, 'processeddata')
    # try:
    #     create_bucket_minio(minio, 'newbucket')
    # except:
    #     pass
    # create_file_minio(minio, 'newbucket', 'newfile.txt', 'bla bla \n')
    # read_file_object_minio(minio, 'newbucket', 'newfile.txt')
