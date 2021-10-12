import psycopg2


def get_db_connection_cursor(db_info: dict):
    """
    a method to get a connection to the postgres database given its information.
    :param db_info: a dictionary containing the database info like its name host, user, password, etc.
    :return: a connection to the postgres database.
    """
    conn = psycopg2.connect(dbname=db_info['db_name'], user=db_info['db_user'],
                            password=db_info['db_password'], host=db_info['db_host'])
    cur = conn.cursor()
    return conn, cur


def init_db(db_info: dict) -> None:
    """
    a method for initiating the database. It mainly creates the users table.
    :param db_info: a dictionary containing the postgres database info.
    :return: None.
    """
    conn, cur = get_db_connection_cursor(db_info)
    command = """CREATE TABLE IF NOT EXISTS users(
                id SERIAL PRIMARY KEY NOT NULL,
                user_id varchar (50) NOT NULL,
                first_name varchar (50) NOT NULL,
                last_name varchar (50) NOT NULL,
                birthts varchar (50) NOT NULL,
                img_path varchar (250) NOT NULL
            );"""
    cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()


def update_users_row(db_info: dict, id: int, values: tuple) -> None:
    """
    A method to update an existing row in the database.
    :param db_info: a dictionary containing the postgres database info.
    :param id: the id in the users table we want
    :param values: the new values that should be in the row [user_id, first_name, last_name, birthts, img-path].
    :return: None.
    """
    conn, cur = get_db_connection_cursor(db_info)
    command = """UPDATE users SET first_name = %s, last_name = %s, birthts = %s, img_path = %s 
    WHERE id = %s;"""
    cur.execute(command, [*values[1:], id])
    conn.commit()
    cur.close()
    conn.close()


def add_users_row(db_info: dict, values: tuple):
    """
    a method for adding a new row to the users table.
    :param db_info: a dictionary containing the postgres database info.
    :param values: the new values that should be in the row [user_id, first_name, last_name, birthts, img-path].
    :return: None.
    """
    conn, cur = get_db_connection_cursor(db_info)
    command = """INSERT INTO users(user_id, first_name, last_name, birthts, img_path) 
                                   VALUES(%s, %s, %s, %s, %s);"""
    cur.execute(command, values)
    conn.commit()
    cur.close()
    conn.close()


def get_users_ids(db_info: dict):
    """
    a method to get id, user_id for all users in the users table.
    :param db_info: a dictionary containing the postgres database info.
    :return:
    """
    conn, cur = get_db_connection_cursor(db_info)
    command = """SELECT id, user_id FROM users;"""
    cur.execute(command)
    conn.commit()
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res


def update_db(db_info: dict, *values) -> None:
    """
    a method to update the database by either adding a new row to the users or updating an existing row.
    :param db_info: a dictionary containing the postgres database info.
    :param values: the new values that should be in the row [user_id, first_name, last_name, birthts, img-path].
    :return: None.
    """
    user_ids = get_users_ids(db_info)
    for id, user_id in user_ids:
        if values[0] == user_id:
            update_users_row(db_info, id, values)
            break
    else:
        add_users_row(db_info, values)


def drop_users_table(db_info: dict) -> None:
    """
    a method to drop users table from the database. This method was used for debugging.
    :param db_info: a dictionary containing the postgres database info.
    :return: None.
    """
    conn, cur = get_db_connection_cursor(db_info)
    command = """DROP TABLE IF EXISTS users;"""
    cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()


def get_db_all_users(db_info: dict) -> dict:
    """
    A method to convert the postgres database content to a dictionary with the user_id as the primary key. For example,
    the returned dictionary will contain the value of user_id as a key and other columns as the key's value, i.e. the
    returned value = {'10': {'first_name': 'ahmad', 'last_name': 'smith', 'birthts': '7685476260, 'img_path': '10.png'}}
    :param db_info: a dictionary containing the postgres database info.
    :return: a dictionary in the format previously explained.
    """
    conn, cur = get_db_connection_cursor(db_info)
    command = """SELECT * FROM users;"""
    cur.execute(command)
    conn.commit()
    res = cur.fetchall()
    cur.close()
    conn.close()
    ret_dict = {}
    for row in res:
        ret_dict[row[1]] = {
            'first_name': row[2],
            'last_name': row[3],
            'birthts': row[4],
            'img_path': row[5]
        }
    return ret_dict
