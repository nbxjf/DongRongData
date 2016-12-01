import mysql.connector.pooling
import logging
import ConfigParser
import io
import json

cnxpool = None


def init_pool(filename):
    global cnxpool
    try:
        config_parser = ConfigParser.ConfigParser()
        config_parser.readfp(io.open(filename, encoding="utf8"))
        dbconfig = json.loads(config_parser.get("default", "dbconfig"))
        cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=3, **dbconfig)
    except Exception, e:
        logging.getLogger("Persist").exception("Exception occurs when connecting to database")
        logging.getLogger("Persist").exception(e)
        raise e


def fetchone(stmt, data=()):
    cnx, cursor = None, None
    try:
        cnx = cnxpool.get_connection()
        cursor = cnx.cursor()
        cursor.execute(stmt, data)
        result = dict(zip(cursor.column_names, cursor.fetchone()))
        return result
    except Exception, exception:
        logging.getLogger("Persist").exception(msg="Exception occurs when fetching data")
        logging.getLogger("Persist").exception(exception)
        raise exception
    finally:
        cnx.commit()
        cursor.close()
        cnx.close()


def fetchmany(stmt, data=()):
    cnx, cursor = None, None
    try:
        cnx = cnxpool.get_connection()
        cursor = cnx.cursor()
        cursor.execute(stmt, data)
        fetches = cursor.fetchmany(size=1000)
        result = []
        for m in fetches:
            result.append(dict(zip(cursor.column_names, m)))
        return result
    except Exception, exception:
        logging.getLogger("Persist").exception(msg="Exception occurs when fetching data")
        logging.getLogger("Persist").exception(exception)
        raise exception
    finally:
        cnx.commit()
        cursor.close()
        cnx.close()


def execute(stmt, data=()):
    cnx, cursor = None, None
    try:
        cnx = cnxpool.get_connection()
        cursor = cnx.cursor()
        cursor.execute(stmt, data)
    except Exception, exception:
        logging.getLogger("Persist").exception(msg="Exception occurs when executing statements")
        logging.getLogger("Persist").exception(exception)
        raise exception
    finally:
        cnx.commit()
        cursor.close()
        cnx.close()


def executemany(stmt, data=()):
    cnx, cursor = None, None
    try:
        cnx = cnxpool.get_connection()
        cursor = cnx.cursor()
        cursor.executemany(stmt, data)
    except Exception, exception:
        logging.getLogger("Persist").exception(msg="Exception occurs when executing statements")
        logging.getLogger("Persist").exception(exception)
        raise exception
    finally:
        cnx.commit()
        cursor.close()
        cnx.close()


if __name__ == "__main__":
    init_pool()
