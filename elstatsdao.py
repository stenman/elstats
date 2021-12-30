import sqlite3
import logging
from sqlite3 import Error
from datetime import datetime


# logging
LOG = "elstats.log"
logging.basicConfig(filename=LOG, filemode="a", level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    logger.debug("Creating a database connection...")

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        logger.debug("Error when connecting to DB: " + e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        logger.debug("Error when creating tables: " + e)


def create_tables():
    """ Should only be run once
    """

    logger.debug("Creating database tables...")

    database = r"elstats.db"

    sql_create_pulse_stats_table = """CREATE TABLE IF NOT EXISTS pulse_stats (
                id                              INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
            ,   dateAdded                       DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'NOW', 'localtime'))
            ,   timestamp                       DATETIME NOT NULL
            ,   power                           REAL
            ,   lastMeterConsumption	        REAL
            ,   accumulatedConsumption          REAL
            ,   accumulatedConsumptionLastHour  REAL
            ,   accumulatedCost                 REAL
            ,   currency                        TEXT
            ,   minPower                        REAL
            ,   averagePower                    REAL
            ,   maxPower                        REAL
            ,   powerReactive                   REAL
            ,   powerFactor                     REAL
            ,   voltagePhase1                   REAL
            ,   voltagePhase2                   REAL
            ,   voltagePhase3                   REAL
            ,   currentL1                       REAL
            ,   currentL2                       REAL
            ,   currentL3                       REAL
            ,   signalStrength                  REAL
        );"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create pulseStats table
        logger.debug("Creating table...")
        create_table(conn, sql_create_pulse_stats_table)
        logger.debug("Created table successfully!")
    else:
        logger.debug("Error when creating the database connection")


def save_pulse_stats(data):
    """
    Create a new pulse_stat
    :param conn:
    :param task:
    :return:
    """

    logger.debug("Saving pulse stats...")

    ps = (  
            sanitizeTimeStamp(data["data"]["liveMeasurement"]["timestamp"])
        ,   data["data"]["liveMeasurement"]["power"]
        ,   data["data"]["liveMeasurement"]["lastMeterConsumption"]
        ,   data["data"]["liveMeasurement"]["accumulatedConsumption"]
        ,   data["data"]["liveMeasurement"]["accumulatedConsumptionLastHour"]
        ,   data["data"]["liveMeasurement"]["accumulatedCost"]
        ,   data["data"]["liveMeasurement"]["currency"]
        ,   data["data"]["liveMeasurement"]["minPower"]
        ,   data["data"]["liveMeasurement"]["averagePower"]
        ,   data["data"]["liveMeasurement"]["maxPower"]
        ,   data["data"]["liveMeasurement"]["powerReactive"]
        ,   data["data"]["liveMeasurement"]["powerFactor"]
        ,   data["data"]["liveMeasurement"]["voltagePhase1"]
        ,   data["data"]["liveMeasurement"]["voltagePhase2"]
        ,   data["data"]["liveMeasurement"]["voltagePhase3"]
        ,   data["data"]["liveMeasurement"]["currentL1"]
        ,   data["data"]["liveMeasurement"]["currentL2"]
        ,   data["data"]["liveMeasurement"]["currentL3"]
        ,   data["data"]["liveMeasurement"]["signalStrength"])

    database = r"elstats.db"
    
    conn = create_connection(database)

    with conn:
        sql = ''' INSERT INTO pulse_stats(
            timestamp
        ,   power
        ,   lastMeterConsumption
        ,   accumulatedConsumption
        ,   accumulatedConsumptionLastHour
        ,   accumulatedCost
        ,   currency
        ,   minPower
        ,   averagePower
        ,   maxPower
        ,   powerReactive
        ,   powerFactor
        ,   voltagePhase1
        ,   voltagePhase2
        ,   voltagePhase3
        ,   currentL1
        ,   currentL2
        ,   currentL3
        ,   signalStrength
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

        cur = conn.cursor()
        cur.execute(sql, ps)
        conn.commit()

def sanitizeTimeStamp(ts):
    timeStamp = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%f%z')
    timeStamp = timeStamp.replace(tzinfo=None)
    return timeStamp