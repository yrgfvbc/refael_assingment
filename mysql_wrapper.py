import mysql.connector as connector
import logger

try:
    import credentials

    CREDENTIALS_PASSED = True
except ModuleNotFoundError:
    CREDENTIALS_PASSED = False  # Global parameter to understand if credentials were passed for the mysql db

mysql_logger = logger.make_logger("mysql_logger")


# Can pass the username and password for the db in a "credentials.py" file or as a value

class MysqlWrapper:
    def __init__(self, username=None, password=None, current_host='localhost'):
        global CREDENTIALS_PASSED

        if username is None and password is None:
            if CREDENTIALS_PASSED:
                self.username = credentials.username
                self.password = credentials.password
            else:
                error_message = "Authentication error - No credentials file and no key passed"
                mysql_logger.debug(error_message)
                raise ConnectionError(error_message)
        else:
            self.username = username
            self.password = password
        self.host = current_host
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def connect(self, db = None):
        try:
            connection = connector.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                database=db
            )
        except Exception as error:
            mysql_logger.log(error)
            raise error
        return connection

    def create_db(self, name):
        mysql_logger.debug("Creating database with name %s" % name)
        try:
            self.cursor.execute("CREATE DATABASE %s" % name)
        except Exception as error:
            mysql_logger.debug(error)

    def show_all_databases(self):
        try:
            self.cursor.execute("SHOW DATABASES")
            for db in self.cursor:
                print(db)
        except Exception as error:
            mysql_logger.debug(error)

    def remove_db(self,name):
        mysql_logger.debug("Removing database with name %s" % name)
        try:
            self.cursor.execute("DROP DATABASE %s" % name)
        except Exception as error:
            mysql_logger.debug(error)



my_connection = MysqlWrapper()
#my_connection.show_all_databases()
my_connection.create_db('test')
my_connection.remove_db('test')

