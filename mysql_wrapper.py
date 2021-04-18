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
    def __init__(self, username=None, password=None, current_host='localhost', db=None):
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
        self.db = db
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def connect(self):
        try:
            if self.db:
                mysql_logger.debug("Connecting to database with name %s" % self.db)
            else:
                mysql_logger.debug("Connecting to mysql")
            connection = connector.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                database=self.db
            )
            return connection
        except Exception as error:
            mysql_logger.debug(error)
            raise ConnectionError("Error connecting to db")


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

    def drop_db(self,name):
        mysql_logger.debug("Removing database with name %s" % name)
        try:
            self.cursor.execute("DROP DATABASE %s" % name)
        except Exception as error:
            mysql_logger.debug(error)

    def create_table(self, name, columns=None):
        if columns is None:
            columns = {}
        mysql_logger.debug("Creating table with name %s and columns" % name + str(columns))

        execute_string = "CREATE TABLE %s (" % name
        if columns:
            for column in columns:
                data_type = columns[column]["datatype"]
                if "parameters" in columns[column]:
                    parameters = columns[column]["parameters"]
                else:
                    parameters = ""
                execute_string += "\n %s %s %s," % (column, data_type, parameters)
            execute_string = execute_string[:-1]
        execute_string += "\n )"
        try:
            mysql_logger.debug(execute_string)
            self.cursor.execute(execute_string)
        except Exception as error:
            mysql_logger.debug(error)

    def drop_table(self,name):
        mysql_logger.debug("Removing table with name %s" % name)
        try:
            self.cursor.execute("DROP TABLE %s" % name)
        except Exception as error:
            mysql_logger.debug(error)

    def describe_table(self, name):
        try:
            self.cursor.execute("DESCRIBE %s" % name)
            table = self.cursor.fetchall()
            for row in table:
                mysql_logger.debug(row)
        except Exception as error:
            mysql_logger.debug(error)

    def insert_record(self, table_name,field_names, data):
        mysql_logger.debug("Inserting record to table %s - %s" %(table_name,data))
        try:
            mysql_logger.debug("INSERT INTO %s \n %s \n VALUES %s" % (table_name,field_names, data))
            self.cursor.execute('INSERT INTO %s \n %s \n VALUES %s' %(table_name,field_names, data))
        except Exception as error:
            mysql_logger.debug(error)

    def read_table(self, table_name):
        try:
            self.cursor.execute('SELECT * from %s' % table_name)
            table = self.cursor.fetchall()
            for row in table:
                mysql_logger.debug(row)
        except Exception as error:
            mysql_logger.debug(error)

