import mysql_wrapper
import logger
import random
import string


def generate_random_string(min=5, max=15, chars=string.ascii_lowercase + string.ascii_uppercase + string.digits):
    string_length = random.randint(min, max)
    generated_string = ''.join(random.choices(chars, k=string_length))
    return generated_string


def get_string_uuid(cursor):
    cursor.execute("SELECT UUID()")
    uuid = cursor.fetchall()
    return uuid[0][0]


def initialize_table(table_name, row_amount, db_name='my_db'):
    msql = mysql_wrapper.MysqlWrapper(db=db_name)
    msql.create_table(table_name,
                      columns={
                          "row_index": {
                              "datatype": "INT",
                              "parameters": "AUTO_INCREMENT PRIMARY KEY"
                          },
                          "id": {
                              "datatype": "CHAR(36)"
                          },
                          "name": {
                              "datatype": "VARCHAR(15)"
                          },
                      })
    msql.describe_table(table_name)
    for i in range(row_amount):
        ad_name = generate_random_string()
        ad_uuid = get_string_uuid(msql.cursor)
        msql.insert_record(table_name, field_names="(id,name)", data='("%s", "%s")' % (ad_uuid, ad_name))
    msql.connection.commit()
    msql.connection.close()


def create_db(name):
    msql = mysql_wrapper.MysqlWrapper()
    msql.create_db(name)
    msql.connection.close()

def get_table(db,table):
    msql = mysql_wrapper.MysqlWrapper(db=db)
    msql.read_table(table)

if __name__ == "__main__":
    msql = mysql_wrapper.MysqlWrapper()
    msql.drop_db('my_db')
    create_db('my_db')
    initialize_table('ads', 10)
    get_table('my_db', 'ads')

