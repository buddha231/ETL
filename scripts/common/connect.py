import snowflake.connector as connector
# import SQLalchemy
import configparser


def read_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    config = config['DATABASE']
    return config


def connection(schema = 'STG' ):
    config = read_config()
    print("requesting connection")
    connected = connector.connect(
            user=config['USER'],
            password=config['PASSWORD'],
            account=config['ACCOUNT'],
            database=config['DATABASE'],
            schema=schema
    )
    print("connected!!\n")
    return connected

if __name__=="__main__":
    connection()
