from scripts.common.connect import connection
import sys

def transform_to():
    pass


def load_DLL(schema, filename):
    conn = connection(schema=schema)
    sql_file = open(f"scripts/sql/{filename}", "r")
    sql = sql_file.read()
    print(sql)
    try:
        result = conn.execute_string(sql)
        print(result)
    except Exception as e:
        print(e)
    sql_file.close()


if __name__ == "__main__":
    n = len(sys.argv)
    load_DLL(schema=sys.argv[n-2], filename=sys.argv[n-1])


