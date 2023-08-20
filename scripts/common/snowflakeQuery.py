import sys
from connect import connection
# import SQLalchemy

def main(schema):
    conn = connection(schema)
    cursor = conn.cursor()
    while True:
        try:
            query = input("QUERY> ")
            result = cursor.execute(query).fetchall()
            print(result)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    n = len(sys.argv)
    main(sys.argv[n-1])
    
