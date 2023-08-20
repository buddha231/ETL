# import snowflake.connector as connector
from scripts.common.connect import connection
import os


conn = connection(schema="STG")

# folder path
data_path = r'C:\\Users\\buddha.gautam\\Desktop\\ASSIGNMENTS\\ETL\\data\\'


# list to store files
data_files = []
cursor = conn.cursor()

# Iterate directory
for path in os.listdir(data_path):
    # check if current path is a file
    if os.path.isfile(os.path.join(data_path, path)):
        filename = path[1:-4]
        try:
            cursor.execute(
                f'CREATE OR REPLACE STAGE {filename}'
            )
            cursor.execute(
                f'''PUT
                    file://{data_path}{path} @{filename}
                    OVERWRITE=TRUE'''
            )
            print(f"staging {path}")

            cursor.execute(f"TRUNCATE TABLE STG_{filename}")

            print(f"truncating STG_{filename}")

            cursor.execute(
                    f"""COPY INTO STG_{filename}
                     FROm @{filename} ON_ERROR = CONTINUE"""
                )
            print(f"loading STG_{filename}")
            print(f"loading to TGT_{filename}")
            sql_file = open(f"scripts/sql/tl_{filename.lower()}.sql", "r")
            sql = sql_file.read()
            conn.execute_string(sql)
            sql_file.close()
            print(f"ETL completed for {filename}\n")

        except Exception as e:
            print(e)

        data_files.append(path)
print("Done! \n")

