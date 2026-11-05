import psycopg

def load_csv(connection, table, csv_path):
    with connection.cursor() as cursor:
        with open(csv_path, "rb") as file:
            with cursor.copy(
                f"COPY {table} FROM STDIN WITH CSV HEADER"
            ) as copy:
                while chunk := file.read(1024 * 1024):
                    copy.write(chunk)

    connection.commit()