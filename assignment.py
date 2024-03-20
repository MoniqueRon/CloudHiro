import csv
import os
import requests
import zipfile
from io import BytesIO
import psycopg2
from flask import Flask, request, jsonify

hostname = "localhost"
database = "cloudhiro_data"
username = "postgres"
password = "admin"
port_id = "5432"

conn = None
cur = None
table_name = None

url = 'https://public-chi.s3.us-east-2.amazonaws.com/573040c9-3feb-46d4-99e9-0b5999613e8e.zip'
url_password = 'chipasscsv'


app = Flask(__name__)

def execute_query(query, fetch=True):
    try:
        conn = psycopg2.connect(host=hostname,
                                database=database,
                                user=username,
                                password=password,
                                port=port_id)
        
        cur = conn.cursor()
        cur.execute(query)
        if fetch:
            result = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            data = [dict(zip(columns, row)) for row in result]
            return data
        else:
            conn.commit()
            return None
    except Exception as error:
        print(error)
        return None
    finally:
        cur.close()
        conn.close()

def init_db():
    global table_name
    try:
        # Download the zip file from the URL with password and extract the csv
        response = requests.get(url)
        if response.status_code == 200:
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                zip_file.extractall(path=".", pwd=url_password.encode('utf-8'))

                # upload the extracted file to the PostgreSQL database
                for file in zip_file.namelist():
                    with open(file, 'r') as f:
                        table_name = os.path.basename(file).replace('-', '_').replace('.csv', '')
                        if table_name[0].isdigit():
                            table_name = '_' + table_name

                        conn = psycopg2.connect(host=hostname,
                                                database=database,
                                                user=username,
                                                password=password,
                                                port=port_id)
        
                        cur = conn.cursor()
                        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
                        # add auto increment primary key id to the table
                        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN id SERIAL PRIMARY KEY")
                        conn.commit()
                        print(f"{table_name} uploaded to PostgreSQL successfully")
    except Exception as error:
        print(error)
    finally:
        cur.close()
        conn.close()

@app.before_request
def startup():
    init_db()
# Define routes for CRUD operations:

# Create new data
@app.route('/data', methods=['POST'])
def create_data():
    new_data = request.json
    columns = ', '.join(new_data.keys())
    values = ', '.join(f"'{value}'" for value in new_data.values())
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
    execute_query(query, fetch=False)
    return jsonify({'message': 'New data created successfully'})


# Read data
@app.route('/data', methods=['GET'])
def read_all_data():
    query = f"SELECT * FROM {table_name} LIMIT 50"
    data = execute_query(query)
    return jsonify(data)

@app.route('/data/<int:id>', methods=['GET'])
def read_data_by_id(id):
    query = f"SELECT * FROM {table_name} WHERE id={id}"
    data = execute_query(query)
    return jsonify(data)


# Update data
@app.route('/data/<int:id>', methods=['PUT'])
def update_data(id):
    try:
        new_data = request.json
        columns = ', '.join(f"{key} = '{value}'" for key, value in new_data.items())
        query = f"UPDATE {table_name} SET {columns} WHERE id={id}"
        execute_query(query, fetch=False)
        return jsonify({'message': f'Data with id {id} updated successfully'})
    except Exception as error:
        print(error)
        return jsonify({'message': f'Error updating data with id {id}'})


# Delete data
@app.route('/data/<int:id>', methods=['DELETE'])
def delete_data(id):
    try:
        query = f"DELETE FROM {table_name} WHERE id={id}"
        execute_query(query, fetch=False)
        return jsonify({'message': f'Data with id {id} deleted successfully'})
    except Exception as error:
        print(error)
        return jsonify({'message': f'Error deleting data with id {id}'})

if __name__ == '__main__':
    app.run()







