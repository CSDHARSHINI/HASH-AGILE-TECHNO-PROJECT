import pymysql
from pymysql import Error
# Database connection function
def create_connection():
    try:
        connection = pymysql.connect(
            host='localhost',
            user='DHARSH1',
            password='DN',
            database='management'
           
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

# Function definitions
def createCollection(p_collection_name):
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {p_collection_name} (
                    EmployeeID VARCHAR(10) PRIMARY KEY,
                    Name VARCHAR(100),
                    Department VARCHAR(50),
                    Gender VARCHAR(10)
                )
            """)
            connection.commit()
            print(f"Collection {p_collection_name} created successfully.")
        except Error as e:
            print(f"Error creating collection: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def indexData(p_collection_name, p_exclude_column):
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            columns = ["EmployeeID", "Name", "Department", "Gender"]
            columns.remove(p_exclude_column)
            
            for column in columns:
                cursor.execute(f"CREATE INDEX idx_{column.lower()} ON {p_collection_name} ({column})")
            
            connection.commit()
            print(f"Indexed data in {p_collection_name}, excluding {p_exclude_column}.")
        except Error as e:
            print(f"Error indexing data: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {p_collection_name} WHERE {p_column_name} = %s", (p_column_value,))
            results = cursor.fetchall()
            print(f"Search results for {p_column_name} = {p_column_value} in {p_collection_name}:")
            for row in results:
                print(row)
            return results
        except Error as e:
            print(f"Error searching data: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def getEmpCount(p_collection_name):
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {p_collection_name}")
            count = cursor.fetchone()[0]
            print(f"Employee count in {p_collection_name}: {count}")
            return count
        except Error as e:
            print(f"Error getting employee count: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def delEmpById(p_collection_name, p_employee_id):
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(f"DELETE FROM {p_collection_name} WHERE EmployeeID = %s", (p_employee_id,))
            connection.commit()
            print(f"Employee {p_employee_id} deleted from {p_collection_name}.")
        except Error as e:
            print(f"Error deleting employee: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def getDepFacet(p_collection_name):
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(f"SELECT Department, COUNT(*) as Count FROM {p_collection_name} GROUP BY Department")
            results = cursor.fetchall()
            print(f"Department facet for {p_collection_name}:")
            for row in results:
                print(f"{row['Department']}: {row['Count']}")
            return results
        except Error as e:
            print(f"Error getting department facet: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

# Function executions
v_nameCollection = 'Hash_YourName'
v_phoneCollection = 'Hash_YourPhoneLastFourDigits'



# Sample data insertion (for demonstration purposes)
def insert_sample_data():
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            sample_data = [
                ('E01001', 'John Doe', 'IT', 'Male'),
                ('E01002', 'Jane Smith', 'HR', 'Female'),
                ('E01003', 'Bob Johnson', 'Finance', 'Male'),
                ('E01004', 'Alice Brown', 'IT', 'Female'),
            ]
            cursor.executemany(f"INSERT INTO {v_nameCollection} (EmployeeID, Name, Department, Gender) VALUES (%s, %s, %s, %s)", sample_data)
            cursor.executemany(f"INSERT INTO {v_phoneCollection} (EmployeeID, Name, Department, Gender) VALUES (%s, %s, %s, %s)", sample_data)
            connection.commit()
            print("Sample data inserted successfully.")
        except Error as e:
            print(f"Error inserting sample data: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

insert_sample_data()