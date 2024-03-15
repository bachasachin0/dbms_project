
import os
from dotenv import load_dotenv
load_dotenv()
import ibm_db
import pandas as pd

dsn_hostname=os.getenv("Host_name")
dsn_uid= os.getenv("uid")
dsn_pwd=os.getenv("password")
dsn_port= os.getenv("Port_number")
dsn_database=os.getenv("database")
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_protocol = "TCPIP" 
dsn_security = "SSL"


dsn = (
        "DRIVER={0};"
        "DATABASE={1};"
        "HOSTNAME={2};"
        "PORT={3};"
        "PROTOCOL={4};"
        "UID={5};"
        "PWD={6};"
        "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)


try:
        conn = ibm_db.connect(dsn, "", "")
        print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
        # return conn
except:
        print ("Unable to connect: ", ibm_db.conn_errormsg() )
        # return None


# to connect the database
def connect_to_db(dsn):
     conn=ibm_db.connect(dsn,"","")
     return conn

# to check the database connection
def check_db_connection(conn):
    if conn:
        # print("connected!")
        return True
    else:
        return False
    

# # # Function to insert data into the Person table
def insert_person_data(conn, name, dob, ssn, st_name, city):
    sql_insert = "INSERT INTO Person (name, dob, ssn, st_name, city) VALUES (?, ?, ?, ?, ?)"
    stmt_insert = ibm_db.prepare(conn, sql_insert)
    if not stmt_insert:
        return None, "Failed to prepare insert statement"
    
    ibm_db.bind_param(stmt_insert, 1, name)
    ibm_db.bind_param(stmt_insert, 2, dob)
    ibm_db.bind_param(stmt_insert, 3, ssn)
    ibm_db.bind_param(stmt_insert, 4, st_name)
    ibm_db.bind_param(stmt_insert, 5, city)
    
    result = ibm_db.execute(stmt_insert)
    if result:
        sql_last_id = "SELECT pid FROM Person ORDER BY pid DESC LIMIT 1"
        stmt_last_id = ibm_db.exec_immediate(conn, sql_last_id)
        last_pid = ibm_db.fetch_both(stmt_last_id)
        if last_pid:
            return last_pid[0], f"Person with PID {last_pid[0]} successfully registered."
        else:
            return None, "Failed to fetch last inserted PID"
    else:
        return None, "Failed to insert data into the database"
    
def insert_incident_data(conn, crimetype, time, st_name, city):
    try:
        sql = "INSERT INTO Incident (crimetype, time, st_name, city) VALUES (?, ?, ?, ?)"
        stmt = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(stmt, 1, crimetype)
        ibm_db.bind_param(stmt, 2, time)
        ibm_db.bind_param(stmt, 3, st_name)
        ibm_db.bind_param(stmt, 4, city)
        ibm_db.execute(stmt)
        
        # Fetch the iid of the last inserted record
        sql_last_iid = "SELECT iid FROM Incident ORDER BY iid DESC LIMIT 1"
        stmt_last_iid = ibm_db.exec_immediate(conn, sql_last_iid)
        last_iid = ibm_db.fetch_both(stmt_last_iid)
        
        if last_iid:
            return last_iid[0]
        else:
            return None
    
    except Exception as e:
        return False ,f"Failed to insert incident data: {e}"



def insert_participation_data(conn, pid, iid, role):
    try:
        # Check if pid exists in Person table
        sql_check_pid = f"SELECT pid FROM Person WHERE pid = ?"
        stmt_check_pid = ibm_db.prepare(conn, sql_check_pid)
        ibm_db.bind_param(stmt_check_pid, 1, pid)
        ibm_db.execute(stmt_check_pid)
        if not ibm_db.fetch_assoc(stmt_check_pid):
            return False, f"PID {pid} does not exist in Person table"
        
        # Check if iid exists in Incident table
        sql_check_iid = f"SELECT iid FROM Incident WHERE iid = ?"
        stmt_check_iid = ibm_db.prepare(conn, sql_check_iid)
        ibm_db.bind_param(stmt_check_iid, 1, iid)
        ibm_db.execute(stmt_check_iid)
        if not ibm_db.fetch_assoc(stmt_check_iid):
            return False, f"IID {iid} does not exist in Incident table"
        
        # # Validate role
        # valid_roles = ['Victim', 'Witness', 'Suspect']
        # if role not in valid_roles:
        #     return False, f"Role should be one of {', '.join(valid_roles)}"
        
        # Insert data into Participation table
        sql_insert = "INSERT INTO Participation (pid, iid, role) VALUES (?, ?, ?)"
        stmt_insert = ibm_db.prepare(conn, sql_insert)
        ibm_db.bind_param(stmt_insert, 1, pid)
        ibm_db.bind_param(stmt_insert, 2, iid)
        ibm_db.bind_param(stmt_insert, 3, role)
        ibm_db.execute(stmt_insert)
        
        return True, "Participation data inserted successfully"
    
    except Exception as e:
        return False, f"Failed to insert participation data: {e}"

    
# Function to execute SQL query and return data
def execute_query(conn, query, params=None):
    try:
        stmt = ibm_db.prepare(conn, query)
        if params:
            if isinstance(params, (list, tuple)):
                ibm_db.execute(stmt, params)
            else:
                ibm_db.execute(stmt, (params,))
        else:
            ibm_db.execute(stmt)
        
        data = []
        result = ibm_db.fetch_assoc(stmt)
        while result:
            data.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
        
        return data
    except Exception as e:
        return None, f"Error executing SQL query: {e}"
        # return None
        
    
# Function to select all data from a table
def select_all_data(conn, table_name):
    query = f"SELECT * FROM {table_name}"
    return execute_query(conn, query)

#Function to select specific columns of table
def select_specific_columns(conn, table_name, selected_columns):
    try:
        # Construct the SQL query
        columns_str = ', '.join(selected_columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        
        # Execute the query
        data = execute_query(conn, query)
        return data
    except Exception as e:
        return False, f"Error executing SQL query: {e}"
        # return None



# Function to retrieve suspects based on role
def retrieve_suspects(conn, role):
    query = """
    SELECT p.name, i.time, i.st_name, i.city,pa.role
    FROM person p
    JOIN participation pa ON p.pid = pa.pid
    JOIN incident i ON pa.iid = i.iid
    WHERE pa.role = ?
    """
    params = (role,)
    return execute_query(conn, query, params)

