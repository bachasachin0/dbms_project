import os
import streamlit as st
from sql import connect_to_db,check_db_connection,insert_person_data,dsn,insert_incident_data,insert_participation_data,execute_query,select_all_data,select_specific_columns,retrieve_suspects
import pandas as pd

def insertion_page():
    # Connect to the database
    conn = connect_to_db(dsn)
 
    tab1, tab2, tab3 = st.tabs(["Add Person Data", "Add Incident Data", "Add Participation Data"])
    # Page for adding data to Person table
    with tab1:
        st.header('Add Person Data')
        name = st.text_input('Name:', key='name_input')
        dob = st.date_input('Date of Birth:', key='dob_input')
        ssn = st.text_input('Social Security Number: (eg: 9xx-8xx-7xx)', key='ssn_input')
        st_name = st.text_input('State:', key='st_name_input')
        city = st.text_input('City:', key='city_input')
        if st.button('Add Person Data'):
            pid = insert_person_data(conn, name, dob, ssn, st_name, city)
            st.success(f'Data added successfully to Person table. PID: {pid}')

    # Page for adding data to Incident table
    with tab2:
        st.header('Add Incident Data')
        crimetype_options = ['Theft', 'Assault', 'Burglary', 'Robbery', 'Vandalism', 'Fraud', 'Drug Possession', 'Homicide', 'Other']
        crimetype = st.selectbox('Crime Type:', crimetype_options, key='crimetype_input')
        date = st.date_input('Date of Incident:', key='date_input')
        time = st.time_input('Time of Incident:', key='time_input')
        formatted_datetime = f"{date} {time}:00"
        st_name_incident = st.text_input('State:', key='st_name_incident_input')
        city_incident = st.text_input('City:', key='city_incident_input')

        if st.button('Add Incident Data'):
            iid = insert_incident_data(conn, crimetype, formatted_datetime, st_name_incident, city_incident)
            if iid:
                st.success(f'Data added successfully to Incident table. IID: {iid}')
            else:
                st.error('Failed to add data to Incident table.')

    # Page for adding data to Participation table
    with tab3:
        # Input fields for participation data
        st.header("insert Particiaption data")
        pid = st.text_input('Person ID (PID):', key='pid_input')
        iid = st.text_input('Incident ID (IID):', key='iid_input')
        role = st.selectbox('Role:', ['victim', 'witnessed', 'suspect'], key='role_input')

        if st.button('Add Participation Data'):
            success, message = insert_participation_data(conn, pid, iid, role)
            if success:
                st.success(message)
            else:
                st.error(message)

def retrival_page():
    # Connect to the database
    conn = connect_to_db(dsn)
 
    st.title('Database Query Tool')
    st.write('Interface To Search data from database')
     # Query options
    query_type = st.selectbox('Select query type:', ['Select All Data', 'Select Specific Columns', 'Retrieve Suspects'])

    # Execute selected query type
    if query_type == 'Select All Data':
        table_name = st.selectbox('Select table:', ['person', 'incident', 'participation'])
        data = select_all_data(conn, table_name)
        if data is not None:
            st.write('Query Result:')
            st.dataframe(data)
        else:
            st.error('No data found.')
        
            
    elif query_type == 'Select Specific Columns':
        table_name = st.selectbox('Select table:', ['person', 'incident', 'participation'])
        
        # Retrieve column names for the selected table
        query = f"SELECT COLUMN_NAME FROM SYSIBM.COLUMNS WHERE TABLE_NAME = '{table_name.upper()}'"
        column_names = [row['COLUMN_NAME'] for row in execute_query(conn, query)]
        
        selected_columns = st.multiselect('Select columns to retrieve:', column_names)
        
        if selected_columns:
            data = select_specific_columns(conn, table_name, selected_columns)
            if data:
                df = pd.DataFrame(data)
                st.write('Query Result:')
                st.dataframe(df)
            else:
                st.warning('Please select at least one column.')

  

    elif query_type == 'Retrieve Suspects':
        role = st.selectbox('Select role:', ['victim', 'witnessed', 'suspect'])
        data = retrieve_suspects(conn, role)
        if data:
            df1=pd.DataFrame(data)
            st.write('Query Result:')
            st.write(df1)


# Streamlit main web application
def main():
    
    st.set_page_config(
    page_title="Crime databse Application",
    page_icon="🖇️🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    
    )

    st.markdown(""" <style> .font {
    font-size:50px ; font-family: 'Cooper Black'; color: #6cb9f5;} 
    </style> """, unsafe_allow_html=True)
    st.markdown('<p class="font">Crime Database</p>', unsafe_allow_html=True)
    st.sidebar.image('img/logo2.jpg',use_column_width=True)
    st.sidebar.title("Navigation Pane")
    
    page = st.sidebar.radio("Go to", ("Insert Data", "Retrieve Data"))
    
    # Connect to the database
    conn = connect_to_db(dsn)
 
    # Check database connection
    if check_db_connection(conn):
        st.success("Database connected successfully!")
        if page == "Insert Data":
            insertion_page()
        elif page == "Retrieve Data":
            retrival_page()
    else:
        st.error("Failed to connect to the database.")
   

if __name__ == '__main__':
    main()
