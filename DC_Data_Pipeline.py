################################################################
#Auther Pradeep 
#Last_Modifed : 23-09-2024
################################################################

# Step 1: Import necessary libraries
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import psycopg2
import pyodbc
from datetime import datetime

# Step 2: Define PostgreSQL connection details
ssh_host = '52.66.179.206'
ssh_port = 22
ssh_user = 'ubuntu'
ssh_private_key = "C:\\Users\\PradeepKapu\\Downloads\\ihx-dms-dw-keypair.pem"

db_host = 'ihx-dms-dw.cnrmtnlpfeaw.ap-south-1.rds.amazonaws.com'
db_port = 5432
db_user = 'pradeepk'
db_password = 'VibKq0'

# Step 3: Set up an SSH tunnel and connect to PostgreSQL
try:
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=ssh_private_key,
        remote_bind_address=(db_host, db_port)
    )
    tunnel.start()

    conn = psycopg2.connect(
        dbname="ma_mt_prod_dw",
        user=db_user,
        password=db_password,
        host='localhost',
        port=tunnel.local_bind_port
    )
    print("Connected to PostgreSQL successfully")

except Exception as e:
    print(f"Failed to connect to PostgreSQL: {e}")
    exit()

# Step 4: Define the SQL Server connection details
source_server = 'db-prod-rr.ihx.in'
source_database = 'IHXProvider'
source_username = 'pradeep.kapu@ihx.in'
source_password = 'WJ[G4QdYvEp#\\v&F'

# Step 5: Connect to SQL Server and fetch the data
try:
    source_connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=' + source_server + ';'
        'DATABASE=' + source_database + ';'
        'UID=' + source_username + ';'
        'PWD=' + source_password + ';'
        'Trusted_Connection=no'
    )
    source_cursor = source_connection.cursor()
    print("Source connection successful")

    # Step 6: Define the query to fetch data from SQL Server
    query_txnid = '''
        WITH Data AS (
            SELECT 
                ROW_NUMBER() OVER (PARTITION BY a.HospActionId ORDER BY ActionAt DESC) AS RID,
                a.HospActionId, 
                a.HospActionFlowId, 
                b.PayerClaimNo, 
                a.StatusId, 
                ActionAt, 
                a.ProviderId, 
                a.ProcedureDesc AS TreatmentName,
                FieldValue AS PayerTransactionId
            FROM IHXProvider..tblPayerHospActionFlow (NOLOCK) a
            JOIN IHXProvider..tblPayerHospAction (NOLOCK) b ON a.HospActionId = b.HospActionId
            LEFT JOIN IHXProvider..tblPayerHospActionExtension (NOLOCK) c 
                ON a.HospActionFlowId = c.ExtensionHospActionFlowId 
                AND FieldName = 'PayerTransactionId'
            WHERE ActionAt >= '2024-09-18 00:00:00' and ActionAt <'2024-09-23 00:00:00'
            AND a.StatusId IN (3, 8, 4) 
        ),
        ClaimsData AS (
            SELECT 
                a.HospActionId, 
                a.HospActionFlowId, 
                a.PayerClaimNo, 
                a.StatusId, 
                a.ActionAt, 
                a.ProviderId, 
                a.TreatmentName,
                a.PayerTransactionId,
                b.HospActionFlowId AS H_FlowId, 
                CASE WHEN b.StatusId IN (3, 8) THEN 'Online' ELSE '' END AS Online
            FROM Data a
            LEFT JOIN Data b 
                ON a.hospActionId = b.HospActionId 
                AND a.RID = b.RID - 1
            WHERE a.StatusId = 4
        ),
        DCTotal AS (
            SELECT DISTINCT FlowId AS Total_FlowId 
            FROM IHXProvider..tblClassificationDetail (NOLOCK)
            WHERE HospActionId IN (SELECT HospActionId FROM ClaimsData)
        ),
        DCBill AS (
            SELECT DISTINCT FlowId AS Bill_FlowId, TransactionId 
            FROM IHXProvider..tblClassificationDetail (NOLOCK)
            WHERE HospActionId IN (SELECT HospActionId FROM ClaimsData)
            AND AvailableDocumentType LIKE '%Bill%'
        )
        SELECT 
            TransactionId,
            a.HospActionId,
            a.HospActionFlowId,
            a.PayerClaimNo,
            a.StatusId,
            CONVERT(varchar, a.ActionAt, 23) AS actionat,
            a.ProviderId,
            d.E_FullName AS hosp_name,
            a.TreatmentName,
            a.PayerTransactionId,
            a.H_FlowId,
            a.[Online],
            CASE WHEN b.Total_FlowId IS NULL THEN '' ELSE 'Yes' END AS DC_Total,
            CASE WHEN c.Bill_FlowId IS NULL THEN '' ELSE 'Yes' END AS DC_Bill_Total,
            CONVERT(varchar, a.ActionAt, 8) AS [time]
        FROM ClaimsData a
        LEFT JOIN DCTotal b ON a.H_FlowId = b.Total_FlowId
        LEFT JOIN IHXSupreme..Entity d ON a.ProviderId = d.e_id
        LEFT JOIN DCBill c ON a.H_FlowId = c.Bill_FlowId
    '''

    # Execute query
    source_cursor.execute(query_txnid)
    txnid = source_cursor.fetchall()
    print("Data fetched from SQL Server")

    if txnid:
        columns = [column[0].lower() for column in source_cursor.description]
        df_platform = pd.DataFrame([list(row) for row in txnid], columns=columns)
    else:
        print("No records found.")
        df_platform = None

except Exception as e:
    print(f"Error fetching data from source: {e}")

finally:
    source_cursor.close()
    source_connection.close()

# Step 7: Insert data into PostgreSQL
if df_platform is not None:
    print("Inserting data into PostgreSQL at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    try:
        # Create SQLAlchemy engine for PostgreSQL
        from sqlalchemy import create_engine
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@localhost:{tunnel.local_bind_port}/ma_mt_prod_dw')

        # Insert data into PostgreSQL
        df_platform.to_sql('platform_dc_data_test', con=engine, index=False, if_exists='append')
        print("Data successfully inserted into public.platform_dc_data_test.")

    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")



# Step 8: Close PostgreSQL connection and stop the SSH tunnel
if conn:
    conn.close()
    print("PostgreSQL connection closed")
if tunnel:
    tunnel.stop()
    print("SSH tunnel stopped")
