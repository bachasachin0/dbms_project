import mysql.connector
from dotenv import load_dotenv
import os
load_dotenv()


db = mysql.connector.connect(
    host=os.getenv("Host"),
    user=os.getenv("user"),
    password=os.getenv("password"),
    database=os.getenv("database")
)

cursor = db.cursor()

cursor.execute("SELECT * FROM chicago_crime")

# Fetch the results
results = cursor.fetchall()

# Process the results
for row in results:
    print(row)

db.commit()
cursor.close()
db.close()