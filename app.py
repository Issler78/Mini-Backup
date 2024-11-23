from connect import connect
from mysql.connector import cursor as Cursor
import pandas as pd
import datetime
import schedule
from time import sleep

def Get_Result_of_the_Table(cursor: Cursor):
    cursor.execute("SELECT * FROM persons;")
    return cursor.fetchall()

def To_CSV(result):
    try:
        df = pd.DataFrame(result)
        df.to_csv(
            f"backup_{datetime.datetime.now().strftime("%Y-%m-%d")}.csv",
            sep="|", 
            index=False, 
            columns=[0, 1, 2, 3, 4, 5, 6],
            header=["ID", "FIRST NAME", "LAST NAME", "EMAIL", "PHONE NUMBER", "BIRTH DATE", "ADDRESS"]
        )

        print("Novo backup criado!")
    except Exception as e:
        print(f"Ocorreu algum erro: {e}")

def Main():
    try:
        conn = connect()
        cursor = conn.cursor()
    
        result = Get_Result_of_the_Table(cursor)

        To_CSV(result)
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Ocorreu algum erro: {e}")



if __name__ == "__main__":
    schedule.every().minute.do(Main)
    
    while True:
        schedule.run_pending()
        sleep(1)
        