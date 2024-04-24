import sqlite3
import re
import numpy as np
import pandas as pd

def command():
    sentinel = '' # ends when this string is seen
    com = ''
    for line in iter(input, sentinel):
        line+=' '
        com+=line
        pass # do things here
    return com

def readcom(file):
    com = ''
    with open(file,'r') as f:
        com = re.findall(r'([^;]+)(?=;)',f.read())[-1].strip('\t\n\r')
        print(com)
        '''for row in f:
            print("I'm here"+row.strip('\n\r'))
            com += row'''
    return com

def regexp(y, x, search=re.search):
    return 1 if search(y, x) else 0

class query():
    def __init__(self,name):
        self.db_file = name+'.db'
        self.conn = None
    
    def sql2db(self,sql_file):
        """ create a database connection to a SQLite database """
        #https://stackoverflow.com/questions/58416082/how-to-create-a-database-file-in-python
        #https://stackoverflow.com/questions/51065457/how-to-import-a-sql-file-to-python
        conn = None
        try:
            if not self.db_file:
                conn = sqlite3.connect(db_file)
                cur = conn.cursor() #cursor object
                with open(sql_file, 'r') as f: #Not sure if the 'r' is necessary, but recommended.
                    cur.executescript(f.read())
            else:
                print(f"{self.db_file} exists.")
        except Exception as e:
            print(f"Encounter exception: {e}")
        finally:
            if conn:
                conn.close()
    
    def connect(self):
        self.conn = sqlite3.connect(self.db_file)
        self.conn.create_function('regexp', 2, regexp)
    
    def get_tables(self):
        table_name_query = "SELECT name FROM sqlite_master WHERE type='table';"
        if self.conn:
            cursor = self.conn.execute(table_name_query)
        else:
            self.conn = sqlite3.connect(self.db_file)
            cursor = self.conn.execute(table_name_query)
        print('\n'.join([f"{name[0]}"for name in cursor.fetchall()]))
        self.conn.close()
        self.conn = None
    
    def execute(self,command):
        try:
            cursor = self.conn.execute(command) #cursor object
            df = pd.DataFrame(cursor.fetchall())
            df.columns = [cols[0] for cols in cursor.description]
            '''for cols in cursor.description:
                print("{}\t".format(cols[0]),end="")
            print()
            for row in cursor:
                for field in row:
                    print("{}\t".format(field), end="")
                print()'''
            return df
        except Exception as e:
            print(f"Encounter exception: {e}")
        
    
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            print("the sqlite connection is closed")