import mysql.connector
import pprint
import csv

#connection

class DBConnection:
    def __init__(self, DB_HOST, DB_USER, DB_PASSWORD, DB_NAME):
        self.host = DB_HOST
        self.database = DB_NAME
        self.user = DB_USER
        self.password = DB_PASSWORD
        self.conn = None

    def get_conn(self):
        if self.conn is None:
            self.conn = mysql.connector.connect(
                                    host = self.host,
                                    db = self.database,
                                    user = self.user,
                                    passwd = self.password)
        return self.conn

mydb = DBConnection('localhost','root','0000','sakila').get_conn()
mycursor = mydb.cursor()

print('Print to CSV File')
tablename = input('table name: ')
csvname = input('csv name: ')
mycursor.execute("SELECT * FROM "+ tablename)
results = mycursor.fetchall()

#csv export
if not mycursor.rowcount:
    print('Error #4')
    print('No Log on this date found')
    time.sleep(2)
    restart_program()
else:
    fp = open('C:/Users/LS-COM-00044/Desktop/파이썬/'+ csvname +'.csv', 'w')
    myFile = csv.writer(fp, lineterminator='\n')
    myFile.writerows(results)
    fp.close()
    print('csv exported')

#extra option
mycursor.execute("UPDATE actor SET last_name='HE'")
mydb.commit()
