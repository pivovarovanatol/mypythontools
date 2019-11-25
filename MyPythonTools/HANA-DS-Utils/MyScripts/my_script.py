from hdbcli import dbapi
import connections
 ## Replace SCHEMA1 with your schema 

# assume HANA host id is abcd1234 and instance no is 00. user id is USER1 and password is Password1 
conn = connections.conn2_00_024 

#Check if database connection was successful or not 
print conn.isconnected() 




cursor = conn.cursor() 
cursor2 = conn.cursor() 
cursor3 = conn.cursor() 


#stmnt = 'select top 10 * from "SAPBWH"."/BIC/B0001460000"' 

#stmnt = 'call CATCH_ROWS();' 

#stmnt = 'select TOP 100 ROWNO from "SAPBWH"."ROWS" where ROWNO like \'78%\';'
stmnt = 'select ROWNO from "SAPBWH"."ROWS" where "ROWNO" >= \'7881299340000000\' and "ROWNO" <= \'7881299379999999\' order by "ROWNO";' #and "ROWNO" like \'78%\'
stmnt2 = 'select UVFAS from "SAPBWH"."/BIC/B0001460000" WHERE "$rowid$" ='


cursor.execute(stmnt) 
result = cursor.fetchall() 

for row in result:    
	for col in row:    
		#stmnt3 = 'UPDATE "SAPBWH"."ROWS" SET "FLAG"=\'E\' WHERE "ROWNO" =\'' + col + '\';'
		#cursor3.execute(stmnt3) 

		#stmnt3 = 'commit;'
		#cursor3.execute(stmnt3) 

		#print '==== Checking:' + col
		stmnt2 = 'select UVFAS from "SAPBWH"."/BIC/B0001460000" WHERE "$rowid$" =' + col + ';'
		#print stmnt2
		try:
			cursor2.execute(stmnt2) 
			#stmnt3 = 'UPDATE "SAPBWH"."ROWS" SET "FLAG"=\'A\' WHERE "ROWNO" =\'' + col + '\';'

			#cursor3.execute(stmnt3) 

		except Exception,ex:  
	        	print '==== Got error at rowid:' + col  + ' !!!'
			stmnt3 = 'UPDATE "SAPBWH"."ROWS" SET "FLAG"=\'E\' WHERE "ROWNO" =\'' + col + '\';'
			cursor3.execute(stmnt3) 


#print result 

cursor3.close() 
cursor2.close() 
cursor.close() 

conn.close()    


