#from hdbcli import dbapi
import connections

## Replace SCHEMA1 with your schema 

conn = connections.conn2_00_024

#Check if database connection was successful or not
print conn.isconnected() 

cursor = conn.cursor() 

i = 0

while True:    
		#cursor3.execute(stmnt3) 
	stmnt = 'INSERT into system."/BIC/PYPARTID" values (\'' + i.__str__() + '\', \'A\', \'\');'
	print 'Executing statement for: ' + i.__str__()
	try:
		cursor.execute(stmnt) 

	except Exception,ex:  
        	print '==== Got error at rowid:' + i.__str__()  + ' !!!'

	i = i + 1
	if i > 1000 :
		break
		
#while i < 2147450001

#print result 

cursor.close() 

conn.close()    


