from hdbcli import dbapi

 ## Replace SCHEMA1 with your schema 

# assume HANA host id is abcd1234 and instance no is 00. user id is USER1 and password is Password1 
conn = dbapi.connect('llbpal98.pal.sap.corp', 30515, 'system', 'Manager1')

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


