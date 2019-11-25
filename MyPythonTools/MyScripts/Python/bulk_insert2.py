from hdbcli import dbapi

 ## Replace SCHEMA1 with your schema 

# assume HANA host id is abcd1234 and instance no is 00. user id is USER1 and password is Password1 
conn = dbapi.connect('vanpghana07.pgdev.sap.corp', 31115, 'system', 'manager') 

#Check if database connection was successful or not 
print conn.isconnected() 

cursor = conn.cursor() 

i=0
steps = 10 # Number of iterations to create bulk data


while True:    
	stmnt = 'INSERT into system."/BIC/PYPARTID" values (\'' + i.__str__() + '\', \'A\', \'\');'
	print stmnt
	
	print 'Inserting : ' + i.__str__()
	try:
		cursor.execute(stmnt) 

	except Exception,ex:  
		print '==== Got error at rowid:' + i.__str__()  + ' !!!'
		print ex
	i = i + 1
	if i > 1000000:
		break
		

# 	i = 1
# while True:    
# 	stmnt = 'UPDATE system."/BIC/PYPARTID" SET "/BIC/YPARTID" = "$rowid$");'
# 	print 'Updating table with new values at step: ' + i.__str__()
# 	try:
# 		cursor.execute(stmnt)
# 
# 	except Exception,ex:  
#         	print '==== Got error at rowid:' + i.__str__()  + ' !!!'
# 
# 	
# 	stmnt = 'INSERT INTO system."/BIC/PYPARTID" (select concat(\'A\', "/BIC/YPARTID"), objvers, changed from "/BIC/PYPARTID");'
# 	#print stmnt
# 	print 'Inserting new values at step: ' + i.__str__()
# 	try:
# 		cursor.execute(stmnt)
# 
# 	except Exception,ex:  
#         	print '==== Got error at rowid:' + i.__str__()  + ' !!!'
# 
# 	
# 	
# 	
# 	i = i + 1
# 	if i > steps:
# 		break




cursor.close() 

conn.close()    




