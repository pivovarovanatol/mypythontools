#from hdbcli import dbapi
import connections

# Reproducing the Crash in ESX::Window::fetch
#
#
conn = connections.conn2_00_044 

#Check if database connection was successful or not 
print conn.isconnected() 

cursor = conn.cursor() 
cursor.execute('set schema SAPS4FIN;');

stmntCrash = """SELECT /* FDA READ */ "MANDT" , "AEDAT" , "TXZ01" , "IDNLF" , "UMREZ" , "SPRAS" , "MAKTX" , "ORDINAL_TYPE" , "ZNAME" , "LOG_CHAIN_ID"  
			 	"NETPR" , "EFFWR" , "UNIT_TYPE" , "VGPOS_5" , "VBELN" , "POSNR" , "MATNR" , "WERKS" , "UMVKZ" , "UMVKN" , "VGBEL" , "VGPOS" , "MEINS" , 
			  	"VRKME" , "ERDAT" , "ERZET" , "BLDAT" , "GRUND" , "BWART" , "LFIMG" , "WBSTK" , "LAST_UPDATE" , "MENGE" , "vgpos_4" FROM "YMBCV_DN_LIPS"  
			  	WHERE "MANDT" = ? AND "SPRAS" = ? AND "WERKS" = ? AND "LAST_UPDATE" >= ? ORDER BY "YMBCV_DN_LIPS" . "VGBEL";"""

################### Get distinct WERKS for parameters 
stmntWerks = 'select distinct WERKS from YMBT_CATALOG order by WERKS'
cursor.execute(stmntWerks)
resultWerks = cursor.fetchall() 

################### Get distinct SPRAS for parameters 
stmntSpras = 'select distinct SPRAS from MAKT order by SPRAS'
cursor.execute(stmntSpras)
resultSpras = cursor.fetchall() 

################### Executing Crash Statement for every combination of values for SPRAS and WERKS 
for rowSpras in resultSpras:    
	for colSpras in rowSpras:
		for rowWerks in resultWerks:    
			for colWerks in rowWerks:
				try:
					params = []
					params.append('800')
					params.append(colSpras)
					params.append(colWerks)
					params.append('20191010')
					print 'Executing select for SPRAS = ' + colSpras + ' WERKS = ' + colWerks + '... ',
					cursor.execute(stmntCrash, params)
					print 'success!'
				except Exception,ex:  
					print '==== Got error at: ' + params.__str__()
					print ex
					exit()

print 'Finished successfully!'
cursor.close() 
conn.close()    
