from testbase import TestBase, TestError
import traceback

class Test(TestBase):
    """
    Usage: loadTables.py [options] tableName
    
    ####### connection Data #######
    --user=        user to use for DB connection
    --password=    password of the user
    --address=     NDB host
    --port=        NDB port
    --remotemode=  default: True
    --autocommit=  default: False
    --tracelevel=  default: 0
    ###############################
    
    ###### configurationData ######
    --tableName=   table to load (repeat as needed)
    --namespace=   load all tables in namespace (default: None)
    --allTables    load _all_ tables! (of all namespaces)
    --verbose      print info which tables are currently loading
    ###############################
    
    """
    requiredArgs = 0
    argnames = ['tableName']
    longopts = ['user=','password=','address=','port=','remotemode=','autocommit=','tracelevel=','allTables','namespace=','verbose']
    def runHandler(self, tableName=None, user='system', password=None, address='localhost', port=None, remotemode=True, autocommit=False, tracelevel=0, \
                   allTables=False, namespace=None, verbose=False):
        if tableName is None and not allTables and namespace is None:
            raise Exception('Please specify any option, either all, a selection by namespace or a single table.')

        if port:
            port = int(port)
        self.openConnection(user, password, address, port, autocommit, tracelevel, remotemode)
        if allTables:
            tables = self.runTest('listTables', address=address, port=port, user=user, password=password, onlyColumnTables=1)
        elif namespace is not None:
            tables = self.runTest('listTables', namespace=namespace, address=address, port=port, user=user, password=password, onlyColumnTables=1)
        else:
            tables = []
            if not isinstance(tableName, list):
                tableName = [tableName]
            for table in tableName:
                if not isinstance(table, list):
                    pos = table.find(':')
                    if pos == -1:
                        schema = 'SYSTEM' # default schema
                    else:
                        schema = table[:pos]
                        table = table[pos + 1:]
                    tables.append([schema, table])
                else:
                    tables.append(table)
        rc = 0
        for table in tables:
            if verbose:
                print('start loading "%s"."%s"' % tuple(table))
            try:
                self.executeSQLStatement('LOAD "%s"."%s" ALL' % tuple(table))
            except:
                rc = rc +1
                traceback.print_exc()
        
        self.closeConnection()
        if rc != 0:
            raise TestError, "ERROR IN LOAD_TABLES"
        return True

if __name__ == '__main__':
    Test().main()
