from testbase import TestBase

class Test(TestBase):
    """
    Load all column tables in main memory.
    
    Usage: loadAllTables.py [options]
    
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
    --namespace=   load all tables in namespace (default: None)
    --allTables    load _all_ tables! (of all namespaces)
    --verbose      print info which tables are currently loading
    ###############################
    
    """
    requiredArgs = 0
    argnames = []
    longopts = ['user=','password=','address=','port=','remotemode=','autocommit=','tracelevel=','allTables','namespace=','likeCondition=','verbose']
    def runHandler(self, user='system', password=None, address='localhost', port=None, remotemode=True, autocommit=False, tracelevel=0, \
                   allTables=False, namespace=None, likeCondition=None, verbose=False):
        if not allTables and namespace is None:
            raise Exception('Please specify an option, either all tables or a selection by namespace.')

        try:
            if allTables:
                tables = self.runTest('listTables', address=address, port=port, user=user, password=password, likeCondition=likeCondition, onlyColumnTables=1)
            else:
                tables = self.runTest('listTables', address=address, port=port, namespace=namespace, user=user, password=password, likeCondition=likeCondition, onlyColumnTables=1)
            threadList = []
            for i in range(0, len(tables), 1000):
                #print i, min(i+999, len(tables)-1)
                threadList.append(self.runTestThread('loadTables', tableName=tables[i:i+999], address=address, port=port, user=user, password=password, verbose=verbose))
            for thread in threadList:
                thread.join()
        except:
            import traceback
            traceback.print_exc()


if __name__ == '__main__':
    Test().main()
