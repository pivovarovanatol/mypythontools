import testbase, os, sys, ConfigMgrPy, re, cStringIO, string, time

defaultDotFile = 'plan.dot'
defaultPlanFile = 'plan.py'

class Test(testbase.TestBase):
    """
    Usage: viewPlan.py [options] command [arg]

    Commands:
    list
         display a list of plans in the executor python trace file that
         may be viewed or dotted
    dot [<plannr>]
         create a GraphViz file of the specified plan and open it with
         the program dotty, depending on the options --dotFile, --runDotty
    pdf [<plannr>]
         create a GraphViz file of the specified plan and convert it to pdf.
         Specify a pdf viewer with --pdfViewer= to open the pdf after
         generation.
    run [<plannr> [outputfile]] run the specified plan
    view [<plannr> [outputfile]]
         print the part of the executor python trace file specified to stdout
    count [<plannr>]
         count the number of connections for each server referenced in a plan
    mark <plans to keep>
         set a marker in the executor python trace file so only the end of
         this file will be seen by this script
    unmark
         remove the mark (don't forget to do this after you delete the
         trace file
    trace <on or off>
         switch the executor python trace on or off, in executor.ini
    delete
         remove the trace file
         
    Options:
    --dotFile=      The filename of the dotty file (dot command)
    --planFile=     The filename of the plan file (run command)
    --runDotty=     specify 0 or 1, 1 to run dotty in the dot command (default)
    --includeTrace= specify 1 to include trace data in the dotty file
                    (default is 1)
    --traceFile=    specify a trace file path (usually the trace file path
                    is determined by reading executor.ini)
    --verbose=      verbose output (especially for count command)
    --addPrint=     add code to print the result when using the view command
    --oneServer=    change locations to refer to one index server
                    when using the view command
    --popAsPlan=    generate a plan to execute a single pop with the view cmd
    --replace=      replace existing output files
    --pdfViewer=    Specify a pdf viewer to show the generated pdf. If unset
                    the pdf is only generated (default) 
    --dotViewer=    specify an alternative to dotty
    --classify=     specify 1 to classify plans with the list command
    --listOperations= specify 1 to list operations with the list command

    Options modifying the plan (view, dot, run commands):
    --removePopFlags=1         remove the setPopFlags(something) from all pops
    --setPopFlags=pop=number   use setPopFlags with the specified number on
                               specified pops.  For pops, either a string like
                               pop23, or c connector name (output of the
                               producing pop) like c_k, or a substring of
                               a pop or connector class (e.g. dimfn) may be
                               used
    --retrieve=connector       insert code for retrieving the specified
                               connector(s).  Connector may also be a pop
                               name or a substr (as above)
    --stopAfter=pop            remove parts of the plan that are not after pop
                               Again pop may contain a pop, connector or substr

    Options used with the run command to generate a new plan file
    --onlyPop=pop              create a trace of only one operation by
                               running a partial plan to create inputs and
                               prepending the constant inputs to the original
                               plan.  Pop may be anything, multiple times
    --replaceInput=connector   create a plan by running a fragment to create
                               the values for connector(s) and replacing
                               the operations in the original plan with those
                               (now constant) values
    --keepTemporaries=         set 1 to keep temporary plan and input files
    """

    executePop_comment = '# tracing executePop '
    calculate_comment = '# tracing calculate '
    import_prefix = 'import cPickle, executorPy, fuzzypy, aepy\n\n'
    cmethod_l_prefix = """# assign local indexes:
import testbase
t = testbase.TestBase()
o = fuzzypy.Search()
t.setCommunicationMethod(o, 'l')

"""
    patAddRequestedValue = re.compile(r'ex.addRequestedValue\(([^)]+)\)')
    patAssignConnector = re.compile(
        r'([a-z0-9_]+) = executorPy\.PlanConnector\(plan, (\d+|'
        r'executorPy\.dataClassIds\[\'([^\']+)\'\])\)')
    patExecutePop = re.compile(
        r'(rc = )?pop\.executePop\(\[(in\d+(, in\d+)*)?\], '
        r'\[(out\d+(, out\d+)*)?\]\)')
    patAssignInput = re.compile(r'(in\d+) = (([^(]+\.)*[^(]+)\(\)')
    patAssignOutput = re.compile(r'(out\d+) = (([^(]+\.)*[^(]+)\(\)')
    patLocation = re.compile(r'(pop\.setLocation|ex\.setExecutorLocation)(\(\s*(\'[^\']+\'|"[^"]+")\s*,\s*\d+\s*\))')
    getServerFn = 'tuple(fuzzypy.Admin().showServers(0, 1)[0][:2])'
    patModule = re.compile(r'(inputValue|pop) = ([^(]+\.)*[^(]+\(')
    patFileSuffix = re.compile(r'\.[a-zA-Z]+$')
    dataRepresentation = {'BwDocids': 'python',
                          'BwSids': 'python',
                          'BwDict': 'python',
                          'BwRefTable': 'python',
                          'BwDimFn': 'python',
                          'BwResult': 'python',
                          'BwDummyData': 'python',
                          'BwReturnCode': 'python',
                          'BwMultiValues': 'python',
                          'BwIndexResult': 'python',
                          'BwOuterTuples': 'python',
                          'InternalTableData': 'python',
                          'BwBits': 'python',
                          'BwHandle': 'python',
                          'BwARMiningData': 'binary',
                          'ParallelHashTableData': 'binary',
                          'BwAggrHashMapResultWrapper': 'binary',
                          'BwAggrHashMapResultWrapperData': 'binary',
                          'QueryPartOverviewData': 'binary',

                          # 'TableData': 'binary', # XXX ask
                          'BwFemsDict': 'python', # some comments in python trace
                          # 'BwInterDistinctRows': 'python', # ask author, incomplete python trace
                          # 'BwGNResult': 'binary', # ask author
                          # 'ReturnTableData': 'binary', # XXX ask author
                      }
    commands = ('view', 'run', 'dot', 'dotty', 'pdf', 'count',
                'mark', 'unmark', 'trace', 'list', 'delete')
    defaultCommand = 'dot'
                          
    def runHandler(self, cmd=None, arg1=None, arg2=None,
                   dotFile=defaultDotFile, runDotty=True, pdfViewer='',
                   dotViewer='',
                   includeTrace=True, traceFile=None, verbose=False,
                   addPrint=True, oneServer=None, popAsPlan=False,
                   replace=False, limitNodeLength=10000,
                   classify=False, listOperations=False,
                   removePopFlags=False, setPopFlags=[],
                   retrieve=[], stopAfter=[], planFile=defaultPlanFile,
                   onlyPop=[], replaceInput=[],
                   defaultDataRepresentation='python', dataRepresentation=[],
                   today='', keepTemporaries=False):
        self.dotFile = dotFile
        self.runDotty = runDotty
        self.pdfViewer = pdfViewer
        self.dotViewer = dotViewer
        self.includeTrace = includeTrace
        self.traceFile = traceFile
        self.verbose = int(verbose)
        self.addPrint = int(addPrint)
        self.replace = replace
        if oneServer:
            oneServer = int(oneServer)
        self.oneServer = oneServer
        self.popAsPlan = popAsPlan
        self.limitNodeLength = limitNodeLength
        self.classify = classify
        self.listOperations = listOperations
        self.removePopFlags = removePopFlags
        self.setPopFlags = setPopFlags
        self.retrieve = retrieve
        self.stopAfter = stopAfter
        self.planFile = planFile
        self.onlyPop = onlyPop
        self.replaceInput = replaceInput
        self.defaultDataRepresentation = defaultDataRepresentation
        self.today = today
        self.keepTemporaries = keepTemporaries
        if dataRepresentation:
            self.setDataRepresentation(dataRepresentation)
        if cmd not in self.commands:
            cmd, arg1, arg2 = self.guessCommand(cmd, arg1, arg2)
        ret = None
        if cmd == 'view':
            trace = self.getTransformedTrace(arg1)
            if arg2 is None:
                print trace
            elif arg2 is 'get':
                return trace
            else:
                self.writeTestFile(arg2, trace)
        elif cmd == 'run':
            ret = self.runTrace(arg1, arg2)
        elif cmd in ('dot', 'dotty'):
            self.checkArgsEmpty(cmd, ('arg2', arg2)) # assert arg2 is None
            trace = self.getTransformedTrace(arg1)
            ret = self.convertTrace(trace)
        elif cmd in ('pdf'):
            self.checkArgsEmpty('pdf', ('arg2', arg2)) # assert arg2 is None
            self.runDotty = False
            trace = self.getTransformedTrace(arg1)
            self.convertTrace(trace)
            self.convertToPdf(trace)
        elif cmd in 'count':
            trace = self.getTrace(arg1)
            parser = self.parseTrace(trace)
            parser.countConnections()
        elif cmd == 'mark':
            self.checkArgsEmpty('mark', ('arg2', arg2)) # assert arg2 is None
            ret = self.mark(arg1)
        elif cmd == 'unmark':
            # assert arg1 is None and arg2 is None
            self.checkArgsEmpty('unmarg', ('arg1', arg1), ('arg2', arg2))
            ret = self.unmark()
        elif cmd == 'trace':
            self.checkArgsEmpty('trace', ('arg2', arg2)) # assert arg2 is None
            ret = self.trace(arg1)
        elif cmd == 'list':
            self.checkArgsEmpty('list', ('arg2', arg2)) # assert arg2 is None
            ret = self.list_(arg1)
        elif cmd == 'delete':
            # assert arg1 is None and arg2 is None
            self.checkArgsEmpty('delete', ('arg1', arg1), ('arg2', arg2))
            ret = self.delete()
        else:
            self.log.write('ERROR: unknown command %s\n' % repr(cmd))
            raise testbase.TestError
        return ret

    def checkArgsEmpty(self, cmd, *arglist):
        for (name, value) in arglist:
            if value is not None:
                self.log.write('error: %s expects %s to be None (found %s)\n'
                               % (cmd, name, repr(value)))
                raise testbase.TestError

    def getTrace(self, what):
        if what is None:
            self.itemno = 0
        elif not re.match(r'-?\d+$', what):
            if not os.path.isfile(what):
                self.log.write('can not read file %s; specify either an item number, a file name or nothing (nothing means the last item in the trace file)' % repr(what))
                raise testbase.TestError
            of = open(what, 'rb')
            t = of.read()
            of.close()
            return t
        else:
            self.itemno = int(what)
        tf = TraceFile(self.getTraceFileName(), self.log)
        tf.readDirectory()
        t = tf.read(self.itemno)
        if self.itemno == 0:
           self.itemno = tf.getDirectorySize()
        return t

    def getTransformedTrace(self, arg):
        """return the python source of the referred to trace, parsed and
        rewritten if necessary"""
        trace = self.getTrace(arg)
        if self.removePopFlags or self.setPopFlags or self.retrieve \
                or self.stopAfter:
            parser = self.parseTrace(trace)
            f = cStringIO.StringIO()
            parser.writePython(f)
            trace = f.getvalue()
        return trace

    def parseTrace(self, trace):
        trace = trace.replace('\r\n', '\n')
        trace = trace.replace('\r', '\n')
        if trace.startswith(self.executePop_comment):
            trace = self.convertPopToPlan(trace)
        parser = TraceParser(self.log, includeTrace=self.includeTrace,
                             verbose=self.verbose,
                             limitNodeLength=self.limitNodeLength)
        parser.setTrace(trace)
        if self.removePopFlags:
            parser.removePopFlags()
        if self.setPopFlags:
            for s in self.setPopFlags:
                n, v = s.split('=', 1)
                parser.setPopFlags(n, int(v))
        if self.stopAfter:
            parser.clearOutputs()
        if self.retrieve:
            parser.addOutputs(self.retrieve)
        if self.stopAfter:
            parser.removeAfter(self.stopAfter)
        return parser
    
    def convertTrace(self, trace):
        dotFile = self.dotFile
        if dotFile == defaultDotFile:
            dotFile = self.checkFilename(self.dotFile)
        parser = self.parseTrace(trace)
        parser.writeDot(dotFile)
        if self.runDotty:
            os.system((self.dotViewer or 'dotty')
                      + ' \"' + dotFile + '\"')

    def convertToPdf(self, trace):
        dotFile = self.dotFile
        pdfFile = dotFile + '_' + repr(self.itemno) + '.pdf'
        if dotFile == defaultDotFile:
            dotFile = self.checkFilename(dotFile)
            pdfFile = self.checkFilename(pdfFile)
        os.system('dot -Tpdf -Gsize=\"30,30\" -o \"' + pdfFile
                  + '\" \"' + dotFile + '\"')
        if self.pdfViewer:
            os.system(self.pdfViewer + ' \"' + dotFile + '_'
                      + repr(self.itemno) + '.pdf\"')
        else:
            self.log.write('Generated pdf file: \'' + dotFile + '_'
                           + repr(self.itemno) + '.pdf\'');
    
    def runTrace(self, arg1, optionalOutput):
        if self.replaceInput and self.onlyPop:
            self.log.write('ERROR: please use either replaceInput or onlyPop, '
                           'not both\n')
            raise testbase.TestError
        isPlanRequest = (self.onlyPop or self.replaceInput) and True
        if not isPlanRequest:
            trace = self.getTransformedTrace(arg1)
            if optionalOutput:
                planFile = optionalOutput
            else:
                planFile = self.planFile
                if planFile == defaultPlanFile:
                    planFile = self.checkFilename(planFile)
            self.writeTestFile(planFile, trace, replace=True)
            os.system('python %s' % planFile)
            return
        # we should run the plan first, to determine some connector values,
        # then create the final plan from the connector values returned
        # and written to trace in the first plan run
        # (1) create (inputs, outputs, operations) for the temporary plan
        #     create a list of filenames to write the outputs to
        # (2) run the plan
        # (3) create another plan with (inputs, outputs, operations) based on
        #     the initial one
        # (4) write the final plan to the output file
        
        # parse the plan, applying transformations
        # as done in getTransformedTrace:
        trace = self.getTrace(arg1)
        parser = self.parseTrace(trace)  # applies stopAfter, setPopFlags etc
        # f = cStringIO.StringIO()
        # parser.writePython(f)
        # trace = f.getvalue()
        
        if not optionalOutput:
            self.log.write('please specify a filename for the resulting plan\n')
            raise testbase.TestError
        planFile = optionalOutput
        if not self.replace and os.path.isfile(planFile):
            self.log.write('ERROR: file %s exists - please delete it first'
                           ' or use --replace=1\n' % planFile)
            raise testbase.TestError
        parser.runPlan(planFile, self.replaceInput, self.onlyPop,
                       self.keepTemporaries, self)
        return planFile

    def checkFilename(self, filename):
        'return the filename or a derivate if the filename is not writable to'
        if os.path.sep != '/':
            return filename
        try:
            f = open(filename, 'a+')
        except IOError:
            pass
        else:
            f.close()
            return filename
        suffix = ''
        maxTries = 10
        pos = filename.rfind('.')
        if pos < 0:
            pos = len(filename)
        while True:
            filename1 = '/tmp/%s%s%s' % (filename[:pos], suffix, filename[pos:])
            try:
                f = open(filename1, 'w')
            except IOError:
                pass
            else:
                f.close()
                if self.verbose:
                    self.log.write(
                        'using filename %s because %s is not writable\n'
                        % (repr(filename1), repr(filename)))
                return filename1
            if suffix:
                i = int(suffix) + 1
                if i > 10:
                    break
                suffix = str(i)
            else:
                suffix = '1'
        msg = 'failed to write %s in temp after %d tries' % (filename, i)
        raise IOError, msg
        
    def mark(self, spec):
        if spec is None:
            itemno = 0
        else:
            itemno = int(spec)
        tf = TraceFile(self.getTraceFileName(), self.log)
        tf.readDirectory()
        tf.mark(itemno)

    def unmark(self):
        tf = TraceFile(self.getTraceFileName(), self.log)
        tf.unmark()

    def trace(self, spec):
        """turn trace on or off"""
        import fuzzypy, ServiceClientPy
        v = {'0': 0, 'off': 0, 'no': 0, 'false': 0,
             '1': 1, 'on': 1, 'yes': 1, 'true': 1}.get(
            str(spec).lower().strip(), None)
        if v is None:
            self.log.write('ERROR: illegal trace spec %s - use on or off\n'
                           % repr(spec))
        iniFile = self.getConfig()
        iniFile.setStringValue('pythontrace', 'trace', ('off', 'on')[v])
        ao = fuzzypy.Admin()
        for t in ao.showServers(0, 1):
            rs = ServiceClientPy.RemoteService(t[0] + ':' + str(t[1]))
            rs.reconfig()        

    def list_(self, what):
        """list traces in the trace file"""
        tf = TraceFile(self.getTraceFileName(), self.log)
        tf.readDirectory()
        from_ = to_ = None
        if what is 'get':
            return tf.getDirectoryListing()
        elif what is not None:
            m = re.match(r'(\d+)$', what)
            if m:
                from_ = to_ = int(m.group(1))
            m = re.match(r'(\d+)-$', what)
            if m:
                from_ = int(m.group(1))
            m = re.match(r'-(\d+)$', what)
            if m:
                to_ = int(m.group(1))
            m = re.match(r'(\d+)-(\d+)$', what)
            if m:
                from_ = int(m.group(1))
                to_ = int(m.group(2))
        if self.today and from_ is None:
            if to_ is None:
                from_, to_ = tf.getToday(self.today, result='range')
            else:
                from_ = tf.getToday(self.today)
            if self.verbose:
                self.log.write('today is %s-%s\n' % (from_, to_))
        tf.listDirectory(self.log, from_, to_,
                         self.classify, self.listOperations)

    def delete(self):
        fn = self.getTraceFileName()
        tf = TraceFile(fn, self.log)
        if (fn is not None and os.path.isfile(fn)):
            tf.unmark()
            del tf
            if self.safeToRemoveExtrace(fn):
                os.remove(fn)
            else:
                self.log.write('please delete the file manually:\nrm %s\n' % fn)
                raise testbase.TestError
    
    def safeToRemoveExtrace(self, fn):
        # We call this only interactively, from the sidadm user on the
        # command line.  Anyway some Fortify tool thinks it may be a
        # good idea to take a look at what we are deleting here,
        # because the filename comes from a configuration file which
        # some db user may modify.
        if fn.find('..') >= 0:
            return False
        if not re.match(r'[a-zA-Z0-9_+-/\\.]*$', fn):
            return False
        if fn.startswith('/dev/') or fn.startswith('/etc/'):
            return False
        basefn = os.path.basename(fn)
        if basefn.find('extrace') < 0 and not basefn.endswith('.trc'):
            return False
        f = open(fn, 'r')
        t = f.read(1024)
        if not t:
            # empty file
            return True
        for s in ('# creating trace file', '# tracing ', 'plan = '):
            if t.find(s) >= 0:
                return True
        return False

    def getConfig(self):
        executorIni = 'executor.ini'
        layer = os.getenv('DEFAULT_CONFIG_LAYER', 'CUSTOMER')
        iniFile = ConfigMgrPy.LayeredConfiguration(executorIni, getattr(ConfigMgrPy, layer))
        if not iniFile.hasSection('pythontrace'):
            self.log.write('ERROR: can not find section pythontrace in %s\n'
                           % executorIni)
            raise testbase.TestError
        return iniFile

    def setTraceFile(self, traceFile):
        self.traceFile = None

    def getTraceFileName(self):
        if self.traceFile is not None:
            return self.traceFile
        iniFile = self.getConfig()
        fn = iniFile.getStringValue('pythontrace', 'tracefile')

        # previously we specified the location of this trace file
        # relative to SAP_RETRIEVAL_PATH.  Somebody (probably for some
        # holly security reason) must have changed this to the trace
        # directory.  For the sake of change, we need to adapt the
        # code here as well...

        sap_retrieval_path = os.getenv('SAP_RETRIEVAL_PATH')
        tracedir = os.path.join(sap_retrieval_path, 'trace')
        candidate = os.path.join(tracedir, fn)
        if os.path.isfile(candidate):
            return candidate
        candidate = os.path.join(sap_retrieval_path, fn)
        return candidate

    def writeTestFile(self, filename, trace, replace=None,
                      connectorOutput=None, outputFileList=None):
        trace = trace.replace('\r\n', '\n')
        trace = trace.replace('\r', '\n')
        traceStartsWithExecutePop = trace.startswith(self.executePop_comment)
        if traceStartsWithExecutePop and self.popAsPlan:
            trace0 = trace
            trace = self.convertPopToPlan(trace)
            if trace0 is not trace:
                traceStartsWithExecutePop = False
        if not (self.oneServer is not None and not self.oneServer):
            trace = self.replaceServers(trace, self.oneServer)
        if self.addPrint or connectorOutput:
            suffix = self.getPrintSuffix(
                trace, traceStartsWithExecutePop,
                connectorOutput, outputFileList)
        if replace is None:
            replace = self.replace
        if os.path.isfile(filename) and not replace:
            self.log.write('ERROR: file %s exists - please delete it first'
                           ' or use --replace=1\n' % filename)
            raise testbase.TestError
        of = open(filename, 'w')
        modules = self.getModules(trace)
        prefix = self.import_prefix
        if modules:
            prefix = prefix.strip() + '\nimport %s\n\n' % ', '.join(modules)
        if traceStartsWithExecutePop:
            prefix += self.cmethod_l_prefix
        of.write(prefix)
        of.write(trace)
        if self.addPrint and suffix is not None:
            of.write(suffix)
        of.close()

    def getModules(self, trace):
        """return a list of additional module names to import"""
        modules = []
        haveit = set(('', 'executorPy'))
        for line in trace.split('\n'):
            m = self.patModule.match(line)
            if m and m.group(2) is not None:
                s = m.group(2)[: -1]
                if s not in haveit:
                    modules.append(s)
                    haveit.add(s)
        return modules

    def getPrintSuffix(self, trace, traceStartsWithExecutePop,
                       connectorOutput, outputFileList):
        lines = [s for s in map(string.strip, trace.split('\n')) if s]
        if not lines:
            return
        if traceStartsWithExecutePop:
            lastLine = lines[-1]
            m = self.patExecutePop.match(lastLine)
            if m:
                return self.getPrintSuffixOperation(lines, m)
        else:
            return self.getPrintSuffixPlan(lines, connectorOutput,
                                           outputFileList)

    def getPrintSuffixPlan(self, traceLines, connectorOutput, outputFileList):
        i = len(traceLines) - 2
        connectors = []
        while True:
            if i < 0:
                break
            m = self.patAddRequestedValue.match(traceLines[i])
            if not m:
                break
            connectors.append(m.group(1))
            if not connectors:
                return
            i -= 1
        j = 0
        conTypes = {}
        while j < i:
            m = self.patAssignConnector.match(traceLines[j])
            if m:
                con = m.group(1)
                if con in connectors:
                    className = m.group(3)
                    if className:
                        conTypes[con] = className
                    else:
                        classId = int(m.group(2))
                        conTypes[con] = classId
                    if len(conTypes) == len(connectors):
                        break
            j += 1
        if self.verbose:
            self.log.write('-- %d lines, i == %d, j == %d, conTypes %s\n'
                           % (len(traceLines), i, j, repr(conTypes)))
        f = cStringIO.StringIO()
        connectors.reverse()
        i = 0
        for con in connectors:
            variable = 'out%d' % i
            f.write('%s = ex.getValue(%s)\n' % (variable, con))
            conType = conTypes.get(con, None)
            self.writePlanData(f, variable, con, conType,
                               connectorOutput, outputFileList)
            i += 1
        f.write('\n')
        return f.getvalue()

    def getPrintSuffixOperation(self, traceLines, m):
        outList = m.group(4)
        f = cStringIO.StringIO()
        if m.group(1):
            f.write('if rc != 0:\n'
                    '    raise fuzzypy.error, fuzzypy.ApiError(rc)\n')
        if outList:
            varTypes = {}
            i = len(traceLines) - 2
            while i > 0:
                m = self.patAssignOutput.match(traceLines[i])
                if not m:
                    break
                varTypes[m.group(1)] = m.group(2)
                i -= 1
            for variable in outList.split(', '):
                type_ = varTypes.get(variable, None)
                # quick and dirty hack because we have the module name, too
                # in order to enable scripted operations:
                if type_ == 'executorPy.BwResult':
                    type_ = 'BwResult'
                elif type_ == 'BwResult':
                    type_ = '__main__.BwResult'
                # end quick and dirty hack
                self.writePlanData(f, variable, variable, type_, None, None)
        f.write('\n')
        return f.getvalue()

    def writePlanData(self, f, name, printName, type_,
                      connectorOutput, outputFileList):
        if self.verbose:
            self.log.write('-- writePlanData(%s, %s)\n'
                           % (repr(name), repr(type_)))
        if connectorOutput:
            r = self.getDataRepresentation(type_)
            fn = None
            if r == 'python':
                fn = self.addToFilename(connectorOutput, '_' + printName,
                                        '.py')
                d = dict(name=name, printName=printName, fn=repr(fn))
                f.write("f = open(%(fn)s, 'w')\n"
                        "f.write(%(name)s.pythonTrace('inputValue'))\n"
                        "f.close()\n"
                        % d)
            elif r == 'binary':
                fn = self.addToFilename(connectorOutput, '_' + printName,
                                        '.bin')
                d = dict(name=name, printName=printName, fn=repr(fn))
                f.write("f = open(%(fn)s, 'wb')\n"
                        "cPickle.dump(%(name)s, f, 2)\n"
                        "f.close()\n" % d)
            else:
                self.log.write('unknown data representation %s for %s\n'
                               % (repr(r), repr(printName)))
                raise Testbase.TestError
            if fn and isinstance(outputFileList, list):
                outputFileList.append(fn)
        elif type_ == 8 or type_ == 'BwResult':
            f.write('fuzzypy.writeResult(%s.getOlapResult())\n' % name)
        else:
            f.write('print %s.pythonTrace(%s)\n' % (name, repr(printName)))

    def replaceServers(self, trace, oneServer):
        if oneServer is None:
            m = self.patLocation.search(trace)
            if m:
                firstServer = m.group(2)
                while m:
                    if m.group(2) != firstServer:
                        return trace
                    m = self.patLocation.search(trace, m.end(0))
        f = cStringIO.StringIO()
        if trace.find('# determine index server') < 0:
            f.write('# determine index server:\n')
            f.write('indexServer = %s\n\n' % self.getServerFn)
        pos = 0
        m = self.patLocation.search(trace)
        while m:
            f.write(trace[pos: m.start(0)])
            f.write('%s(*indexServer)' % m.group(1))
            pos = m.end(0)
            m = self.patLocation.search(trace, pos)
        f.write(trace[pos:])
        return f.getvalue()

    def convertPopToPlan(self, trace):
        # split lines:
        lines = trace.split('\n')
        while lines and not lines[-1]:
            del lines[-1]
        while lines and not lines[0]:
            del lines[0]
        if self.verbose:
            for i in range(len(lines)):
                self.log.write('%d: %s\n' % (i, lines[i]))
        if not lines:
            return trace
        m = self.patExecutePop.match(lines[-1])
        if m is None:
            return trace
        # find inputs and outputs, their assigns in the trace and the end of
        # the variable assignments:
        s = m.group(2)
        if s:
            inputs = s.split(', ')
        else:
            inputs = []
        outputs = m.group(4).split(', ')
        if self.verbose:
            self.log.write('-- in: %s out: %s\n '
                           % (repr(inputs), repr(outputs)))
        k = len(lines) - 1
        i = k - 1
        values = {}
        m = len(inputs) + len(outputs)
        while i >= 0:
            s = lines[i]
            m = self.patAssignInput.match(s) or self.patAssignOutput.match(s)
            if m:
                name = m.group(1)
                if name not in values:
                    cls = m.group(2)
                    values[name] = (cls, i, k)
                    k = i
                    if len(values) >= m:
                        break
            i -= 1
        if len(values) < m:
            self.log.write('convertPopToPlan: can not find '
                           'types of all inputs\n')
            return trace
        if self.verbose:
            self.log.write('k %d values %s\n' % (k, values))
        # write some prefix:
        f = cStringIO.StringIO()
        f.write('# determine index server:\n')
        f.write('indexServer = %s\n\n' % self.getServerFn)
        i = 0
        if lines[0].startswith(self.executePop_comment):
            f.write(self.calculate_comment
                    + lines[0][len(self.executePop_comment):] + '\n')
            i = 1
        while i < len(lines) and lines[i].startswith('#'):
            f.write(lines[i] + '\n')
            i += 1
        f.write('ex = executorPy.PlanExecutor()\n'
                'ex.setExecutorLocation(*indexServer)\n'
                'plan = executorPy.ExecutionPlan()\n')
        # define connectors:
        connectors = {}
        j = 0
        for iname in inputs + outputs:
            cname = j < 26 and 'c_%c' % (ord('a') + j) or 'c_a%d' % (j + 1)
            cls = values[iname][0]
            connectors[iname] = cname
            if cls.startswith('executorPy.'):
                f.write('%s = executorPy.PlanConnector(plan, '
                        'executorPy.dataClassIds[%s])\n'
                        % (cname, repr(cls[11:])))
            else:
                f.write('%s = executorPy.PlanConnector(plan, '
                        'executorPy.dataClassIds[\'PythonData\'], %s)\n'
                        % (cname, repr(cls)))
            j += 1
        # write plan operation:
        while i < k:
            f.write(lines[i] + '\n')
            i += 1
        for iname in inputs:
            cname = connectors[iname]
            f.write('pop.addInput(%s)\n' % cname)
        for iname in outputs:
            cname = connectors[iname]
            f.write('pop.addOutput(%s)\n' % cname)
        f.write('plan.append(pop)\n'
                'ex.setPlan(plan)\n')
        # create inputs:
        for iname in inputs:
            (cls, i, k) = values[iname]
            while i < k:
                f.write(lines[i] + '\n')
                i += 1
            cname = connectors[iname]
            f.write('ex.setInput(%s, %s)\n' % (cname, iname))
        # add outputs:
        for iname in outputs:
            cname = connectors[iname]
            f.write('ex.addRequestedValue(%s)\n' % cname)
        f.write('ex.calculate()\n')
        return f.getvalue()

    def addToFilename(self, fn, suffix, ext=''):
        """return a new filename based on fn and suffix
        test.py + _suffix -> test_suffix.py
        test + _suffix -> test_suffix"""
        m = self.patFileSuffix.search(fn)
        if m and m.start(0) > 0:
            pos = m.start(0)
        else:
            pos = len(fn)
        if not ext:
            ext = fn[pos:]
        fn = fn[:pos] + suffix + ext
        return fn

    def setDataRepresentation(self, dataRepresentation):
        """copy the default dataRepresentation dictionary to the class instance
        and update with the specified changes"""
        self.dataRepresentation = Test.dataRepresentation.copy()
        for s in dataRepresentation:
            n, v = s.split('=', 1)
            v = v.lower()
            found = false
            for n in ('python', 'binary'):
                if v.startswith(n):
                    assert not found
                    v = n
                    found = True
            assert found
            self.dataRepresentation[n] = v

    def getDataRepresentation(self, cls):
        """return the data representation for a plan data class"""
        return self.dataRepresentation.get(cls, self.defaultDataRepresentation)

    def guessCommand(self, cmd, arg1, arg2):
        # attempt to guess a command:
        # python viewplan.py --classify=1 # command list missing
        # python viewPlan.py 204 # command dot missing
        # python viewPlan.py 26 plan_25.py --popAsPlan=1 # command view missing
        # python viewPlan.py # command dot missing
        cmdWas = cmd
        if self.popAsPlan and not arg2:
            if self.verbose:
                self.log.write('guess 1\n')
            arg2 = arg1
            arg1 = cmd
            cmd = 'view'
        elif cmd is not None and not arg2:
            if self.verbose:
                self.log.write('guess 2\n')
            if (re.match(r'\d+$', cmd) # plain number
                or not re.match(r'[a-zA-Z]+$', cmd) # try only plain names
                or os.path.isfile(cmd)): # to be a file, anything else w/o tst
                # the cmd is assumed to be an arg:
                arg2 = arg1
                arg1 = cmd
                if self.verbose:
                    self.log.write('using %s as an arg\n' % repr(cmd))
            elif self.verbose:
                self.log.write('discarding %s\n' % repr(cmd))
            cmd = self.defaultCommand
        elif cmd is None:
            if self.verbose:
                self.log.write('guess 3\n')
            if self.classify or self.today or self.listOperations:
                cmd = 'list'
            else:
                cmd = self.defaultCommand
        if cmd != cmdWas:
            if arg2 is not None:
                l = [cmd, arg1, arg2]
            elif arg1 is not None:
                l = [cmd, arg1]
            else:
                l = [cmd]
            scmd = ' '.join(repr(i) for i in l)
            self.log.write('Error: %s is not a command to viewPlan.py\n'
                           'guessing command %s\n' % (repr(cmdWas), scmd))
        return cmd, arg1, arg2

class TraceFile:
    titlePat= re.compile(r'(calculate|executePop) call \(thread (\d+)\) at ')
    planIdPat = re.compile(r'# id (<Executor |<plan_|plan)(\d+)( at |@)([^:>]+:\d+)>?(, parent (<Executor |<plan_|plan)(\d+)( at |@)([^:>]+:\d+)>?( pop(\d+))?)?')
    abbrevMethods = {'calculate': 'cal', 'executePop': 'pop', 'popArgs': 'arg'}
    def __init__(self, filename, log=sys.stdout):
        self.filename = filename
        self.log = log
        self.markFile = os.path.join(os.getenv('SAP_RETRIEVAL_PATH'), 'trace',
                                     'viewPlan.mark')

    def readDirectory(self):
        """set self.directory [(startpos, pos, title)], self.file"""
        self.directory = []
        self.file = open(self.filename, 'rb')
        pos = self.getMark()
        self.file.seek(pos)
        s = self.file.readline()
        title = None
        startpos = None
        prefix = '# tracing '
        prefixLen = len(prefix)
        while s:
            nextpos = self.file.tell()
            nextline = self.file.readline()
            if s.startswith(prefix):
                if startpos is not None:
                    self.directory.append((startpos, pos, title))
                startpos = pos
                title = s[prefixLen:].strip()
                m = self.titlePat.match(title)
                if m:
                    methodName = m.group(1)
                    methodName = self.abbrevMethods.get(methodName, methodName)
                    threadId = m.group(2)
                    when = title[m.end(0):]
                    m = self.planIdPat.match(nextline)
                    if m:
                        executorNr = m.group(2)
                        planServer = m.group(4)
                        if methodName == 'cal' and m.group(5):
                            parentExecutorNr = m.group(7)
                            parentServer = m.group(9)
                            if parentServer == planServer:
                                title = 'sub %s.%s.%s at %s' % (parentServer, parentExecutorNr, executorNr, when)
                            else:
                                title = 'sub %s.%s.%s.%s at %s' % (parentServer, parentExecutorNr, planServer, executorNr, when)
                        else:
                            title = ('%s %s.%s at %s'
                                     % (methodName, planServer, executorNr, when))
            pos = nextpos
            s = nextline
        if startpos is not None:
            endpos = self.file.tell()
            self.directory.append((startpos, endpos, title))

    def getDirectorySize(self):
        return len(self.directory)

    def listDirectory(self, of, from_ = None, to_=None,
                      classify=False, listOperations=False):
        if from_ is None:
            from_ = 1
        if to_ is None:
            to_ = len(self.directory)
        if to_ > len(self.directory):
            to_ = len(self.directory)
        for i in xrange(from_ - 1, to_):
            (startpos, endpos, title) = self.directory[i]
            of.write('%d: %s\n' % (i + 1, title))
            if classify or listOperations:
                t = self.read(i + 1)
                popClasses, popCount, lines = self.getPopClasses(t)
                if classify:
                    of.write(' ' * (len('%d' % (i + 1)) + 2))
                    of.write(self.classify(popClasses))
                    of.write(', ')
                of.write('%d pops, %d lines' % (popCount, lines))
                if listOperations:
                    of.write(', class %s' % ', '.join(popClasses))
                of.write('\n')

    def getDirectoryListing(self):
        ret = []
        for i in xrange(0, len(self.directory)):
            (startpos, endpos, title) = self.directory[i]
            ret.append('%s\n' % (title))
        return ret

    def mark(self, keepItems):
        if keepItems == 0:
            if len(self.directory) == 0:
                self.unmark()
                return
            startpos, endpos, title = self.directory[-1]
            markPos = endpos
        else:
            itemno = len(self.directory) - keepItems
            if itemno < 0:
                self.log.write('ERROR: only see %d items\n'
                               % len(self.directory))
                raise testbase.TestError
            startpos, endpos, title = self.directory[itemno]
            markPos = startpos
        of = open(self.markFile, 'wb')
        of.write('%d\n' % markPos)
        of.close()

    def unmark(self):
        if (os.path.isfile(self.markFile)):
            os.remove(self.markFile)

    def read(self, itemno):
        (startpos, endpos, title) = self.directory[itemno - 1]
        # self.log.write('### item %d startpos %d endpos %d title %s\n'
        #                % (itemno, startpos, endpos, title.strip()))
        self.file.seek(startpos)
        buf = self.file.read(endpos - startpos)
        assert len(buf) == endpos - startpos
        return buf

    def getMark(self):
        if not os.path.isfile(self.markFile):
            return 0
        markPos = int(open(self.markFile, 'rb').read().strip())
        return markPos

    def getPopClasses(self, t):
        """return a unique list of pop classes and the number of operations
        for some plan trace"""
        n = 0
        pat = TraceParser.pat1
        classes = set()
        lines = 0
        for s in t.split('\n'):
            lines += 1
            m = pat.match(s)
            if m:
                classes.add(m.group(2))
                n += 1
        return tuple(classes), n, lines

    def classify(self, classes):
        """return a unique classification for a list of pop types,
        current values: olap, join, ptime, kalk, mixed, unknown"""
        t = set()
        for cls in classes:
            if cls in ('BwPopJoin1Inwards', 'BwPopJoin13',
                       'BwPopAggregateParallel', 'BwPopJoin3Inwards'):
                t.add('olap')
            elif cls in ('JECreateNTuple', 'JERequestedAttributes',
                         'JECaptureIndex', 'JEAssembleResults',
                         'JEEvalPrecond'):
                t.add('join')
            elif cls == 'RowPlanOperator':
                t.add('ptime')
            elif cls.startswith('ce') and cls.endswith('Pop'):
                t.add('kalk')
        if len(t) == 1:
            return t.pop()
        elif len(t) > 1:
            return 'mixed'
        else:
            return 'unknown'

    def getToday(self, today, result='range'):
        """if result == 'range' return (startIndex, endIndex) else startIndex
        indexes start with 1, to be used as inputs to listDirectory()"""

        # calculate string representation of the given today or current date:
        m = re.match(r'(\d+)-(\d+)-(\d+)', str(today))
        if m:
            y = int(m.group(1))
            mon = int(m.group(2))
            d = int(m.group(3))
        else:
            tm = time.localtime(time.time())
            y = tm.tm_year
            mon = tm.tm_mon
            d = tm.tm_mday
        ds = '%04d-%02d-%02d' % (y, mon, d)

        # search for start and end:
        patDate = re.compile(r' at (\d{4}-\d\d-\d\d)')
        i = 0
        n = len(self.directory)
        while i < n:
            m = patDate.search(self.directory[i][2])
            i += 1
            if m and m.group(1) >= ds:
                if m.group(1) > ds:
                    break
                start = i
                if result == 'start':
                    return start
                while i < n:
                    m = patDate.search(self.directory[i][2])
                    if m and m.group(1) > ds:
                        break
                    i += 1
                return start, i
        if result == 'start':
            return n + 1
        elif result == 'range':
            return n + 1, n + 1
        raise ValueError, 'unexpected result ' + str(result)

class TraceParser:
    pat1 = re.compile(r'pop = (executorPy\.)?([^(]+).*?# pop(\d+)')
    pat2 = re.compile(r'plan.append\(pop\)')
    pat3 = re.compile(r'([a-z][a-z0-9_]*) = executorPy.PlanConnector\(plan(, ((\d+)|executorPy\.dataClassIds\[\'([^\']*)\'\]))?\)( # executorPy\.dataClassIds\[\'([^\']*)\'\])?') # group 4: class id; group 5: class name from assignment; group 7: class name from comment
    pat4 = re.compile(r'pop\.addInput\(([a-z][a-z0-9_]*)\)')
    pat4a = re.compile(r'pop\.setInputs\(\[([^)]+)\]\)')
    pat5 = re.compile(r'pop\.addOutput\(([a-z][a-z0-9_]*)\)')
    pat5a = re.compile(r'pop\.setOutputs\(\[([^)]+)\]\)')
    pat6 = re.compile(r'ex\.setInput\(([a-z][a-z0-9_]*), inputValue\)')
    pat7 = re.compile(r'ex\.addRequestedValue\(([a-z][a-z0-9_]*)\)')
    pat8 = re.compile(r'inputValue = ([^(.]+\.)*([^(]+)')
    pat9 = re.compile(r'pop\.setLocation\(\'([^\']+)\',\s+(\d+)\)')
    pat10 = re.compile(r'ex\.setExecutorLocation\(\'([^\']+)\',\s+(\d+)\)')
    pat11 = re.compile(r'pop\d*\.setPopFlags\(')
    pat12 = re.compile(r'(c_)?([a-z]|a\d+)$')
    pat13 = re.compile(r'(pop)?\d+$')
    reportTruncatedLines = 3

    def __init__(self, log=sys.stdout, includeTrace=0, verbose=0, limitNodeLength=None):
        self.log = log
        self.includeTrace = includeTrace
        self.verbose = verbose
        self.limitNodeLength = limitNodeLength
        self.truncatedLineCount = 0

    def setTrace(self, traceText):
        self.initParse()
        lines = traceText.split('\n')
        for line in lines:
            self.parseLine(line.strip())
        self.finishParse()

    def initParse(self):
        self.popName = 'pop'
        self.popClass = 'PlanOperation'
        self.inputs = []
        self.outputs = []
        self.globalInputs = []
        self.globalOutputs = []
        self.plan = []
        self.popTrace = []
        self.popServer = None
        self.inputClass = None
        self.inputTrace = []
        self.locations = {}
        self.executorLocation = None
        self.connectorClasses = {}
        self.prefixLines = []

    def parseLine(self, line):
        if line.startswith('# tracing ') or line.startswith('# id '):
            self.prefixLines.append(line)
            return
        m = self.pat10.match(line)
        if m:
             self.executorLocation = m.group(1) + ':' + m.group(2)
        m = self.pat1.match(line)
        if m:
            popClass = m.group(2)
            popId = m.group(3)
            self.startPop(popClass, popId)
            return
        m = self.pat2.match(line)
        if m:
            self.finishPop()
            return
        m = self.pat3.match(line)
        if m:
            newConnector = m.group(1)
            className = None
            classId = None
            if m.group(4):
                classId = int(m.group(4))
            if m.group(5):
                className = m.group(5)
            elif m.group(7):
                className = m.group(7)
            if not className and classId is not None:
                className = self.getPdataClassName(classId)
            if className:
                self.connectorClasses[newConnector] = className
            return
        m = self.pat4.match(line)
        if m:
            connector = m.group(1)
            self.inputs.append(connector)
            return
        m = self.pat4a.match(line)
        if m:
            self.inputs = [s.strip() for s in m.group(1).split(',') if s.strip()]
            return
        m = self.pat5.match(line)
        if m:
            connector = m.group(1)
            self.outputs.append(connector)
            return
        m = self.pat5a.match(line)
        if m:
            self.outputs = [s.strip() for s in m.group(1).split(',') if s.strip()]
            return
        m = self.pat6.match(line)
        if m:
            connector = m.group(1)
            self.globalInputs.append(
                (connector, self.inputClass, self.inputTrace))
            self.inputClass = None
            self.inputTrace = []
            return
        m = self.pat7.match(line)
        if m:
            connector = m.group(1)
            self.globalOutputs.append(connector)
            return
        m = self.pat8.match(line)
        if m and m.group(1) != 'cPickle.':
            self.inputClass = m.group(2)
            modules = m.group(1)
            if modules and modules != 'executorPy.':
                self.inputClass = modules + self.inputClass
            self.inputTrace = []
            return
        m = self.pat9.match(line)
        if m:
            self.popServer = m.group(1) + ':' + m.group(2)
        if line.startswith('pop.'):
            line = self.popName + line[3:]
        elif self.inputClass is not None:
            self.inputTrace.append(line)
        self.popTrace.append(line)

    def startPop(self, popClass, popId):
        self.popName = 'pop%d' % int(popId)
        self.popClass = popClass
        self.inputs = []
        self.outputs = []
        self.popTrace = []
        self.popServer = None

    def finishPop(self):
        self.plan.append((self.popName, self.popClass,
                          self.inputs, self.outputs, self.popTrace))
        if self.popServer:
            self.locations[self.popName] = self.popServer
        self.inputs = []
        self.outputs = []
        self.popTrace = []

    def finishParse(self):
        del self.popName, self.popClass, \
            self.inputs, self.outputs, self.popTrace
##         for t in self.plan:
##             print t
##         print self.globalInputs, self.globalOutputs

    def writeDot(self, outputFilename):
        if not self.plan and not self.globalInputs and not self.globalOutputs:
            self.log.write('error: no plan\n')
            raise testbase.TestError
        self.truncatedLineCount = 0
        self.of = open(outputFilename, 'w')
        self.writePrefix()
        self.writePops()
        self.calculateProducers()
        self.writeGlobalInputs()
        self.writeConnectInputs()
        self.writeGlobalOutputs()
        self.writeSuffix()
        if self.truncatedLineCount > self.reportTruncatedLines:
            self.log.write(
                '%d more lines truncated\n'
                % (self.truncatedLineCount - self.reportTruncatedLines))
        self.reportTruncatedLines = 0
        self.of.close()
        del self.of

    def writePrefix(self):
        self.of.write('digraph G {\n')

    def writeSuffix(self):
        self.of.write('}\n')

    def writePops(self):
        for (popName, popClass, inputs, outputs, trace_) in self.plan:
            if self.includeTrace:
                if self.includeTrace > 3:
                    origTrace = trace_
                    trace_ = []
                    for l in origTrace:
                        if len(l) > self.includeTrace:
                            l = l[: self.includeTrace - 3] + '...'
                        trace_.append(l)
                # the order of inputs is on the lines of the graph; the order
                # of the outputs will be displayed inside the node
                if len(outputs) > 1:
                    trace_ = (list(trace_)
                              + ['outputs: %s'
                                 % ', '.join([self.shortConnector(con)
                                              for con in outputs])])
                self.of.write('%s [shape=record, label="%s %s\\l%s"]\n'
                              % (popName, popClass, popName,
                                 self.quoteTrace(trace_)))
            else:
                self.of.write('%s [shape=box,label="%s %s"]\n'
                              % (popName, popClass, popName))

    def writeConnectInputs(self):
        globalInputConnectors = set()
        for (connector, class_, trace_) in self.globalInputs:
            globalInputConnectors.add(connector)
        for (popName, popClass, inputs, outputs, trace) in self.plan:
            k = 0
            for connector in inputs:
                producer = self.producerOf.get(connector, connector)
                if connector in globalInputConnectors:
                    cs = self.shortConnector(connector)
                else:
                    cs = self.shortConnectorAndClass(connector)
                if len(inputs) > 1:
                    cs = '%s (input %d)' % (cs, k)
                self.of.write('%s -> %s [label="%s"]\n'
                              % (producer, popName, cs))
                k += 1

    def writeGlobalInputs(self):
        for (connector, class_, trace_) in self.globalInputs:
            if self.includeTrace:
                for i in xrange(len(trace_)):
                    if trace_[i].startswith('inputValue.'):
                        trace_[i] = (self.shortConnector(connector)
                                     + trace_[i][10:])
                    if self.includeTrace > 3:
                        if len(trace_[i]) > self.includeTrace:
                            trace_[i] = (trace_[i][: self.includeTrace - 3]
                                         + '...')
                self.of.write('%s [shape=ellipse, color=yellow, style=filled, '
                              'label="%s %s\\l%s"]\n'
                              % (connector, class_,
                                 self.shortConnector(connector),
                                 self.quoteTrace(trace_)))
            else:
                self.of.write('%s [shape=circle, color=yellow, style=filled, label="%s"]\n'
                              % (connector, self.shortConnectorAndClass(connector)))
        for connector in self.globalOutputs:
            self.of.write('%s [shape=ellipse, color=orange, style=filled, label="%s"]\n'
                          % (connector, self.shortConnectorAndClass(connector)))

    def writeGlobalOutputs(self):
        for connector in self.globalOutputs:
            producer = self.producerOf.get(connector, connector)
            self.of.write('%s -> %s [label="%s"]\n'
                          % (producer, connector,
                             self.shortConnector(connector)))

    def calculateProducers(self):
        self.producerOf = {}
        for (popName, popClass, inputs, outputs, trace) in self.plan:
            for output in outputs:
                self.producerOf[output] = popName
        
    def shortConnector(self, connector):
        s = connector
        if connector.startswith('c_') and connector[2:3].isalpha():
            s = connector[2:]
        return s

    def shortConnectorAndClass(self, connector):
        s = self.shortConnector(connector)
        className = self.connectorClasses.get(connector, None)
        if className:
            s = className + ' ' + s
        return s

    def quoteTrace(self, traceLines):
        f = cStringIO.StringIO()
        pat = re.compile(r'[ "{}|\\<>&]')
        for line in traceLines:
            m = pat.search(line)
            pos = 0
            while m:
                c = m.group(0)
                assert len(c) == 1
                f.write(line[pos: m.start(0)])
                if c in ('&',):
                    f.write('&#%d;' % ord(c))
                elif c == '\\':
                    f.write('\\\\\\\\')
                else:
                    f.write('\\%s' % c)
                pos = m.end(0)
                m = pat.search(line, pos)
            f.write(line[pos:])
            f.write('\\l')
        s = f.getvalue()
        if self.limitNodeLength and len(s) > self.limitNodeLength:
            self.truncatedLineCount += 1
            if self.truncatedLineCount <= self.reportTruncatedLines:
                self.log.write('oversize line truncated to %d: %s\n'
                               % (self.limitNodeLength, repr(s[:16] + '...')))
            while True:
                pos = s[:-2].rfind('\\l')
                if pos < 0:
                    s = '...\\l'
                    break
                if pos > self.limitNodeLength - 10:
                    s = s[:pos + 2]
                else:
                    s = s[:pos] + '\\l...\\l'
                    break
        return s

    def countConnections(self):
        produce = {}
        consume = {}
        connections = {}
        classes = {}
        defaultLocation = self.executorLocation or 'localhost:30003'
        popsWithoutLocation = []
        for (popName, popClass, inputs, outputs, trace) in self.plan:
            try:
                popServer = self.locations[popName]
            except KeyError:
                popsWithoutLocation.append(popName)
                popServer = defaultLocation
            connections[popServer] = 0
            m = consume.get(popServer, None)
            if m is None:
                m = consume[popServer] = {}
            for con in inputs:
                m[con] = None
            for con in outputs:
                produce[con] = popServer
                classes[con] = popClass
        if popsWithoutLocation:
            self.log.write('pops %s not found in locations %s\n'
                           % (repr(popsWithoutLocation), repr(self.locations)))
        if self.executorLocation:
            for (con, class_, trace_) in self.globalInputs:
                produce[con] = self.executorLocation
                classes[con] = 'plan'
            m = consume.get(self.executorLocation, None)
            if m is None:
                m = consume[self.executorLocation] = {}
            for con in self.globalOutputs:
                m[con] = None
        servers = connections.keys()
        servers.sort()
        totalConnections = 0
        classified = {}
        for receiver in servers:
            for con in consume[receiver].keys():
                try:
                    sender = produce[con]
                except KeyError:
                    self.log.write('no producer for %s\n' % con)
                    continue
                if sender != receiver:
                    if self.verbose:
                        popClass = classes.get(con, '')
                        if popClass:
                            classified[popClass] = classified.get(popClass,
                                                                  0) + 1
                            popClass = ' (%s)' % popClass
                        self.log.write('%s %s to %s%s\n'
                                       % (con, sender, receiver, popClass))
                    connections[sender] += 1
                    connections[receiver] += 1
                    totalConnections += 1
        if self.verbose:
            self.log.write('\n')
            l = classified.keys()
            l.sort()
            for n in l:
                self.log.write('%-4d %s\n' % (classified[n], n))
            self.log.write('\n')
        for server in servers:
            self.log.write('%-4d %s\n' % (connections[server], server))
        self.log.write('%-4d\n' % totalConnections)
        return totalConnections

    def removePopFlags(self):
        removedLineCount = 0
        for (popName, popClass, inputs, outputs, trace) in self.plan:
            for i in xrange(len(trace) - 1, -1, -1):
                if self.pat11.match(trace[i]):
                    del trace[i]
                    removedLineCount += 1
        if self.verbose:
            self.log.write('removePopFlags removed %d lines of trace\n'
                           % removedLineCount)

    def setPopFlags(self, where, value):
        for popIndex in self.getPopIndexes(where):
            popName, popClass, inputs, outputs, trace = self.plan[popIndex]
            for i in xrange(len(trace) - 1, -1, -1):
                if self.pat11.match(trace[i]):
                    del trace[i]
                    break
            if value is None:
                return
            trace.insert(max(i, 0), '%s.setPopFlags(%d)' % (popName, value))

    def getPopIndexes(self, where):
        """return a list of indexes to plan operations in self.plan
        accept a list or a single pop or connector or pop class substring"""


        if not isinstance(where, list):
            where = [where]
        connectorNames = []
        popNames = []
        searchTexts = []
        indexes = []
        for s in where:
            s = s.lower()
            if self.pat12.match(s):
                if not s.startswith('c_'):
                    s = 'c_' + s
                connectorNames.append(s)
            elif self.pat13.match(s):
                popNames.append(s)
            else:
                searchTexts.append(s)
        foundSearches = set()
        for i in xrange(len(self.plan)):
            popName, popClass, inputs, outputs, trace = self.plan[i]
            found = popName in popNames
            if not found and connectorNames:
                for c in outputs:
                    if c in connectorNames:
                        found = True
                        break
            if not found and searchTexts:
                for s in searchTexts:
                    if popClass.lower().find(s) >= 0:
                        foundSearches.add(s)
                        found = True
            if found:
                indexes.append(i)
        for s in foundSearches:
            searchTexts.remove(s)
        if searchTexts:
            # allow to specify a substring of connectors and search
            # in connector class names after searching pop class names
            for i in xrange(len(self.plan)):
                popName, popClass, inputs, outputs, trace = self.plan[i]
                found = False
                for c in outputs:
                    className = self.connectorClasses.get(c, '').lower()
                    for s in searchTexts:
                        if className.find(s) >= 0:
                            found = True
                            break
                if found and i not in indexes:
                    indexes.append(i)
        if self.verbose:
            self.log.write('getPopIndexes found %s\n'
                           % ', '.join(self.plan[i][0] for i in indexes))
        return indexes

    def getConnectors(self, where):
        """return a list of connector names
        accept a list of either connector names, pop names
        or substrings of connector class or pop class
        pops are converted to their list of output connectors, the result
        does not contain duplicates but is not in any particular order"""
        connectorSet = set()
        popNames = []
        searchTexts = []
        for s in where:
            s = s.lower()
            if self.pat12.match(s):
                if not s.startswith('c_'):
                    s = 'c_' + s
                connectorSet.add(s)
            elif self.pat13.match(s):
                popNames.append(s)
            else:
                if s not in searchTexts:
                    searchTexts.append(s)
        if popNames:
            for popName, popClass, inputs, outputs, trace in self.plan:
                if popName.lower() in popNames:
                    for c in outputs:
                        connectorSet.add(c)
                    break
        if searchTexts:
            founds = set()
            for c in self.getUsedConnectors(False):
                className = self.connectorClasses.get(c, '').lower()
                found = False
                for s in searchTexts:
                    if className.find(s) >= 0:
                        found = True
                        founds.add(s)
                        break
                if found:
                    connectorSet.add(c)
            for s in founds:
                searchTexts.remove(s)
        if searchTexts:
            for popName, popClass, inputs, outputs, trace in self.plan:
                popClass = popClass.lower()
                for s in searchTexts:
                    if popClass.find(s) >= 0:
                        for c in outputs:
                            connectorSet.add(c)
                        break
        return list(connectorSet)

    def getUsedConnectors(self, sort=True):
        """return a sorted list of all connectors used in all pops"""
        connectorSet = set()
        for popName, popClass, inputs, outputs, trace in self.plan:
            for c in inputs:
                connectorSet.add(c)
            for c in outputs:
                connectorSet.add(c)
        connectorList = list(connectorSet)
        if sort:
            connectorList = self.sortConnectors(connectorList)
        return connectorList

    def sortConnectors(self, connectorList):
        shortConnectors = [c for c in connectorList if len(c) <= 3]
        longIds = [int(c[3:]) for c in connectorList if len(c) > 3]
        shortConnectors.sort()
        longIds.sort()
        return shortConnectors + ['c_a%d' % c for c in longIds]
        
    def writePython(self, f):
        """synthesize the python source parsed into this object"""
        # we need to write back to python since we want to modify operations
        # (add or remove flags) and plan structure (reconnect inputs and
        # outputs)

        # plan id:
        for line in self.prefixLines:
            f.write(line + '\n')
        f.write('ex = executorPy.PlanExecutor()\n')
        f.write('ex.setExecutorLocation(')
        if self.executorLocation:
            host, port = self.executorLocation.split(':')
            f.write('%s, %s' % (repr(host), port))
        else:
            f.write('*indexServer')
        f.write(')\n')
        f.write('plan = executorPy.ExecutionPlan()\n')
        # connector list:
        for c in self.getUsedConnectors():
            # XXX insert connectors 'not used in plan', or not (maybe rename)
            f.write('%s = executorPy.PlanConnector(plan' % c)
            connectorClass = self.connectorClasses.get(c, None)
            if c:
                f.write(', executorPy.dataClassIds[%s]' % repr(connectorClass))
            f.write(')\n')
        # pops:
        for popName, popClass, inputs, outputs, trace in self.plan:
            # XXX executorPy. is not general, empty constructor ist not either
            f.write('pop = executorPy.%s() # %s\n' % (popClass, popName))
            for s in trace:
                if s.startswith(popName + '.'):
                    s = 'pop' + s[len(popName):]
                f.write(s + '\n')
            for c in inputs:
                f.write('pop.addInput(%s)\n' % c)
            for c in outputs:
                f.write('pop.addOutput(%s)\n' % c)
            f.write('plan.append(pop)\n')
        # call calculate:
        f.write('ex.setPlan(plan)\n')
        for connector, inputClass, trace in self.globalInputs:
            # XXX input values may have classes not defined in executorPy
            f.write('inputValue = executorPy.%s()\n' % inputClass)
            for line in trace:
                f.write(line + '\n')
            f.write('ex.setInput(%s, inputValue)\n' % connector)
        for c in self.globalOutputs:
            f.write('ex.addRequestedValue(%s)\n' % c)
        f.write('ex.calculate()\n\n')

    def addOutputs(self, outputs):
        """append user supplied connectors to requested value list"""
        for c in self.sortConnectors(self.getConnectors(outputs)):
            if not c in self.globalOutputs:
                self.globalOutputs.append(c)

    def clearOutputs(self):
        self.globalOutputs = []

    def removeAfter(self, where):
        """remove all pops not required from the plan.
        required are pops producing stuff in self.globalOutputs and the
        pops or connectors or pop class substrings specified in where"""

        requiredPops = set(self.getPopIndexes(where))
        if self.globalOutputs:
            # add pops producing globalOutputs to the list of required pops:
            for i in xrange(len(self.plan)):
                popName, popClass, inputs, outputs, trace = self.plan[i]
                found = False
                for c in self.globalOutputs:
                    if c in outputs:
                        found = True
                        break
                if found:
                    requiredPops.add(i)
        else:
            # add the outputs of this operation to the list of required outputs:
            for i in requiredPops:
                popName, popClass, inputs, outputs, trace = self.plan[i]
                for c in outputs:
                    if c not in self.globalOutputs:
                        self.globalOutputs.append(c)
        self.calculateProducerIndexes()
        requiredPops, usedConnectors = self.addPopsBefore(requiredPops)

        # XXX we could add connectors produced but not consumed
        # to self.globalInputs if self.globalInputs was empty before
        # (meaning that the user has not specified --retrieve=something)

        self.removeOtherPops(requiredPops)
        self.removeOtherGlobalInputs(usedConnectors)

    def removeOtherGlobalInputs(self, usedConnectors):
        """remove input values not required for the remaining plan operations
        usedConnectors is a set of connector names"""
        for i in xrange(len(self.globalInputs) - 1, -1, -1):
            connector, inputClass, trace = self.globalInputs[i]
            if connector not in usedConnectors:
                del self.globalInputs[i]

    def removeOtherPops(self, requiredPops):
        """remove pops not in requiredPops, a list of indexes to self.plan"""
        for i in xrange(len(self.plan) - 1, -1, -1):
            if i in requiredPops:
                continue
            if self.verbose:
                popName, popClass, inputs, outputs, trace = self.plan[i]
                self.log.write('removing %s\n' % popName)
            del self.plan[i]

    def popsBeforeGlobalOutputs(self):
        """return the pops and connectors required for the subset of
        this plan that calculates self.globalOutputs"""

        requiredPops = set()
        for c in self.globalOutputs:
            i = self.producerIndexOf[c]
            if i not in requiredPops:
                requiredPops.add(i)
        return self.addPopsBefore(requiredPops)
        
    def addPopsBefore(self, requiredPops):
        """requiredPops is a set of indexes into self.pops,
        modified and returned here
        append all pops connected to requiredPops
        return (requiredPops, set of connectors used"""

        self.calculateProducerIndexes()
        pops = list(requiredPops)
        usedConnectors = set(self.globalOutputs)
        while pops:
            i = pops.pop()
            popName, popClass, inputs, outputs, trace = self.plan[i]
            for c in inputs:
                usedConnectors.add(c)
                if self.verbose:
                    self.log.write('scanning inputs of %s\n' % popName)
                i = self.producerIndexOf.get(c, None)
                if self.verbose and i is None:
                    self.log.write('producer of %s not found\n' % c)
                if i is not None and i not in requiredPops:
                    if self.verbose:
                        self.log.write('adding pop index %d\n' % i)
                    requiredPops.add(i)
                    pops.append(i)
        return requiredPops, usedConnectors

    def calculateProducerIndexes(self):
        self.producerIndexOf = {}
        for i in xrange(len(self.plan)):
            popName, popClass, inputs, outputs, trace = self.plan[i]
            for c in outputs:
                self.producerIndexOf[c] = i

    def getPdataClassName(self, classId):
        import executorPy
        for (n, v) in executorPy.dataClassIds.items():
            if v == classId:
                return n

    def fixProducerOf(self, c):
        """replace the output connector c in the producer pop of c
        with a new connector to disconnect that operation (partially)
        from the rest"""

        i = self.producerIndexOf[c]
        popName, popClass, inputs, outputs, trace = self.plan[i]
        outputs.remove(c)
        self.plan[i] = popName, popClass, inputs, outputs, trace
        
    def runPlan(self, outputFilename, replaceInput, onlyPop, keepTemporaries,
                planWriter):
        """run a prequery to determine connector values, then create
        a plan containing the predetermined valuesm
        write it to outputFilename
        What to calculate in the pre query is determined by either
        replaceInput: a list of names evaluating to connectors to replace
        onlyPop: a list of names evaluating to plan operations"""

        # determine connectors for pre query:
        if replaceInput:
            inputConnectors = set(self.getConnectors(replaceInput))
        else: # onlyPop:
            inputConnectors = set()
            self.globalOutputs = []
            for i in self.getPopIndexes(onlyPop):
                popName, popClass, inputs, outputs, trace = self.plan[i]
                inputConnectors.update(set(inputs))
                for c in outputs:
                    if c not in self.globalOutputs:
                        self.globalOutputs.append(c)
        for t in self.globalInputs:
            inputConnectors.discard(t[0])
        self.calculateProducerIndexes()
        tempFiles = []
        if inputConnectors:
            self.save()
            self.globalOutputs = inputConnectors
            requiredPops, usedConnectors = self.popsBeforeGlobalOutputs()
            self.removeOtherPops(requiredPops)
            self.removeOtherGlobalInputs(usedConnectors)
            fn = planWriter.addToFilename(outputFilename, '_prepare_inputs')
            outputFileList = []
            self.writePlan(planWriter, fn, connectorOutput=outputFilename,
                           outputFileList=outputFileList)
            tempFiles.append(fn)
            os.system('python %s' % fn)
            self.restore()
            missingFiles = [fn1 for fn1 in outputFileList
                            if not os.path.isfile(fn1)]
            if missingFiles:
                self.log.write('--\n'
                               'ERROR: The previous execution of %s failed'
                               ' to produce the expected output files %s.\n'
                               'Please check for error output above.\n'
                               % (repr(fn), repr(missingFiles)))
                raise testbase.TestError
            for c in inputConnectors:
                self.fixProducerOf(c) # replace output in producer with new con
                cls = self.connectorClasses.get(c, None)
                r = planWriter.getDataRepresentation(cls)
                if r == 'python':
                    # read the python trace of the output and put into trace
                    fn = planWriter.addToFilename(outputFilename, '_' + c,
                                                  '.py')
                    f = open(fn, 'r')
                    trace = f.read().split('\n')
                    f.close()
                    tempFiles.append(fn)
                    assert trace[0].startswith('inputValue = ')
                    trace = trace[1:]
                    while trace and not trace[-1]:
                        trace.pop()
                elif r == 'binary':
                    fn = planWriter.addToFilename(outputFilename, '_' + c,
                                                  '.bin')
                    trace = ["f = open(%s, 'rb')" % repr(fn),
                             'inputValue = cPickle.load(f)',
                             'f.close()']
                else:
                    self.log.write('unknown data representation %s for %s\n'
                                   % (repr(r), repr(c)))
                    raise Testbase.TestError
                self.globalInputs.append((c, cls, trace))
        suppliedInputs = set()
        for t in self.globalInputs:
            suppliedInputs.add(t[0])
        for i in xrange(len(self.globalOutputs) - 1, -1, -1):
            if self.globalOutputs[i] in suppliedInputs:
                del self.globalOutputs[i]
        requiredPops, usedConnectors = self.popsBeforeGlobalOutputs()
        self.removeOtherPops(requiredPops)
        self.removeOtherGlobalInputs(usedConnectors)
        self.writePlan(planWriter, outputFilename)
        if not keepTemporaries:
            for fn in tempFiles:
                if self.verbose:
                    self.log.write('removing %s\n' % fn)
                os.remove(fn)

    def writePlan(self, planWriter, filename, **keys):
        """delegate writing the plan to planWriter, the Test instance,
        because adding prefix and suffix to the plan is implemented
        on a higher level in that class (without deep analysis of the
        plan as we have here), and i did not want to duplicate or replace
        the other code"""

        f = cStringIO.StringIO()
        self.writePython(f)
        trace = f.getvalue()
        planWriter.writeTestFile(filename, trace, replace=True, **keys)

    def save(self):
        """save importand members for restore"""
        self.savedValues = dict(
            plan=list(self.plan),
            globalInputs=list(self.globalInputs),
            globalOutputs=list(self.globalOutputs))

    def restore(self):
        """restore the state saved with self.save()"""
        self.__dict__.update(self.savedValues)

if __name__ == '__main__':
    Test().main()
