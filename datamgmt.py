import sys, os, argparse, re, string, subprocess
import cx_Oracle
import config

def stripEndRec(fileline):
    '''All lines in the export ends with <endrec> which needs trimming, bonus spaces trimming'''
    if fileline.endswith("<endrec>"):
        line = re.sub("<endrec>", '', fileline).strip()
        return line
    return fileline

def getTableName(filename):
    '''Based on csv_file, return table_name'''
    tablename = os.path.splitext(os.path.basename(filename))[0]
    return tablename

def getColumnsCsv(filename):
    ''' Taking full path to csv file, return column names (cols) based on first row'''
    with open(filename, 'r') as file:
      firstline = file.readline().strip()
    
    cols = stripEndRec(firstline).split(',')
    return cols

def getColumnsDB(dbconn, tablename):
    ''' Returns a list of column names from a table name. '''

    dbcols = []
    cursor = dbconn.cursor()
    cursor.rowfactory = lambda *args: list(args)
    for row in cursor.execute("SELECT column_name FROM ALL_TAB_COLUMNS WHERE table_name =:tbl", tbl=tablename):
        dbcols.append(row[0])
    return dbcols


def getColumnsDataTypeDB(dbconn, tablename):
    ''' Returns a dict of column and associated datatype '''
    dbcols = {}
    cursor = dbconn.cursor()
    cursor.rowfactory = lambda *args: list(args)
    for row in cursor.execute("SELECT column_name, data_type FROM ALL_TAB_COLUMNS WHERE table_name =:tbl", tbl=tablename):
        dbcols[row[0]] = row[1]
    return dbcols
    for error in cursor.getbatcherrors():
        print("Error", error.message.rstrip(), "at row offset", error.offset)
        #may just need except cx_Oracle.Error as error:
        #    print(error)

def schema_check(csvdir):
    ''''Compares csv files to columns in database to look for changes.'''
    try:
        connection = cx_Oracle.connect( "/", config.dsn, mode=cx_Oracle.SYSDBA, encoding=config.encoding)

        for f in os.listdir(csvdir):
            if not f.endswith(".csv") or os.path.isdir(f):
                continue
            tablename = getTableName(f)
            fullFile = os.path.join(csvdir, f)
            cols = getColumnsCsv(fullFile)

            dbcols = getColumnsDB(connection, tablename)

            added = list(set(cols)-set(dbcols))
            removed = list(set(dbcols)-set(cols))
            if not dbcols:
                print("Table ", tablename, " is new.")
            elif added or removed:
                print("Table ", tablename, " differs. Added: ", added, " removed: ", removed)

    except cx_Oracle.Error as error:
        print(error)
    else:
        # release the connection
        if connection:
            connection.close()

def writeCtl(filepath, tablename, ctldir):
    '''Creates control file from template'''
    columns = getColumnsCsv(filepath)
    colsDB = {}
    try:
        dbconn = cx_Oracle.connect( "/", config.dsn, mode=cx_Oracle.SYSDBA, encoding=config.encoding)
        colsDB = getColumnsDataTypeDB(dbconn, tablename)
    except cx_Oracle.Error as error:
        print(error)
    else:
        # release the connection
        if dbconn:
            dbconn.close()

    for index, col in enumerate(columns):
        if col in colsDB.keys() and "TIMESTAMP" in colsDB[col]:
            columns[index] = col + ' TIMESTAMP "yyyy-mm-dd hh24:mi:ss.ff6"'
        elif col not in colsDB.keys():
            columns[index] = col + ' FILLER'
        else:
            columns[index] = col + ' CHAR(1000000)'

    colstring=',\n\t'.join(columns)

    # When this executes in flow, it's not guarantueed to be executing from the same directory as the file is located.
    templatefile = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'ctl_template.ctl')
    template = open(templatefile)
    src = string.Template(template.read())

    resstring = src.substitute(FILE_PATH=filepath, TABLE_NAME=tablename, COL_NAME=colstring)
    outputfile = os.path.join(ctldir, tablename + '.ctl')
    f = open(outputfile, 'w+')
    f.write(resstring)
    f.close()
    return True

def generateCtlFiles(csvdir, ctldir):
    '''generate control files for all csv files in given directory'''
    print("Generating CtlFiles")
    try:
        os.mkdir(ctldir)
    except OSError:
        pass

    for f in os.listdir(csvdir):
        if not f.endswith(".csv") or os.path.isdir(f):
            continue
        fullFile = os.path.join(csvdir, f)
        writeCtl(fullFile,getTableName(f), ctldir)
    print("Finished generating ctl files")

def executeCtlFiles(ctldir):
    '''execute import command for all existing ctl files in given directory'''
    print("Execute sqlldr")
    baddir=os.path.join(ctldir, "bad")
    logdir=os.path.join(ctldir, "log")
    try:
        os.mkdir(baddir)
        os.mkdir(logdir)
    except OSError:
        pass
    for f in os.listdir(ctldir):
        if not f.endswith(".ctl") or os.path.isdir(f):
            continue
        ctlFile = os.path.join(ctldir, f)
        print("Importing file ", ctlFile)
        subprocess.call("sqlldr '\"/ as sysdba\"' CONTROL={ctlString} BAD={baddir}/{file}.bad LOG={logdir}/{file}.log skip=1 SILENT=ALL DIRECT=true COLUMNARRAYROWS=50".format(ctlString=ctlFile, baddir=baddir, logdir=logdir, file=f),shell=True)

if __name__ == "__main__":
    '''Look up each tables based on file names and compare columns in existing tables in database'''
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-csv', dest='inputcsv', help="The directory that holds the exported csv files")
    parser.add_argument('--output-ctl', dest='outputctl', help="The directory that holds the generated ctl files")
    args = parser.parse_args()
    csvdir = args.inputcsv
    ctldir = args.outputctl

    # choice: schema_check or create ctl file
    if not ctldir:
        print("Run schema check")
        schema_check(csvdir)
    else:
        generateCtlFiles(csvdir, ctldir)
        executeCtlFiles(ctldir)
