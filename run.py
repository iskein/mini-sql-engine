import sys
from collections import OrderedDict
import sqlparse

class sqlEngine(object):
    opers = ['>=', '<=', '<', '>', '=']
    db = OrderedDict()
    def extractMetadata(self):
        metadata = open("metadata.txt","r").read().split("\n")
        m = len(metadata)
        i = 0
        while i < m:
            metadata[i] = metadata[i][:-1]
            i += 1
        i = 0
        while i < m:
            if metadata[i] == '<begin_table>':
                n = i + 1
                i = i + 2
                self.db[metadata[n]] = OrderedDict()
                while i < m-1 and metadata[i] != '<end_table>':
                    self.db[metadata[n]].update({str(metadata[n])+"."+str(metadata[i]):[]})
                    i += 1
            i += 1

    def populateData(self):
        for table in self.db:
            filename = table + ".csv"
            data = open(filename, "r" ).read().split("\n")
            col = 0
            size = len(self.db[table])
            for line in data:
                col = 0
                line = line.split(',')
                for colName in self.db[table]:
                    val = line[col]
                    if val[-1:] == '\r':
                        val = val[:-1]
                    if val == '':
                        continue
                    self.db[table][colName].append(val)
                    col += 1

    def printTable(self, table): # takes the entire table (as a dict)
        lines = len(table[next(iter(table))])
        i = 0
        for col in table:
            print str(col) + " ",
        print ""
        while i < lines:
            for col in table:
                print str(table[col][i]) + ",",
            i += 1
            print ""
        sys.exit()
    
    def error(self):
        print "*** ERROR ***"
        sys.exit()
    
    def joinTable(self, table1, table2):
        newTable = OrderedDict()
        noLine1 = len(table1[next(iter(table1))])
        noLine2 = len(table2[next(iter(table2))])
        for w in table1:
            newTable.update({w:[]})
        for w in table2:
            newTable.update({w:[]})
        i = 0
        while i < noLine1:
            j = 0
            while j < noLine2:
                for w in table1:
                    newTable[w].append(table1[w][i])
                for w in table2:
                    newTable[w].append(table2[w][j])
                j += 1
            i += 1
        return newTable

    def aggregate(self, fun, col, table): # function name, col name and table name
        temp = ""
        for x in col:
            if x == " ":
                temp += ""
            else:
                temp += x
        col = temp
        temp = ""
        for x in table:
            if x == " ":
                temp += ""
            else:
                temp += x
        table = temp
        col = table + "." + col
        toPrint = OrderedDict()
        if fun == "MAX":
            m = 0
            for val in self.db[table][col]:
                m = max(m, float(val))
            toPrint.update({"MAX("+col+")": [m]})

        if fun == "MIN":
            m = sys.maxint
            for val in self.db[table][col]:
                m = min(m, float(val))
            toPrint.update({"MIN(" + col + ")": [m]})

        if fun == "AVERAGE":
            tlen = len(self.db[table][col])
            total = 0
            for val in self.db[table][col]:
                total += float(val)
            avg = float(total)/float(tlen)
            toPrint.update({"AVERAGE(" + col + ")": [avg]})

        if fun == "SUM":
            total = 0
            for val in self.db[table][col]:
                total += float(val)
            toPrint.update({"SUM("+col+")": [total]})
        
        self.printTable(toPrint)
    
    def checkCondition(self, table, col1, col2, cond):
        newTable = OrderedDict()
        for key in table:
            newTable.update({key:[]})
        noLine = len(table[next(iter(table))])
        i = 0
        isInt = 0
        if col2.replace('.','',1).isdigit():
            isNum = 1
        else:
            isNum = 0
        while i < noLine:
            flag = 0
            if cond == ">=" and ((isNum == 0 and float(table[col1][i]) >= float(table[col2][i])) or (isNum == 1 and float(table[col1][i]) >= float(col2))):
                flag = 1
            elif cond == "<=" and ((isNum == 0 and float(table[col1][i]) <= float(table[col2][i])) or (isNum == 1 and float(table[col1][i]) <= float(col2))):
                flag = 1
            elif cond == "=" and ((isNum == 0 and float(table[col1][i]) == float(table[col2][i])) or (isNum == 1 and float(table[col1][i]) == float(col2))):
                flag = 1
            elif cond == ">" and ((isNum == 0 and float(table[col1][i]) > float(table[col2][i])) or (isNum == 1 and float(table[col1][i]) > float(col2))):
                flag = 1
            elif cond == "<" and ((isNum == 0 and float(table[col1][i]) < float(table[col2][i])) or (isNum == 1 and float(table[col1][i]) < float(col2))):
                flag = 1
            if flag == 1:
                for key in table:
                    newTable[key].append(table[key][i])
            i += 1
        return newTable
        

    def conditions(self, table, cond): # entire table as a dict, conditions as tok
        newTable = OrderedDict()
        andcond = 0
        orcond = 0
        cond = cond.split('WHERE')[1]
        temp = ""
        for x in cond:
            if x != " ":
                 temp += x
        cond = temp                     # need to give input as table1.colName
        if cond[-1:] == ";":
            cond = cond[:-1]
        if "AND" in cond.upper():
            andcond = 1
        if "OR" in cond.upper():
            orcond = 1
        if andcond == 0 and orcond == 0:
            for o in self.opers:
                if len(cond.split(o)) == 2:
                    cols = cond.split(o)
                    newTable = self.checkCondition(table, cols[0], cols[1], o)
                    break
            
        elif andcond == 1:
            conds = cond.split("AND")
            cond1 = conds[0]
            cond2 = conds[1]
            table1 = OrderedDict()
            table2 = OrderedDict()
            for o in self.opers:
                if len(cond1.split(o)) == 2:
                    cols = cond1.split(o)
                    table1 = self.checkCondition(table, cols[0], cols[1], o)
                    break
            for o in self.opers:
                if len(cond2.split(o)) == 2:
                    cols = cond2.split(o)
                    table1 = self.checkCondition(table1, cols[0], cols[1], o)
                    break
            newTable = table1
            
        elif orcond == 1:
            conds = cond.split("OR")
            cond1 = conds[0]
            cond2 = conds[1]
            table1 = OrderedDict()
            for o in self.opers:
                if len(cond1.split(o)) == 2:
                    cols = cond1.split(o)
                    table1 = self.checkCondition(table, cols[0], cols[1], o)
                    break
            for o in self.opers:
                if len(cond2.split(o)) == 2:
                    cols = cond2.split(o)
                    table2 = self.checkCondition(table, cols[0], cols[1], o)
                    break
            noLine = len(table2[next(iter(table2))])
            i = 0
            newTable = table1
            while i < noLine:
                for key in newTable:
                    newTable[key].append(table2[key][i])
                i += 1

        return newTable
        
    def checkCols(self, table, cols): # table(dict), cols(list)
        newTable = OrderedDict()
        for col in cols:
            if col in table:
                newTable.update({col:table[col]})
        return newTable

    def distinct(self, cols, table):   # cols(list), table(dict)
        noLine = len(table[next(iter(table))])
        listOfValues = []
        i = 0
        while i < noLine:
            temp = ""
            for col in cols:
                temp = temp + str(table[col][i]) + ","
            listOfValues.append(temp)
            i += 1
        newSet = set(listOfValues)
        newTable = OrderedDict()
        for key in table:
            if key in cols:
                newTable.update({key:[]})
        finalList = list(newSet)
        i = 0
        for line in finalList:
            vals = line.split(',')[:-1]
            i = 0
            for col in cols:
                newTable[col].append(vals[i])
                i += 1
        self.printTable(newTable)

    def checkQuery(self, tok):
        tokens = tok.tokens
        if len(tokens) > 9:
            print 1
            self.error()
        if len(tokens) <= 0:
            print 2
            self.error()
        if str(tokens[0]) != "SELECT":
            print 3
            self.error()
        if str(tokens[4]) != "FROM":
            print 4
            self.error()
        if len(tokens) == 9 and str(tokens[8]).split(' ')[0] != "WHERE":
            print 5
            self.error()
        strTable = str(tokens[6])
        strCols = str(tokens[2])
        temp = ""
        for x in strTable:
            if x == " ":
                temp += ""
            else:
                temp += x
        strTable = temp
        temp = ""
        if ("MAX" in strCols) or ("MIN" in strCols) or ("AVERAGE" in strCols) or ("SUM" in strCols) or ("DISTINCT" in strCols):
            strCols = strCols.split('(')[1].split(")")[0]
        for x in strCols:
            if x == " ":
                temp += ""
            else:
                temp += x
        strCols = temp
        tables = strTable.split(",")
        cols = strCols.split(",")
        for table in tables:
            if table not in self.db:
                print table + " : Table dosen't exist"
                sys.exit()
        for col in cols:
            flag = False
            if col == '*':
                flag = True
            for table in tables:
                check = table+"."+col
                if check in self.db[table]:
                    flag = True
            if flag == False:
                print col + " : Dosen't exist"
                sys.exit()

        cols = str(tokens[2])
        if "MAX" in cols.upper():
            col = cols.split("(")[1].split(")")[0]
            self.aggregate("MAX", col, str(tokens[6]))

        if "MIN" in cols.upper():
            col = cols.split("(")[1].split(")")[0]
            self.aggregate("MIN", col, str(tokens[6]))

        if "AVERAGE" in cols.upper():
            col = cols.split("(")[1].split(")")[0]
            self.aggregate("AVERAGE", col, str(tokens[6]))

        if "SUM" in cols.upper():
            col = cols.split("(")[1].split(")")[0]
            self.aggregate("SUM", col, str(tokens[6]))
        
        if "DISTINCT" in cols.upper():
            cols = cols.split("(")[1].split(")")[0]
            table = str(tokens[6])
            temp = ""
            for x in table:
                if x != " ":
                    temp += x
            temp = ""
            for x in cols:
                if x != " ":
                    temp += x
            cols = temp.split(",")
            for i,col in enumerate(cols):
                cols[i] = table+"."+cols[i]
            self.distinct(cols, self.db[table])
        finalTable = {}
        strTable = str(tokens[6])
        strCols = str(tokens[2])
        temp = ""
        for x in strTable:
            if x == " ":
                temp += ""
            else:
                temp += x
        strTable = temp
        temp = ""
        for x in strCols:
            if x == " ":
                temp += ""
            else:
                temp += x
        strCols = temp
        tables = strTable.split(",")
        cols = strCols.split(",")
        newCols = []
        if len(cols) == 1 and cols[0] == "*":
            for table in tables:
                for key in self.db[table]:
                    newCols.append(key)
            cols = newCols
        newCols = []
        for col in cols:
            for table in tables:
                newCols.append(col)
                newCols.append(table+"."+col)
        cols = newCols
        finalTable = OrderedDict()
        if len(tables) == 2:
            finalTable = self.joinTable(self.db[tables[0]], self.db[tables[1]])
        else:
            finalTable = self.db[tables[0]]
        if len(tokens) == 9:
            finalTable = self.conditions(finalTable, str(tokens[8]))
        finalTable = self.checkCols(finalTable, cols)
        self.printTable(finalTable)


if __name__ == '__main__':
    engine = sqlEngine()
    engine.extractMetadata()
    engine.populateData()
    query = sys.argv[1]
    queryf = sqlparse.format(query, keyword_case = 'upper')
    #try:
    tok = sqlparse.parse(queryf)[0]
    engine.checkQuery(tok)
    #except (ValueError, KeyError):
        #engine.error()
