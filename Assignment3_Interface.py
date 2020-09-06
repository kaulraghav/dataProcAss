#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
sortPartition = "SORT_PART"
joinPartition = "JOIN_PART"

#Function to create 5 threads for parallel sort and parallel join
def getthreads(cur, window, start, inputtab, outputtab, sort):
    if outputtab[-1] =='0':
        query = "CREATE TABLE {out} AS (SELECT * FROM {inp} WHERE {col} >= {start} AND {col} <={window} ORDER BY {col} ASC)".format(out = outputtab, inp = inputtab, col = sort, start = float(start), window = float(start + window))
        cur.execute(query)
    elif outputtab[-1] =='4':
        query = "CREATE TABLE {out} AS (SELECT * FROM {inp} WHERE {col} > {start} ORDER BY {col} ASC)".format(out = outputtab, inp = inputtab, col = sort, start = float(start))
        cur.execute(query)
    else:
        query = "CREATE TABLE {out} AS (SELECT * FROM {inp} WHERE {col} > {start} AND {col} <={window} ORDER BY {col} ASC)".format(out = outputtab, inp = inputtab, col = sort, start = float(start), window = float(start + window))
        cur.execute(query)

#Function to perform thread join 
def threadjoin(cur, start, inputtab1, inputtab2, col1, col2, outputtab, offset):
    query = "CREATE TABLE {inp} AS ( SELECT * FROM ( SELECT * FROM (SELECT *, ROW_NUMBER() OVER() AS inter FROM {tab1} ) t1 WHERE t1.inter > {start} AND t1.inter <={window} ) tx INNER JOIN {tab2} t2 on tx.{cola} = t2.{colb})".format(inp = outputtab, start = start, window = start + offset, tab1 = inputtab1, tab2 = inputtab2, cola = col1, colb = col2)
    cur.execute(query)

#Function for perforing parallel sort 
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    conn = openconnection
    cur = conn.cursor()
    droptable = "Drop TABLE IF EXISTS {inp}".format(inp = OutputTable)
    cur.execute(droptable)
    selectminmax ="SELECT MAX({col}) ,MIN({col}) FROM {inp}".format(col = SortingColumnName, inp = InputTable)
    cur.execute(selectminmax)
    minmax = cur.fetchone()
    max = minmax[0]
    min = minmax[1]
    diff = float(max - min)/5
    window = round(diff, 2)
    threads = []
    start = min
    for i in range(0, 5):
        thread = threading.Thread(target = getthreads, args = (cur, window, start, InputTable, sortPartition + str(i), SortingColumnName))
        threads.append(thread)
        thread.start()
        start = start + window
    for thread in threads:
        thread.join()
    for i in range(0, 5):
        if i != 0:
            query = "INSERT INTO {out} (SELECT * FROM {inp})".format(out = str(OutputTable), inp = sortPartition+str(i))
        else:
            query = "CREATE TABLE {out} AS (SELECT * FROM {inp})".format(out = str(OutputTable), inp = sortPartition+str(i))
            
        cur.execute(query)
        droptable = "DROP TABLE IF EXISTS {inp}".format(inp = sortPartition+str(i))
        cur.execute(droptable)
        disp = "SELECT * FROM {inp}".format(inp = OutputTable)
        cur.execute(disp)
        openconnection.commit()

#Function for performing parallel join 
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    conn = openconnection
    cur = conn.cursor()
    droptable = "Drop TABLE IF EXISTS {inp}".format(inp = OutputTable)
    cur.execute(droptable)
    diff = "SELECT COUNT(*) FROM {inp}".format(inp = InputTable1)
    cur.execute(diff)
    diff1 = cur.fetchone()[0]
    selectdiff = "SELECT COUNT(*) FROM {inp}".format(inp = InputTable2)
    cur.execute(selectdiff)
    diff2 = cur.fetchone()[0]
    if diff1 < diff2:
        diff1, diff2 = diff2, diff1
        InputTable1,InputTable2 = InputTable2,InputTable1
    offset = diff1 / 5
    threads = []
    start = 0
    for i in range(0, 5):
        thread = threading.Thread(target = threadjoin, args = (cur, start, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, joinPartition + str(i), offset))
        threads.append(thread)
        thread.start()
        start = start + offset
    for thread in threads:
        thread.join()
    for i in range(0, 5):
        if i != 0:
            query = "INSERT INTO {inp} SELECT * FROM {join}".format(inp = OutputTable, join = joinPartition + str(i))
        else:
            query = "CREATE TABLE {inp} AS (SELECT * FROM {join})".format(inp = OutputTable, join = joinPartition + str(i))     
        cur.execute(query)

    dropcol = "ALTER TABLE {inp} DROP COLUMN {col}".format(inp = "paralleljoinoutputtable", col = "inter")
    cur.execute(dropcol)
    openconnection.commit()
