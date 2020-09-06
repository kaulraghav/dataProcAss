#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

"""
1. Implement a Python function Load Ratings() that takes a file system path that contains the rating.dat file as input. 
Load Ratings() then load the rating.dat content into a table (saved in PostgreSQL) named Ratings that has the following schema
"""
def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("""
    CREATE TABLE Ratings(
    UserID integer,
    MovieID integer,
    Rating NUMERIC NOT NULL)
    """)

    with open(ratingsfilepath,"r") as file:
        for line in file:
            [userId, movieId, rating, timestamp] = line.split("::")
            cur.execute('INSERT INTO {rn} VALUES ({ui},{mi},{r})'.format(rn = ratingstablename, ui = userId, mi = movieId, r = rating))
    openconnection.commit()


"""
2. Implement a Python function Range_Partition() that takes as input: (1) the Ratings table stored in PostgreSQL and (2) 
an integer value N; that represents the number of partitions. Range_Partition() then generates N horizontal fragments of the Ratings 
table and store them in PostgreSQL. The algorithm should partition the ratings table based on N uniform ranges of the Rating attribute.
"""
def rangePartition(ratingstablename, numberofpartitions, openconnection):
    #Uniformly dividing the partitions 
    cur = openconnection.cursor()
    dtable = 'range_part'
    interim = "CREATE TABLE IF NOT EXISTS range_meta(partition INT, partitionf FLOAT, to_rat float)"
    cur.execute(interim)

    #Iterating number of partitions 
    i = 0
    while (i < numberofpartitions):
        temp = float(5 / numberofpartitions)
        offset = i * temp
        table = (i + 1) * temp
        dname = dtable + str(i)
        totalrange = "CREATE TABLE IF NOT EXISTS {rt} (UserID INT, movieID INT, Rating FLOAT)".format(rt = dname)
        cur.execute(totalrange)
        openconnection.commit()
        if (i != 0):
            tableinsert = "INSERT INTO {rt} select * from {r}  where {r}.rating > {fro} AND {to} >= {r}.rating ".format(rt = dname, r = ratingstablename, fro = offset, to = table)
        else:
            tableinsert = "INSERT INTO {rt} select * from {r}  where {r}.rating BETWEEN {fro} AND {to}  ".format(rt = dname, r = ratingstablename, fro = offset, to = table)
        cur.execute(tableinsert)
        openconnection.commit()
        interiminsert = "INSERT INTO range_meta VALUES ({partition},{fro},{to})".format(partition = i, fro = offset, to = table)
        i = i + 1
        cur.execute(interiminsert)
        openconnection.commit()


"""
3. Implement a Python function RoundRobin_Partition() that takes as input: (1) the Ratings table stored in PostgreSQL and 
(2) an integer value N; that represents the number of partitions. The function then generates N horizontal fragments of the 
Ratings table and stores them in PostgreSQL. The algorithm should partition the ratings table using the round robin partitioning approach
"""
def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    dtable = 'rrobin_part'
    interim = "CREATE TABLE IF NOT EXISTS rrinterim(partition INT, index INT)"
    cur.execute(interim)
    openconnection.commit()
    rrinterim = "CREATE TABLE IF NOT EXISTS rrobin_temp (UserID INT, MovieID INT, Rating FLOAT, index INT)"
    cur.execute(rrinterim)
    openconnection.commit()
    rrinsert = "INSERT INTO rrobin_temp (SELECT {rt}.UserID, {rt}.MovieID, {rt}.Rating , (ROW_NUMBER() OVER() -1) % {n} as index from {rt})".format(n = str(numberofpartitions), rt = ratingstablename)
    cur.execute(rrinsert)
    openconnection.commit()
    i = 0
    while (i < numberofpartitions):
        create_rrobin = "CREATE TABLE IF NOT EXISTS {rt} (UserID INT, MovieID INT, Rating FLOAT)".format(
            rt = dtable + str(i))
        cur.execute(create_rrobin)
        openconnection.commit()
        insert_rrobin = "INSERT INTO {rt} select userid,movieid,rating from rrobin_temp where index = {index}".format(rt = dtable + str(i), index = str(i))
        i = i + 1
        cur.execute(insert_rrobin)
        openconnection.commit()

    interim_insert = "INSERT INTO rrinterim SELECT {N} AS partition, count(*) % {N} from {rt}".format(
        rt = ratingstablename, N = numberofpartitions)
    cur.execute(interim_insert)
    openconnection.commit()
    deleteTables('rrobin_temp', openconnection)

    openconnection.commit()

"""
4. Implement a Python function RoundRobin_Insert() that takes as input: (1) Ratings table stored in PostgreSQL, (2) UserID, 
(3) ItemID, (4) Rating. RoundRobin_Insert() then inserts a new tuple to the Ratings table and the right fragment based on the round robin approach.
"""
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT * from rrinterim")
    numberofpart, index = cur.fetchone()
    offset = index % numberofpart
    cur.execute("DELETE from rrinterim")
    openconnection.commit()
    update_meta = "Insert into rrinterim VALUES ({first},{second})".format(first = numberofpart, second = offset + 1)
    cur.execute(update_meta)
    openconnection.commit()
    rate_insert = "Insert into {rt} values ({u},{it},{r})".format(rt=ratingstablename, u = userid, it =itemid, r = rating)
    cur.execute(rate_insert)
    openconnection.commit()
    rrobin_insert = "Insert into rrobin_part{i} values ({u},{it},{r})".format(i = offset, u = userid, it = itemid, r = rating)
    cur.execute(rrobin_insert)
    openconnection.commit()

"""
5. Implement a Python function Range_Insert() that takes as input: (1) Ratings table stored in Post- greSQL (2) UserID, (3) ItemID, (4) Rating. 
Range_Insert() then inserts a new tuple to the Ratings table and the correct fragment (of the partitioned ratings table) based upon the Rating value.
"""
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    meta_get = "SELECT MIN(r.partition) FROM range_meta as r where r.partitionf <= {rat} and r.to_rat >= {rat} ".format(rat =rating )
    cur.execute(meta_get)
    openconnection.commit()
    partition = cur.fetchone()
    initialp = partition[0]
    rate_insert = "Insert into {rt} values ({u},{it},{r})".format(rt = ratingstablename, u = userid, it = itemid, r = rating)
    cur.execute(rate_insert)
    openconnection.commit()
    range_insert = "Insert into range_part{i} values ({u},{it},{r})".format(i = initialp, u = userid, it = itemid, r = rating)
    cur.execute(range_insert)
    openconnection.commit()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()


#if __name__ == "__main__":
    #Helper functions 
    #print ("I'm Here!")
    #loadRatings()
    #rangePartition("Ratings", 3)
