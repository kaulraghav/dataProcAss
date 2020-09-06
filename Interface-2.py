#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
	cur = openconnection.cursor()
	print("Range is %f to %f" % (ratingMinValue, ratingMaxValue))
	if(ratingMinValue == 0):
		cur.execute("select PartitionNum from RangeRatingsMetadata where (%f >= MinRating and %f <= MaxRating) or (%f >= MinRating and %f <= MaxRating) or (MinRating>= %f and MaxRating<= %f);"
			% (ratingMinValue, ratingMinValue, ratingMaxValue, ratingMaxValue, ratingMinValue, ratingMaxValue))
	else:
		cur.execute("select PartitionNum from RangeRatingsMetadata where (%f > MinRating and %f <= MaxRating) or (%f > MinRating and %f <= MaxRating) or (MinRating> %f and MaxRating<= %f);"
			% (ratingMinValue, ratingMinValue, ratingMaxValue, ratingMaxValue, ratingMinValue, ratingMaxValue))
	rows = cur.fetchall()
	result = []
	for row in rows:
		cur.execute("select * from rangeratingspart%s where rating >= %f and rating <= %f;" % (row[0], ratingMinValue, ratingMaxValue))
		records = cur.fetchall()
		for record in records:
			result.append(["rangeratingspart"+str(row[0]), record[0], record[1], record[2]])

	cur.execute("select PartitionNum from RoundRobinRatingsMetadata;")
	rows = cur.fetchall()
	num_partition = rows[0][0]
	for i in range(0, num_partition):
		cur.execute("select * from roundrobinratingspart%d where rating >= %f and rating <= %f;" % (i, ratingMinValue, ratingMaxValue))
		records = cur.fetchall()
		for record in records:
			result.append(["roundrobinratingspart"+str(i), record[0], record[1], record[2]])

	writeToFile("RangeQueryOut.txt", result)
	cur.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):
	cur = openconnection.cursor()
	print("Point is %f" % (ratingValue))
	if(ratingValue == 0):
		cur.execute("select PartitionNum from RangeRatingsMetadata where %f >= MinRating and %f <= MaxRating;"
			% (ratingValue, ratingValue))
	else:
		cur.execute("select PartitionNum from RangeRatingsMetadata where %f > MinRating and %f <= MaxRating;"
			% (ratingValue, ratingValue))
	rows = cur.fetchall()
	result = []
	for row in rows:
		cur.execute("select * from rangeratingspart%s where rating = %f;" % (row[0], ratingValue))
		records = cur.fetchall()
		for record in records:
			result.append(["rangeratingspart"+str(row[0]), record[0], record[1], record[2]])

	cur.execute("select PartitionNum from RoundRobinRatingsMetadata;")
	rows = cur.fetchall()
	num_partition = rows[0][0]
	for i in range(0, num_partition):
		cur.execute("select * from roundrobinratingspart%d where rating = %f;" % (i, ratingValue))
		records = cur.fetchall()
		for record in records:
			result.append(["roundrobinratingspart"+str(i), record[0], record[1], record[2]])

	writeToFile("PointQueryOut.txt", result)
	cur.close()

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
