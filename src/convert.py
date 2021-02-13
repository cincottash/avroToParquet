from fastavro import reader
import csv
import pandas as pd
import os
import glob

def avroToCSV():
	head = True
	count = 0
	csvFile = csv.writer(open('../csvFiles/twitter.csv', 'w+'))
	with open('../avroFiles/twitter.avro', 'rb') as avroFile:
	    avroReader = reader(avroFile)
	    for data in avroReader:
	        #print(data)
	        if head == True:
	            header = data.keys()
	            csvFile.writerow(header)
	            head = False
	        csvFile.writerow(data.values())
	avroFile.close()

def csvToParquet():
	df = pd.read_csv('../csvFiles/twitter.csv')
	df.to_parquet('../parquetFiles/twitter.parquet')

def deleteCSV():
	csvFiles = glob.glob('../csvFiles/*')
	for file in csvFiles:
	    os.remove(file)
