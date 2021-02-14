from fastavro import reader
import csv
import pandas as pd
import os
import glob

def avroToCSV(avroDirectory, csvDirectory):
	failedConversions = []
	convertedFiles = 0

	print('Converting .avro files')

	for filename in os.listdir(avroDirectory):
		if filename.endswith('.avro'):
			head = True
			count = 0

			csvFile = csv.writer(open(csvDirectory + filename.replace('.avro', '.csv'), 'w+'))

			with open(avroDirectory + filename, 'rb') as avroFile:
				try:
					avroReader = reader(avroFile)
				except ValueError:
					failedConversions.append(filename)
					os.remove(csvDirectory + filename.replace('.avro', '.csv'))
					continue
			    
				for data in avroReader:
			        #print(data)
					if head == True:
						header = data.keys()
						csvFile.writerow(header)
						head = False
					csvFile.writerow(data.values())
			avroFile.close()
			convertedFiles += 1

	print('Converted {} .avro files\n'.format(convertedFiles))
	print('Failed to convert {}\n'.format(failedConversions))

def csvToParquet(csvDirectory, parquetDirectory):
	for filename in os.listdir(csvDirectory):
		if filename.endswith('.csv'):
			csvFile = pd.read_csv(csvDirectory + filename)
			csvFile.to_parquet(parquetDirectory + filename.replace('.csv', '.parquet'))

def deleteCSV(csvDirectory):
	for filename in os.listdir(csvDirectory):
		os.remove(csvDirectory + filename)
	
	os.rmdir(csvDirectory)
	print('Deleted temp csv directory')