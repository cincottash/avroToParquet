from fastavro import reader
import csv
import pandas as pd
import os

def avroToCSV(avroDirectory, csvDirectory):
	failedConversions = []
	convertedFiles = 0

	print('Converting .avro files to csv\n')

	for filename in os.listdir(avroDirectory):
		if filename.endswith('.avro'):
			head = True
			count = 0

			csvFile = csv.writer(open(csvDirectory + filename.replace('.avro', '.csv'), 'w+'))

			with open(avroDirectory + filename, 'rb') as avroFile:
				try:
					avroReader = reader(avroFile)
				except Exception:
					failedConversions.append(filename)
					os.remove(csvDirectory + filename.replace('.avro', '.csv'))
					avroFile.close()
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

	print('Converted {} .avro files to .csv\n'.format(convertedFiles))
	if(len(failedConversions) > 0):
		print('Failed to convert {} to .csv\n'.format(failedConversions))
	else:
		print('Successfully Converted all files to .csv\n')

def csvToParquet(csvDirectory, parquetDirectory):
	failedConversions=[]
	print('Converting .csv files to .parquet\n')
	for filename in os.listdir(csvDirectory):
		if filename.endswith('.csv'):
			try:
				csvFile = pd.read_csv(csvDirectory + filename)
				csvFile.to_parquet(parquetDirectory + filename.replace('.csv', '.parquet'))
			except Exception:
				failedConversions.append(filename)
				continue
	if(len(failedConversions) > 0):
		print('Failed to convert {} to .parquet\n'.format(failedConversions))
	else:
		print('Successfully Converted all files to .parquet\n')

def deleteCSV(csvDirectory):
	for filename in os.listdir(csvDirectory):
		os.remove(csvDirectory + filename)
	
	os.rmdir(csvDirectory)
	print('Deleted temp csv directory')