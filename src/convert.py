import fastavro
import csv
import pandas as pd
import os

def avroToCSV(avroDirectory, csvDirectory):
	failedConversions = []
	convertedFileCount = 0

	print('Converting .avro files to csv\n')

	for filename in os.listdir(avroDirectory):
		if filename.endswith('.avro'):
			head = True
			count = 0

			#try to convert avro files to csv, if any exception happens while trying to convert a file, skip to the next file
			with open(avroDirectory + filename, 'rb') as avroFile:
				
				try:
					avroReader = fastavro.reader(avroFile)
					csvFile = csv.writer(open(csvDirectory + filename.replace('.avro', '.csv'), 'w+'))
					for data in avroReader:
						if head == True:
							header = data.keys()
							csvFile.writerow(header)
							head = False
						csvFile.writerow(data.values())
					avroFile.close()
					convertedFileCount += 1
				except ImportError:
					print("Please install the modules in requirements.txt\n")
					exit(0)
				#thrown from fastavro.reader when failure to read .avro file, aka avro file is corrupted
				except ValueError:
					avroFile.close()
					failedConversions.append(filename)
					continue

	print('Converted {} .avro files to .csv\n'.format(convertedFileCount))
	if(len(failedConversions) > 0):
		print('Failed to convert {} to .csv, ignoring files\n'.format(failedConversions))
	else:
		print('Successfully Converted all .avro files to .csv\n')

def csvToParquet(csvDirectory, parquetDirectory):
	failedConversions=[]
	convertedFileCount = 0

	print('Converting .csv files to .parquet\n')

	#try to convert csv files to parquet, if exception happens while trying to convert a file, skip to the next file
	for filename in os.listdir(csvDirectory):
		if filename.endswith('.csv'):
			try:
				csvFile = pd.read_csv(csvDirectory + filename)
				csvFile.to_parquet(parquetDirectory + filename.replace('.csv', '.parquet'))
				convertedFileCount += 1
			except ImportError:
				print("Please install the modules in requirements.txt\n")
				exit(0)
			#thrown from pd.read_csv when error reading csv file, aka csv is corrupted
			except pd.errors.ParserError:
				failedConversions.append(filename)
				os.remove(csvDirectory+filename)
				continue

	print('Converted {} .csv files to .parquet\n'.format(convertedFileCount))

	if(len(failedConversions) > 0):
		print('Failed to convert {} to .parquet\n'.format(failedConversions))
	else:
		print('Successfully converted all .csv files to .parquet\n')

#delete csv files and then delete the temp directory
def deleteCSV(csvDirectory):
	try:
		print('Deleting temp csv directory\n')
		for filename in os.listdir(csvDirectory):
			os.remove(csvDirectory + filename)
		
		os.rmdir(csvDirectory)
	except PermissionError:
		print('Error, you don\'t have permission to remove files/directories' )
		exit(0)
	print('Deleted temp csv directory')