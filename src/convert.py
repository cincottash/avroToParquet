import fastavro
import csv
import pandas as pd
import os

def avroToCSV(avroDirectory, csvDirectory):
	'''
	Converts all .avro files from a user supplied directory into csv files contained in a temp directory

	Parameters:
	avroDirectory (str): A string representing the location of the directory containing the avro files
	csvDirectory (str): A string representing the location of the directory containing the temporary csv files

	Returns: None
	
	'''

	failedConversions = []
	convertedFileCount = 0

	print('Converting .avro files to csv\n')

	for filename in os.listdir(avroDirectory):
		if filename.endswith('.avro'):
			head = True
			count = 0

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

				except PermissionError:
					#Reading from avro is already checked in dirSetup(), so permission error is probably from the csv writer
					print('Error failed writing csv file, possible missing privileges on temp csv directory\n')
					exit(0)
				except ImportError:
					print('Error missing modules, please install the modules in requirements.txt\n')
					exit(0)
				#thrown from fastavro.reader when failure to read .avro file, aka avro file is corrupted
				except ValueError:
					print('Warning, read failed... skipping file {}'.format(avroFile))
					avroFile.close()
					failedConversions.append(filename)
					continue

	print('Converted {} .avro files to .csv\n'.format(convertedFileCount))
	if(len(failedConversions) > 0):
		print('Failed to convert {} to .csv, ignoring files\n'.format(failedConversions))
	else:
		print('Successfully Converted all .avro files to .csv\n')

def csvToParquet(csvDirectory, parquetDirectory):
	'''
	Converts all csv files from a temporary directory into parquet files contained in an output directory

	Parameters:
	csvDirectory (str): A string representing the location of the directory containing the temporary csv files
	parquetDirectory (str): A string representing the location of the directory which will contain the parquet files
	
	Returns: None
	
	'''

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

def deleteCSV(csvDirectory):
	'''
	Deletes all csv files from a temporary directory and then deletes the directory

	Parameters:
	csvDirectory (str): A string representing the location of the directory containing the temporary csv files
	
	Returns: None
	
	'''

	print('Deleting temp csv directory\n')
	try:
		for filename in os.listdir(csvDirectory):
			os.remove(csvDirectory + filename)
		
		os.rmdir(csvDirectory)
	except PermissionError:
		print('Error, you don\'t have permission to remove files/directories' )
		exit(0)
	print('Deleted temp csv directory')