from exceptions import *
import os 

def dirSetup(avroDirectory, csvDirectory, parquetDirectory):
	
	try:  
		if not os.path.isdir(avroDirectory):
			raise NotADirectoryError("Avro source directory does not exist")
	#avroFiles doesn't exist error
	except NotADirectoryError as error:
		print(error)
	
	#if csv dir already exists, delete the current csv files in it
	if os.path.isdir(csvDirectory):
		print('Clearing temp csvFiles directory')
		deletedFileCount = 0
		for filename in os.listdir(csvDirectory):
			os.remove(csvDirectory + filename)
			deletedFileCount += 1
		print('Deleted {} files from temp csv directory\n'.format(deletedFileCount))
	else:
		os.mkdir(csvDirectory)
		print('Created temp csvFiles directory\n')
	
	#if the parquet dir already exists, loop through it and delete every .parquet file in it
	if os.path.isdir(parquetDirectory):
		print('Deleting existing .parquet files')
		deletedFileCount = 0
		for filename in os.listdir(parquetDirectory):
			if(filename.endswith('.parquet')):
				os.remove(parquetDirectory + filename)
				deletedFileCount += 1
		print('Deleted {} .parquet files\n'.format(deletedFileCount))
	else:
		os.mkdir(parquetDirectory)  
		print('Created parquetFiles directory\n')

	




	return avroDirectory, csvDirectory, parquetDirectory