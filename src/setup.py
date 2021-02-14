import os 

def dirSetup(avroDirectory, csvDirectory, parquetDirectory):
	
	#if csv dir already exists, clear all the files in it
	if os.path.isdir(csvDirectory):
		print('Clearing temp csvFiles directory\n')
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
		print('Deleting existing .parquet files\n')
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