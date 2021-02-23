import os 
import pathlib



def dirSetup():
	'''
	Verifies the existence of avro directory from input, 
	as well as creating or clearing the temp csv and parquet directory
		If a directory with the same name as the temp csv directory already exists, all the csv files within are deleted
		If a directory with the same name as the parquet directory already exists, ONLY the parquet files are deleted

	Parameters: None

	Returns: 
	str:avroDirectory, String representing the path of where the avro files are stored
	str:csvDirectory, String representing the path of where the temporary csv directory will be stored
	str:parquetDirectory, String representing the path of where the output parquet files will be stored
	
	'''

	avroDirectory = str(input('Please input the folder containing .avro files\n'))
	
	#make sure avro files directory exists
	try:
		os.listdir(avroDirectory)

		#make sure the path has a \ or / at the end, listdir still works even without the / or \
		if(avroDirectory[len(avroDirectory)-1] != '\\' and avroDirectory[len(avroDirectory)-1] != '/'):
			raise FileNotFoundError
	except FileNotFoundError:
		print('Error, missing avro files direcectory, please include trailing slash')
		exit(0)
	except PermissionError:
		print('Error, missing privileges on avro directory')
		exit(0)

	#the output directory (where the temp files and parquet files will be located) is one level above the avro files dir
	outputDirectory = str(pathlib.Path(avroDirectory).parents[0])
	csvDirectory = outputDirectory + '/csvFilesTemp/'
	parquetDirectory = outputDirectory + '/parquetFiles/'

	try:
		#if csv directory already exists, clear all the files in it
		if os.path.isdir(csvDirectory):
			print('Clearing temp csvFiles directory\n')
			deletedFileCount = 0
			for filename in os.listdir(csvDirectory):
				os.remove(csvDirectory + filename)
				deletedFileCount += 1
			print('Deleted {} files from temp csv directory\n'.format(deletedFileCount))
		#just make the directory otherwise
		else:
			print('Creating temp csvFiles directory\n')
			os.mkdir(csvDirectory)
			print('Created temp csvFiles directory\n')
	except PermissionError:
		print('Error, missing privileges on csv directory')
		exit(0)
	
	try:
		#if the parquet directory already exists, loop through it and delete every parquet file in it
		if os.path.isdir(parquetDirectory):
			print('Deleting existing .parquet files\n')
			deletedFileCount = 0
			for filename in os.listdir(parquetDirectory):
				if(filename.endswith('.parquet')):
					os.remove(parquetDirectory + filename)
					deletedFileCount += 1
			print('Deleted {} .parquet files\n'.format(deletedFileCount))
		#otherwise just make the directory
		else:
			os.mkdir(parquetDirectory)  
			print('Created parquetFiles directory\n')
	except PermissionError:
		print('Error, mission write privileges on parquet directory')
		exit(0)

	return avroDirectory, csvDirectory, parquetDirectory