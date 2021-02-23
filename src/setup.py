import os 
import pathlib



def dirSetup():
	'''
	Verifies the existence of avro directory from input, 
	as well as creating or clearing the temp csv and parquet directory

	Parameters: None

	Returns: 
	str:avroDirectory, String representing the path of where the avro files are stored
	str:csvDirectory, String representing the path of where the temporary csv folder will be stored
	str:parquet Directory, String representing the path of where the output parquet files will be stored
	
	'''

	avroDirectory = str(input('Please input the folder containing .avro files\n'))
	
	#make sure avro files folder exists
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

	#the output dir is one level above the .avro files dir
	outputDirectory = str(pathlib.Path(avroDirectory).parents[0])
	csvDirectory = outputDirectory + '/csvFilesTemp/'
	parquetDirectory = outputDirectory + '/parquetFiles/'

	#if csv dir already exists, clear all the files in it
	try:
		if os.path.isdir(csvDirectory):
			print('Clearing temp csvFiles directory\n')
			deletedFileCount = 0
			for filename in os.listdir(csvDirectory):
				os.remove(csvDirectory + filename)
				deletedFileCount += 1
			print('Deleted {} files from temp csv directory\n'.format(deletedFileCount))
		else:
			print('Creating temp csvFiles directory\n')
			os.mkdir(csvDirectory)
			print('Created temp csvFiles directory\n')
	except PermissionError:
		print('Error, missing privileges on csv directory')
		exit(0)
	
	#if the parquet dir already exists, loop through it and delete every .parquet file in it
	try:
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
	except PermissionError:
		print('Error, mission write privileges on parquet directory')
		exit(0)

	return avroDirectory, csvDirectory, parquetDirectory