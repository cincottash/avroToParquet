from setup import *
from convert import *

def main():
	'''
	Parameters: None

	Initializes directories, converts avro files -> csv -> parquet files, then deletes the csv files

	Returns: None
	
	'''

	avroDirectory, csvDirectory, parquetDirectory = dirSetup()
	
	avroToCSV(avroDirectory, csvDirectory)
	
	csvToParquet(csvDirectory, parquetDirectory)
	
	deleteCSV(csvDirectory)

if __name__ == '__main__':
	main()