from setup import *
from convert import *

def main():

	avroDirectory, csvDirectory, parquetDirectory = dirSetup()
	
	avroToCSV(avroDirectory, csvDirectory)
	
	csvToParquet(csvDirectory, parquetDirectory)
	
	deleteCSV(csvDirectory)

if __name__ == '__main__':
	main()