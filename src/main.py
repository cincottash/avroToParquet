from setup import *
from convert import *

def main():
	avroToCSV()
	csvToParquet()
	#deleteCSV()

if __name__ == '__main__':
	main()