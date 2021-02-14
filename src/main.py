from setup import *
from convert import *

def main():
	avroToCSV('../avroFiles/', '../csvFiles/')
	
	csvToParquet('../csvFiles/', '../parquetFiles/')
	
	deleteCSV()

if __name__ == '__main__':
	main()