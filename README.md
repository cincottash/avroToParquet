# avroToParquet

Converts a given file from avro to parquet format 

## Features

* Reads .avro files located within the specified directory

* If unable to read .avro file, skips that file and notes that their was a conversion failure

* Converts each .avro file into a .CSV, stores the .CSV files in a temp directory

* The CSV files are stored in a directory called "csvFilesTemp" located in the same directory as the avro files directory

* If csvFilesTemp already exists (e.g from a previous-uncompleted run), ALL the files are removed

* If csvFilesTemp doesn't exist, a new directory is created

* After converting to .CSV, the files are converted to .parquet

* The parquet files are stored in a directory called "parquetFiles" located in the same directory as the avro files directory

* If the parquetFiles directory already exists, ALL of the .parquet files are deleted (only files with the .parquet file extension)

* If the parquetFiles directory doesn't exist, a new directory is created

* If unable to read/convert .CSV file, skips that file and notes that there was a conversion failure(s) as well as the failed file

* After converting each .CSV to .parquet, the temp CSV directory is deleted

## Prerequisites

A list of required libraries is located in requirments.txt and can be installed via Pip with ```pip3 install -r requirments.txt```

## Usage
### Windows
```py main.py ```

### Linux 
```python3 main.py ```

You will then be prompted for the directory containing the .avro files <br/>
An example vaild directory for Windows is C:\Users\USERNAME\Desktop\avroFiles\\ <br/>
An example vaild directory for Linux is /home/USERNAME/Desktop/avroFiles/ <br/>

INCLUD THE TRAILING SLASH FOR BOTH OPERATING SYSTEMS!!! <br/>

## Built With

* Python3

* [Pandas](https://pandas.pydata.org/)

* [Fastavro](https://fastavro.readthedocs.io/en/latest/)

* [Pyarrow](https://arrow.apache.org/docs/python/install.html)


