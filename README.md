# FileCheckpoint

This class can control which files in an S3 directory have been processed and which have not. It's very simple and efficient, all the functions are spark based and they will use the spark environment resource for all the processing that it's needed.

You'll need to set a configuration path for the script save it's checkpoint database, after that, the class object is ready for use. The list of files that aren't processed can be get by calling the method GetFiles, passing only the path of the data. Once your process is done, the last thing to do is call the FinishFiles method and all the file names returned previously will be stored in the configuration path as done, and in the next batch, these files will be ignored, as they are already processed.

Example

"""
asd
"""
