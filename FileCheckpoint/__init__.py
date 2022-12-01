import boto3, datetime
import builtins as b
import pyspark.sql.types as T
import pyspark.sql.functions as F

class FileCheckpoint:
  configPath = None
  resource = None
  filesInProcess = None
  pathInProcess = None
  def __init__(self, **kwargs):
    if "resource" in kwargs:
      self.resource = kwargs["resource"]
    else:
      self.resource = boto3.resource("s3")
    if "configPath" not in kwargs:
      raise Exception("Missing configPath for saving and loading metadata")
    self.configPath = str(kwargs["configPath"])
    if self.configPath[-1] != "/":
      self.configPath += "/"
  
  def _getS3Files(self, path):
    hadoopFileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI.create(path), sc._jvm.org.apache.hadoop.conf.Configuration())
    iterator = hadoopFileSystem.listFiles(sc._jvm.org.apache.hadoop.fs.Path(path), True)
    splited = path.replace("s3://", "").split("/")
    bucket = splited[0]
    files = []
    while iterator.hasNext():
        files.append("s3://" + bucket + iterator.next().getPath().toUri().getRawPath())
    df = spark.createDataFrame([
      {
        "filename": x.split("/")[-1],
        "subfolder": x.replace(path, "").replace("/" + x.split("/")[-1], "")
      } for x in files
    ], schema=T.StructType([
        T.StructField("filename", T.StringType(), True),
        T.StructField("subfolder", T.StringType(), True)
      ]))
    return df
  
  def _getDoneFiles(self, forPath):
    try:
      doneFiles = spark.read.parquet(self.configPath + forPath.replace("s3://", "").replace("/", "_") + "/")
    except:
      doneFiles = spark.createDataFrame([], T.StructType([
        T.StructField("filename", T.StringType(), True),
        T.StructField("subfolder", T.StringType(), True),
        T.StructField("timestamp", T.LongType(), True),
        T.StructField("partition", T.StringType(), True)
      ]))
    return doneFiles
    
  def GetFiles(self, path=None):
    if path is None:
      raise Exception("Missing path for files location")
    if "s3://" not in path or not isinstance(path, str):
      raise Exception("Invalid path, must be something like s3://bucket/pre/fix/folder")
    if path[-1] != "/":
      path += "/"
    doneFiles = self._getDoneFiles(path).select(F.concat(F.col("subfolder"), F.lit("/"), F.col("filename")).alias("doneFullname"))
    allFiles = self._getS3Files(path).select(F.concat(F.col("subfolder"), F.lit("/"), F.col("filename")).alias("listFullname"))
    toReadFiles = (allFiles.alias("allFiles")
                     .join(doneFiles.alias("doneFiles"), F.col("allFiles.listFullname") == F.col("doneFiles.doneFullname"), "left")
                     .filter("doneFiles.doneFullname is null")
                     .select(F.col("allFiles.listFullname").alias("filename"))
                  )
    splited = path.replace("s3://", "").split("/")
    bucket = splited[0]
    prefix = "/".join(splited[1:])
    files = [path + x.filename for x in toReadFiles.collect()]
    self.filesInProcess = files
    self.pathInProcess = path
    return files
  
  def FinishFiles(self):
    now = int(datetime.datetime.now().strftime("%s"))
    df = spark.createDataFrame([
      {
        "filename": x.split("/")[-1],
        "subfolder": x.replace(self.pathInProcess, "").replace("/" + x.split("/")[-1], ""),
        "timestamp": now,
        "partition": x.replace(self.pathInProcess, "").replace("/" + x.split("/")[-1], "").replace("/", "_").replace("=", "_")
      } for x in self.filesInProcess
    ], T.StructType([
      T.StructField("filename", T.StringType(), True),
      T.StructField("subfolder", T.StringType(), True),
      T.StructField("timestamp", T.LongType(), True),
      T.StructField("partition", T.StringType(), True)
    ]))
    df.write.partitionBy("partition").mode("append").parquet(self.configPath + self.pathInProcess.replace("s3://", "").replace("/", "_") + "/")
    return True
