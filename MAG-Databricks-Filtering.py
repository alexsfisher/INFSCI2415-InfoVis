# Databricks notebook source
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from datetime import date

# Azure connection info
storage_account_name = "magstoragefisher"
storage_account_access_key = "Re7EDRwNFd0ffUuBWqFWembQhV+bhAdVMZ93b4vlwlq6UYwXtXfZ1TxPGg7I00X0hZEehMNlLqkwAAIEKuglTw=="
storage_container_path = "wasbs://mag@magstoragefisher.blob.core.windows.net/"
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# schemas
affiliationsSchema = StructType([
            StructField('AffiliationId', LongType(), False),
            StructField('Rank', IntegerType(), False),
            StructField('NormalizedName', StringType(), False),
            StructField('DisplayName', StringType(), False),
            StructField('GridId', StringType(), False),
            StructField('OfficialPage', StringType(), False),
            StructField('WikiPage', StringType(), False),
            StructField('PaperCount', LongType(), False),
            StructField('PaperFamilyCount', LongType(), False),
            StructField('CitationCount', LongType(), False),
            StructField('Iso3166Code', StringType(), False),
            StructField('Latitude', FloatType(), True),
            StructField('Longitude', FloatType(), True),
            StructField('CreatedDate', DateType(), False)
         ])
paperAuthorAffiliationsSchema = StructType([
            StructField('PaperId', LongType(), False),
            StructField('AuthorId', LongType(), False),
            StructField('AffiliationId', LongType(), True),
            StructField('AuthorSequenceNumber', IntegerType(), False),
            StructField('OriginalAuthor', StringType(), False),
            StructField('OriginalAffiliation', StringType(), False)
         ])
papersSchema = StructType([
            StructField('PaperId', LongType(), False),
            StructField('Rank', IntegerType(), False),
            StructField('Doi', StringType(), False),
            StructField('DocType', StringType(), False),
            StructField('PaperTitle', StringType(), False),
            StructField('OriginalTitle', StringType(), False),
            StructField('BookTitle', StringType(), False),
            StructField('Year', IntegerType(), True),
            StructField('Date', DateType(), True),
            StructField('OnlineDate', DateType(), True),
            StructField('Publisher', StringType(), False),
            StructField('JournalId', LongType(), True),
            StructField('ConferenceSeriesId', LongType(), True),
            StructField('ConferenceInstanceId', LongType(), True),
            StructField('Volume', StringType(), False),
            StructField('Issue', StringType(), False),
            StructField('FirstPage', StringType(), False),
            StructField('LastPage', StringType(), False),
            StructField('ReferenceCount', LongType(), False),
            StructField('CitationCount', LongType(), False),
            StructField('EstimatedCitation', LongType(), False),
            StructField('OriginalVenue', StringType(), False),
            StructField('FamilyId', LongType(), True),
            StructField('FamilyRank', IntegerType(), True),
            StructField('CreatedDate', DateType(), False)
         ])
fieldsOfStudySchema = StructType([
            StructField('FieldOfStudyId', LongType(), False),
            StructField('Rank', IntegerType(), False),
            StructField('NormalizedName', StringType(), False),
            StructField('DisplayName', StringType(), False),
            StructField('MainType', StringType(), False),
            StructField('Level', IntegerType(), False),
            StructField('PaperCount', LongType(), False),
            StructField('PaperFamilyCount', LongType(), False),
            StructField('CitationCount', LongType(), False),
            StructField('CreatedDate', DateType(), False)
         ])
paperFieldsOfStudySchema = StructType([
            StructField('PaperId', LongType(), False),
            StructField('FieldOfStudyId', LongType(), False),
            StructField('Score', FloatType(), False)
         ])

# read settings
file_type = "csv"
file_sep = "\t"

# COMMAND ----------

# Filter Papers.txt to get only papers under consideration
papersDF = spark.read.format(file_type).load(storage_container_path+"mag/Papers.txt", schema=papersSchema, sep=file_sep)
filteredPapersDF = papersDF.filter(((papersDF.FamilyId == papersDF.PaperId) | (papersDF.FamilyId == None)) & (papersDF.Date >= date(2018, 11, 11)) & (papersDF.Date <= date(2020, 11, 11))).select(
  "PaperId",
  "Rank",
  "Date",
  "ReferenceCount",
  "CitationCount",
  "FamilyId",
  "FamilyRank"
)
filteredPapersDF.show()

# COMMAND ----------

# Filter FieldsOfStudy.txt to only get fields under consideration (too much data to consider all)
fieldsOfStudyDF = spark.read.format(file_type).load(storage_container_path+"advanced/FieldsOfStudy.txt", schema=fieldsOfStudySchema, sep=file_sep)

# only top 2 level fields (level <= 1) that are actually cited/published
fieldsOfStudyDF = fieldsOfStudyDF.filter((fieldsOfStudyDF.Level <= 1) & (fieldsOfStudyDF.PaperFamilyCount > 0) & (fieldsOfStudyDF.CitationCount > 0))
fieldsOfStudyDF.show()

# COMMAND ----------

# Filter PaperFieldsOfStudy.txt to only get fields under consideration
paperFieldsOfStudyDF = spark.read.format(file_type).load(storage_container_path+"advanced/PaperFieldsOfStudy.txt", schema=paperFieldsOfStudySchema, sep=file_sep)

#logic moved to join

paperFieldsOfStudyDF.show()

# COMMAND ----------

# join to get fields of study for given papers
joinedFieldsOfStudyDF = filteredPapersDF.join(paperFieldsOfStudyDF, filteredPapersDF.PaperId == paperFieldsOfStudyDF.PaperId, "inner") \
                        .join(fieldsOfStudyDF, paperFieldsOfStudyDF.FieldOfStudyId == fieldsOfStudyDF.FieldOfStudyId, "inner")
joinedFieldsOfStudyDF.show()

# COMMAND ----------

# Window functions and final filtering for top 4 per paper
# setup window specification to get top fields for each paper by score
fieldWindowSpec = Window.partitionBy(paperFieldsOfStudyDF.PaperId).orderBy(paperFieldsOfStudyDF.Score.desc())
# select and execute
joinedFieldsOfStudyDF = joinedFieldsOfStudyDF.select(
  filteredPapersDF.Date,
  paperFieldsOfStudyDF.PaperId,
  paperFieldsOfStudyDF.FieldOfStudyId,
  paperFieldsOfStudyDF.Score,
  row_number().over(fieldWindowSpec).alias("PaperFieldRank"),
  fieldsOfStudyDF.DisplayName,
  fieldsOfStudyDF.Level,
  fieldsOfStudyDF.PaperFamilyCount,
  fieldsOfStudyDF.CitationCount
).filter((col("PaperFieldRank") <= 4) | (fieldsOfStudyDF.Level == 0))
joinedFieldsOfStudyDF.show()

# COMMAND ----------

# write Papers to filesystem (coalesce() bottlenecks performance to one node, this could be improved)
filteredPapersDF.coalesce(1).write.mode("overwrite").option("header", True).format("com.databricks.spark.csv").save(storage_container_path+"filtered/Papers.csv")

# COMMAND ----------

# write FieldsOfStudy to filesystem (coalesce() bottlenecks performance to one node, this could be improved)
joinedFieldsOfStudyDF.coalesce(1).write.mode("overwrite").option("header", True).format("com.databricks.spark.csv").save(storage_container_path+"filtered/FieldsOfStudy.csv")
