# Databricks notebook source
# MAGIC %md
# MAGIC ###PySpark Core Use Cases
# MAGIC
# MAGIC After trying/completing the below interesting simple usecases, you will be definately getting some handson and improving programming skills in PySpark Core.
# MAGIC
# MAGIC #Note
# MAGIC  
# MAGIC - Import this notebook into the databricks environment
# MAGIC - Create the cells below to write %pyspark code under every markups given below
# MAGIC - If you are not able to achieve the result as I expected, try to get it as per the way you prefer.
# MAGIC - If you are failing for 1st time, try several times to make it succeeded, 
# MAGIC - Complete the other scenarios and come back if you feel struck in some use cases. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 1. Enable the DBFS File Browser (Top right Login -> Settings -> Advanced -> DBFS File Browser (Enable or disable DBFS File Browser))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2. Upload the given file youtube_videos.tsv into the dbfs location by browsing (catalog -> browse DBFS -> /FileStore/datafiles/)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 3. Create an RDD dbfs_rdd to read the above data

# COMMAND ----------

rdd= sc.textFile("dbfs:/FileStore/datafiles/youtube_videos.tsv")
rdd.getNumPartitions
print(rdd.take(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 4. Split the rows using tab ("\t") delimiter

# COMMAND ----------

rdd1=rdd.map(lambda x:x.split('\t'))
rdd1.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 5. Remove the header record by filtering the first column value does not contains "id" into an rdd split_rdd or try using take/first/zipWithIndex function to remove the header

# COMMAND ----------


header = rdd1.first()
splitrdd = rdd1.filter(lambda x: x != header)
splitrdd.take(2)





# COMMAND ----------

# MAGIC %md
# MAGIC ###### 6. cache the split_rdd

# COMMAND ----------

splitrdd.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ######7. Display only first 10 rows in the screen from spli_trdd.

# COMMAND ----------

splitrdd.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ######8. Filter only Music category data from split_rdd into an rdd called music_rdd

# COMMAND ----------

music_rdd = splitrdd.filter(lambda X: "Music" in X)
music_rdd.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ######9. Filter only duration>100 data from split_rdd into an rdd called long_dur_rdd

# COMMAND ----------

 long_dur_rdd =splitrdd.filter(lambda x : int(x[1])>100)
 long_dur_rdd.count() 
 long_dur_rdd.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ######10. Union music_rdd with longdur_rdd then convert to tuple and get only the deduplicated (distinct) records into an rdd music_longdur

# COMMAND ----------

unionrdd = music_rdd.union(long_dur_rdd)
tuple_rdd = unionrdd.map(lambda x: tuple(x))
distint_rdd = tuple_rdd.distinct()
music_longdur = distint_rdd 
music_longdur.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ######11. Select only id, duration, codec and category by re-ordering the fields like id,category,codec,duration into an rdd map_colsrdd

# COMMAND ----------

map_colsrdd = music_longdur.map(lambda x : (x[0],x[9],x[8],x[1])) 
map_colsrdd.take(30)


# COMMAND ----------

# MAGIC %md
# MAGIC ######12. Select only duration column from map_colsrdd and find max duration by using max function.

# COMMAND ----------

max_dur = map_colsrdd.map(lambda x:x[3]).max()

print(max_dur)





# COMMAND ----------

# MAGIC %md
# MAGIC ######13. Select only codec from map_colsrdd, convert to upper case and print distinct of it in the screen.

# COMMAND ----------

code_distrdd = map_colsrdd.map(lambda x:x[2].upper()).distinct()
code_distrdd.take(100)


# COMMAND ----------

# MAGIC %md
# MAGIC ######14. Create an rdd called filerdd_4part from file_rdd created in step1 by increasing the number of partitions to 4 (Execute this step anywhere in the code where ever appropriate)

# COMMAND ----------

filerdd_4part =rdd.repartition(4)
filerdd_4part.getNumPartitions
num_partitions = filerdd_4part.getNumPartitions()
print(num_partitions)

# COMMAND ----------

# MAGIC %md
# MAGIC ######15. Persist the filerdd4part data into memory and disk with replica of 2, (Execute this step anywhere in the code where ever appropriate)

# COMMAND ----------

from pyspark import StorageLevel

# Assuming filerdd_4part is already defined

# Persist the RDD with the specified storage level
filerdd_4part.persist(StorageLevel.MEMORY_AND_DISK_2)


# COMMAND ----------

# MAGIC %md
# MAGIC ######16. Calculate and print the overall total, max, min duration for Comedy category

# COMMAND ----------

# Assuming splitrdd contains data in the format (title, category, duration, ...)

# Filter splitrdd to include only Comedy category
comedy_rdd = splitrdd.filter(lambda x: "Comedy" in x[9])

# Map to durations and convert them to integers
duration_rdd = comedy_rdd.map(lambda x: int(x[1]))

# Calculate the overall total duration for Comedy category
total_duration = duration_rdd.sum()

# Calculate the maximum duration for Comedy category
max_duration = duration_rdd.max()

# Calculate the minimum duration for Comedy category
min_duration = duration_rdd.min()

# Print the results
print("Overall total duration for Comedy category:", total_duration)
print("Maximum duration for Comedy category:", max_duration)
print("Minimum duration for Comedy category:", min_duration)


# COMMAND ----------

# MAGIC %md
# MAGIC ######17. Print the codec wise count and minimum duration not by using min rather try to use reduce function function.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######17. Print the codec wise count and minimum duration not by using min rather try to use reduce function function.

# COMMAND ----------

# MAGIC %md
# MAGIC ######18. Print the distinct category of videos

# COMMAND ----------

video_rdd = splitrdd.map(lambda x:x[9]).distinct()
print(video_rdd.collect())

# code_distrdd = map_colsrdd.map(lambda x:x[2].upper()).distinct()
# code_distrdd.take(100)

# COMMAND ----------

# MAGIC %md
# MAGIC ######19. Print only the id, duration, height, category and width sorted by duration.

# COMMAND ----------

sor_dur = splitrdd.map(lambda x:(x[0],x[1],x[4],x[9],x[5])).sortBy(lambda x: x[1])
sor_dur.take(200)

# COMMAND ----------

# MAGIC %md
# MAGIC ######20. Create a python function called masking which should take the string as input and returns the hash value of the input string.

# COMMAND ----------

def masking(input_string):
    # Calculate the hash value of the input string
    hash_value = hash(input_string)
    return hash_value
input_str = "example"
hashed_value = masking(input_str)
print("Hash value of", input_str, "is:", hashed_value)


# COMMAND ----------

# MAGIC %md
# MAGIC ######21. Call the masking function created in the above step and pass category column and get the hashed value of category.

# COMMAND ----------

def masking(input_string):
    # Calculate the hash value of the input string
    hash_value = hash(input_string)
    return hash_value

# Assuming splitrdd is your RDD containing the data
category_rdd = splitrdd.map(lambda x: x[9])

# Call the masking function on the category RDD
hashed_categories = category_rdd.map(masking)

# Collect and print the hashed values
hashed_categories_list = hashed_categories.collect()
print("Hashed values of categories:", hashed_categories_list)


# COMMAND ----------

# MAGIC %md
# MAGIC ######22. Store the step 18 result in a dbfs location in a single file with data delimited as | with the id, duration, height, masking(category) and width columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ######23. Try to implement few performance optimization factors like partitioning, caching and broadcasting (whereever applicable)
