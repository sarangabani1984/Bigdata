# Databricks notebook source
# MAGIC %md
# MAGIC ![Profile Pic](http://inceptez.com/wp-content/uploads/2015/11/new_logo.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###This sparkETLMainPipeline1.py is a Modernized/Standardized code our Bread & Butter 2 PySpark Program
# MAGIC This program is using 2 notebooks (main and reusable functions), hence we can learn how to run another notebook from the current
# MAGIC ###Pipeline : DBFS -> ETL -> Hive Catalog & GCP Cloud SQL

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /FileStore/config/

# COMMAND ----------

# MAGIC %md
# MAGIC ####Inline Function Definitions

# COMMAND ----------

import sys
def reord_cols(df):#Inline Function
    return df.select("id","custprofession","custage","custlname","custfname")

def enrich(df):#Inline Function
    enrich_addcols_df6 = df.withColumn("curdt", current_date()).withColumn("loadts",current_timestamp())
    enrich_ren_df7 = enrich_addcols_df6.withColumnRenamed("srcsystem", "src")
    # Concat to combine/merge/melting the columns
    # try with withColumn (that add the derived/combined column in the last)
    enrich_combine_df8 = enrich_ren_df7.withColumn("nameprof",concat("custfname", lit(" is a "), "custprofession")).drop("custfname")
    # Splitting of Columns to derive custfname
    enrich_combine_split_df9 = enrich_combine_df8.withColumn("custfname", split("nameprof", ' ')[0])
    # Reformat same column value or introduce a new column by reformatting an existing column (withcolumn)
    enrich_combine_split_cast_reformat_df10 = enrich_combine_split_df9.withColumn("curdtstr", col("curdt").cast("string")).\
        withColumn("year", year(col("curdt"))).withColumn("curdtstr",concat(substring("curdtstr", 3, 2), lit("/"),substring("curdtstr", 6, 2))).\
        withColumn("dtfmt", date_format("curdt", 'yyyy/MM/dd hh:mm:ss'))
    return enrich_combine_split_cast_reformat_df10

def pre_wrangle(df):# Inline function
    return df.select("id", "custprofession", "custage", "src", "curdt")\
        .groupBy("custprofession") \
        .agg(avg("custage").alias("avgage")) \
        .where("avgage>49") \
        .orderBy("custprofession")
#Equivalent SQL
#select "custprofession", avg("custage") avgage from df group by custprofession having avgage>49 order by custprofession

def prewrang_anal(df):# Inline function
    sample1=df.sample(.2,10)
    smry=df.summary()
    coorval=df.corr("custage","custage")
    covval=df.cov("custage","custage")
    freqval = df.freqItems(["custprofession", "agegroup"], .4)
    return sample1,smry,coorval,covval,freqval

def aggregate_data(df):
    return df.groupby("year", "agegroup", "custprofession").agg(max("curdt").alias("max_curdt"), min("curdt").alias("min_curdt"),
                                                       avg("custage").alias("avg_custage"),
                                                       mean("custage").alias("mean_age"),
                                                       countDistinct("custage").alias("distinct_cnt_age"))\
                                                       .orderBy("year", "agegroup", "custprofession", ascending=[False, True, False])

def standardize_cols(df):#Inline function
    srcsys='Retail'
    #adding columns
    reord_added_df3=df.withColumn("srcsystem",lit(srcsys))
    #replacement of column(s)
    reord_added_replaced_df4=reord_added_df3.withColumn("custfname",col("custlname"))#preffered way if few columns requires drop
    # removal of columns
    chgnumcol_reord_df5=reord_added_replaced_df4.drop("custlname")#preffered way if few columns requires drop
    #achive replacement and removal using withColumnRenamed
    #chgnumcol_reord_df5.withColumnRenamed("custlname","custfname").withColumnRenamed("custage","age").show()#preffered way if few columns requires drop
    return chgnumcol_reord_df5

def ret_struct():
    strt=StructType([StructField("id", IntegerType(), False),
                                  StructField("custfname", StringType(), False),
                                  StructField("custlname", StringType(), True),
                                  StructField("custage", ShortType(), True),
                                  StructField("custprofession", StringType(), True)])
    return strt

# COMMAND ----------

# MAGIC %md
# MAGIC ####Main Method Starts here

# COMMAND ----------

def main(arg):
    print("define spark session object (inline code)")
    spark = SparkSession.builder\
       .appName("Very Important SQL End to End App") \
        .config("spark.sql.shuffle.partitions", "4") \
        .enableHiveSupport()\
       .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    print("Set the logger level to error")
    spark.sparkContext.setLogLevel("ERROR")
    print("1. Data Munging")
    print("a. Raw Data Discovery (EDA) (passive) - Performing an (Data Exploration) exploratory data analysis on the raw data to identify the properties of the attributes and patterns.")
    #I will first take some sample data or actual data and analyse about the columns, datatype, values, nulls, duplicates(low/high cardinality), format
    #statistical analysis - min/max/difference/mean(mid)/counts
    print("b. Combining Data + Schema Evolution/Merging (Structuring)")
    print("b.1. Combining Data- Reading from a path contains multiple pattern of files")
    print("b.2. Combining Data - Reading from a multiple different paths contains multiple pattern of files")
    print("b.3. Schema Merging (Structuring) - Schema Merging data with different structures (we know the structure of both datasets)")
    print("b.4. Schema Evolution (Structuring) - source data is evolving with different structure")
    print("c.1. Validation (active)- DeDuplication")

    # Inline function calling (modularized)
    custstructtype1 = ret_struct()#Inline function calling
#or
    # inline code (clumsy)
    custstructtype1=StructType([StructField("id", IntegerType(), False),
                                  StructField("custfname", StringType(), False),
                                  StructField("custlname", StringType(), True),
                                  StructField("custage", ShortType(), True),
                                  StructField("custprofession", StringType(), True)])

    #Let us clean and get the right data for further consideration
    custdf_clean=read_data('csv',spark,arg[1],custstructtype1,'dropMalformed')#reusable function calling
    custdf_optimized=optimize_performance(spark,custdf_clean,4,True,True,2)#reusable function calling
    custdf_optimized.printSchema()
    custdf_optimized.show(2,False)
    print("*******Dropping Duplicates of cust data**********")
    dedup_dropduplicates_df=deDup(custdf_optimized,["custage"],[False],["id"])#reusable function calling
    dedup_dropduplicates_df.where("id=4000003").show(4)

    #Inline coding
    txnsstructtype2=StructType([StructField("txnid",IntegerType(),False),StructField("dt",StringType()),
                                StructField("custid",IntegerType()),StructField("amt",DoubleType()),
                                StructField("category",StringType()),StructField("product",StringType()),
                                StructField("city",StringType()),StructField("state",StringType()),
                                StructField("spendby",StringType())])

    txns=read_data('csv',spark,arg[2],txnsstructtype2,'dropMalformed')

    txns_clean_optimized = optimize_performance(spark, txns, 1, False, False, 1) #reusable function calling
    print("*******Dropping Duplicates of txns data**********")
    txns_dedup=deDup(txns_clean_optimized,["dt","amt"],[False,False],["txnid"])#reusable function calling
    txns_dedup.show(2)

    print("c.2. Data Preparation (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
    print("Replace (na.replace) the key with the respective values in the columns "
           "(another way of writing Case statement)")
    prof_dict={"Therapist":"Physician","Musician":"Music Director","na":"prof not defined"}
    dedup_dropfillreplacena_clensed_scrubbed_df1=munge_data(dedup_dropduplicates_df,prof_dict,["id"],["custlname","custprofession"],["custprofession"],'any')
    dedup_dropfillreplacena_clensed_scrubbed_df1.show()

    print("d.1. Data Standardization (column) - Column re-order/number of columns changes (add/remove/Replacement)  to make it in a usable format")
    reord_df2=reord_cols(dedup_dropfillreplacena_clensed_scrubbed_df1)#inline function creation & calling
    #reord_df2=dedup_dropfillreplacena_clensed_scrubbed_df1.select("id", "custprofession", "custage", "custlname", "custfname")#inline code
    reord_df2.show(2,False)
    #Convert the below code as a inline function
    munged_df=standardize_cols(reord_df2)#inline function calling
    #Try to do this -> convert into inline function
    #reord_df2 -> chgnumcol_reord_df5

    #munging
    #read_data() -> cleanse_data() -> optimize_data() -> munge_data() -> reord_cols() -> standardize_cols()
    print("********************data munging completed (read_data() -> cleanse_data() -> optimize_data() -> munge_data() -> reord_cols() -> standardize_cols())****************")

    #TRANSFORMATION PART#
    ###########Data processing or Curation or Transformation Starts here###########
    print("***************2. Data Enrichment (values)-> Add, Rename, combine(Concat), Split, Casting of Fields, Reformat, "
          "replacement of (values in the columns) - Makes your data rich and detailed *********************")
    munged_enriched_df=enrich(munged_df)
    munged_enriched_df.show(4)

    print("***************3. Data Customization & Processing (Business logics) -> Apply User defined functions and utils/functions/modularization/reusable functions & reusable framework creation *********************")
    print("Data Customization can be achived by using UDFs - User Defined Functions")
    #Step1: Create a Python function
    #Step2: Importing UDF spark library
    from pyspark.sql.functions import udf
    #Step3A: Converting the above function using UDF into user-defined function (DSL)
    age_custom_validation = udf(age_conversion)
    #Step4: New column deriviation called age group, in the above dataframe (Using DSL)
    custom_agegrp_munged_enriched_df = munged_enriched_df.withColumn("custage",coalesce(col("custage"),lit(0))).withColumn("agegroup",age_custom_validation("custage"))
    custom_agegrp_munged_enriched_df.show(2)

    print("***************4. Core Data Processing/Transformation (Level1) (Pre Wrangling) Curation -> "
          "filter, transformation, Grouping, Aggregation/Summarization, Analysis/Analytics *********************")
    print("Transformation Functions -> select, filter, sort, group, aggregation, having, transformation/analytical function, distinct...")
    pre_wrangled_customized_munged_enriched_df=pre_wrangle(custom_agegrp_munged_enriched_df)#inline function
    print(pre_wrangled_customized_munged_enriched_df)#We can use thid DF to store in some target systems

    print("Filter rows and columns")
    filtered_nochildren_rowcol_df_for_further_wrangling1=fil(custom_agegrp_munged_enriched_df,"agegroup<>'Children'")\
        .select("id","custage","curdt","custfname","year","agegroup")#reusable function call and inline code also
    filtered_nochildren_rowcol_df_for_further_wrangling1.show(2)
    dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2=fil(custom_agegrp_munged_enriched_df,"agegroup<>'Children'")
    aggr_df=aggregate_data(dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2)

    print("Tell me the average age of the above customer is >35 (adding having)")
    aggr_filter_df=fil(aggr_df,"avg_custage>35")#having clause
    aggr_filter_df.show(2)

    print("Analytical Functionalities")
    #Data Random Sampling:
    #randomsample1_for_consumption3=custom_agegrp_munged_enriched_df.sample(.2,10)#Consumer (Datascientists needed for giving training to the models)
    sampledf,summarydf,corrval,covval,freqdf=prewrang_anal(custom_agegrp_munged_enriched_df)
    sampledf.show(2)
    summarydf.show(2)
    print(f"co-relation value of age is {corrval}")
    print(f"co-variance value of age is {covval}")
    freqdf.show(2)
    #munged+enriched+customized df -> pre_wrangle() -> fil() -> aggregate_data() -> fil()
    #munged+enriched+customized df -> prewrang_anal()
    masked_df=mask_fields(custdf_clean, ["custlname", "custfname"], md5)

    print("***************5. Core Data Curation/Processing/Transformation (Level2) Data Wrangling -> Joins, Lookup, Lookup & Enrichment, Denormalization,Windowing, Analytical, set operations, Summarization (joined/lookup/enriched/denormalized) *********************")
    denormalizeddf=custdf_clean.alias("c").join(txns_dedup.alias("t"),on=[col("c.id")==col("t.custid")],how="inner")
    denormalizeddf.show(2)
    rno_txns3 = denormalizeddf.select("*", row_number().over(Window.orderBy("dt")).alias("sno"))
    rno_txns3.show(2)

    print("***************6. Data Persistance (LOAD)-> Discovery, Outbound, Reports, exports, Schema migration  *********************")
    print("Random Sample DF to File System")
    sampledf.write.mode("overwrite").csv("/FileStore/ETLOutput/randomsample1_for_consumption3")
    print("Denormalized DF to File System")
    denormalizeddf.write.mode("overwrite").json("/FileStore/ETLOutput/leftjoined_aggr2")
    print("Cleansed DF to Hive Table")
    custdf_clean.write.mode("overwrite").partitionBy("custprofession").saveAsTable("default.cust_prof_part_tbl")
    print("Masked DF to File System")
    masked_df.write.mode("overwrite").csv("/FileStore/ETLOutput/masked_cust_data")
    print("Aggregated DF to File System")
    print("prop file is ",arg[3])
    writeRDBMSData(aggr_filter_df, arg[3], 'custdbwe43', 'cust_age_aggr2_Sarang','overwrite')
    #aggr_filter_df.write.jdbc(url="jdbc:mysql://34.72.107.154:3306/custdb?user=irfan&password=Inceptez@123", table="cust_age_aggr1",
              # mode="overwrite", properties={"driver": 'com.mysql.jdbc.Driver'})
    print("Spark App1 Completed Successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Textbox widget to pass params

# COMMAND ----------

dbutils.widgets.text("cust_data_path", "")
dbutils.widgets.text("txns_data_path", "")
dbutils.widgets.text("connection_properties_path", "")

input_path1 = dbutils.widgets.get("cust_data_path")
input_path2 = dbutils.widgets.get("txns_data_path")
connection_properties_path3 = dbutils.widgets.get("connection_properties_path")
print(connection_properties_path3)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Util to run reusable functions

# COMMAND ----------

# MAGIC %run "./reusable_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Main method call with arguments

# COMMAND ----------

if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql.window import *
    from pyspark.sql.functions import *
    print(input_path1,input_path2,connection_properties_path3)
    arg = ["ETL Pipeline",input_path1,input_path2,connection_properties_path3]
    main(arg)

# COMMAND ----------


