# Databricks notebook source
# MAGIC %fs
# MAGIC mkdirs "/FileStgore/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calling ETL reusable functions from one notebook to another
# MAGIC
# MAGIC ![Profile Pic](http://inceptez.com/wp-content/uploads/2015/11/new_logo.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Function to read data from filesystems using the parameters of `file type,spark session,source path,custom schema,mode (permissive/dropmalformed/failfast),infer schema flag,delimiter,headerflag`

# COMMAND ----------

def read_data(type,sparksess,src,strtype,mod='failfast',infersch=False,delim=',',headerflag=False
              ): # reusable function
    if type=='csv' and strtype!="":
     df1=sparksess.read.csv(src,schema=strtype, mode=mod,header=headerflag, inferSchema=infersch, sep=delim)
     return df1
    elif type=='csv':
     df1 = sparksess.read.csv(src, mode=mod, header=headerflag, inferSchema=infersch, sep=delim)
     return df1
    elif type=='json':
     df1 = sparksess.read.option("multiline", "true").schema(strtype).json(src)
     #df1.select(col("pagevisit").getItem(0)).show()
     #df1=sparksess.read.json(src)
     return df1


# COMMAND ----------

# MAGIC %md
# MAGIC ####Function to deduplicate with priority based on the arguments passed `dataframe passed, columns, priority of ordering, subset of columns`

# COMMAND ----------

def deDup(df,cols,ord,subst):# reusable function
    return df.sort(cols,ascending=ord).dropDuplicates(subset=subst)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Function to dynamically optimize the performance based on the arguments passed `spark session,dataframe passed,number of partitions,partflag,cacheflag,numshufflepart=200`
# MAGIC

# COMMAND ----------

def optimize_performance(sparksess,df,numpart,partflag,cacheflag,numshufflepart=200):
    print("Number of partitions in the given DF {}".format(df.rdd.getNumPartitions()))
    if partflag:
     df = df.repartition(numpart)
     print("repartitioned to {}".format(df.rdd.getNumPartitions()))
    else:
     df = df.coalesce(numpart)
     print("coalesced to {}".format(df.rdd.getNumPartitions()))
    if cacheflag:
     df.cache()
     print("cached ")
    if numshufflepart!=200:
     # default partions to 200 after shuffle happens because of some wide transformation spark sql uses in the background
     sparksess.conf.set("spark.sql.shuffle.partitions", numshufflepart)
     print("Shuffle part to {}".format(numshufflepart))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ####Function to clean/munge data using the parameters of `dataframe passed,dictionary to replace,subset of columns to drop,subset of columns to fill,subset of columns to replace,columns type of all or any`

# COMMAND ----------


def munge_data(df,dict1,drop,fill,replace,coltype='all'):# reusable function
    df1=df.na.drop(coltype,subset=drop)
    df2=df1.na.fill("na", subset=fill)
    df3=df2.na.replace(dict1, subset=replace)
    return df3

# COMMAND ----------

# MAGIC %md
# MAGIC ####UDF Function to using the parameter of `age` compute the category of age

# COMMAND ----------

def age_conversion(age):#Python Function for UDF conversion
   if age < 13:
      return "Children"
   elif age >=13 and age<=18:
      return "Teen"
   else:
      return "Adults"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Function to Filter the input data using the parameter of `filter conditions passed as argument`

# COMMAND ----------

def fil(df,condition):# reusable function
    return df.filter(condition)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Function to Mask the input using the parameter of `dataframe passed,columns to mask,mask type,bits of data to mask`

# COMMAND ----------

def mask_fields(df,cols,masktype,bits=-1):#is it supposed to be a inline or reusable function?reusable function
    #df.withColumn(i[0],masktype(i[0])).withColumn(i[1],masktype(i[1]))
    for i in cols:
        if (bits<0):
         df=df.withColumn(i,masktype(i))
        else:
         df=df.withColumn(i,masktype(i,bits))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ####Function to write the data into the RDBMS using the parameter of `dataframe passed,property file contains connection info,database name,table name,mode for overwrite/append`

# COMMAND ----------

from configparser import *
import io
def writeRDBMSData(df,propfile,db,tbl,mode):
    config_content = dbutils.fs.head(propfile)
    config_file = io.StringIO(config_content)
    config = ConfigParser()
    config.read_file(config_file)
    user = config['PRODDBCRED']['user']
    passwd = config['PRODDBCRED']['pass']
    host = config['PRODDBCRED']['host']
    port = config['PRODDBCRED']['port']
    driver = config['PRODDBCRED']['driver']
    url=host+":"+port+"/"+db
    url1 = url+"?user="+user+"&password="+passwd
    df.write.jdbc(url=url1, table=tbl, mode=mode, properties={"driver": driver})

# COMMAND ----------

spark.read.jdbc(url="jdbc:mysql://34.28.205.247:3306/custdbwe43?user=inceptez&password=Inceptez@123",table='cust_age_aggr2_Sarang').show()
