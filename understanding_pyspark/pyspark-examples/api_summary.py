import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc, lit, concat_ws, current_date
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness 
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance,var_samp,  var_pop
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

-------------------------------------------------------------------------------------------
# intialize spark object
spark = SparkSession.builder.master('local[2]').appName('my_app').getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]

-------------------------------------------------------------------------------------------
# create dataframe 
df = spark.createDataFrame(data = simpleData, schema = columns)

df = spark.read.csv(file_path)
df = spark.read.options(header='true', inferSchema='true') \
          .csv(csv_filePath)

# to and from pandas
sparkDF2 = spark.createDataFrame(pandasDF,schema=mySchema)
pandasDF2 = sparkDF2.select("*").toPandas

-------------------------------------------------------------------------------------------
# defining schema, structtypes
schema = StructType([
            StructField("city", StringType(), True),
            StructField("dates", StringType(), True),
            StructField("population", IntegerType(), True)])
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

data = [
    ("James Smith", ["Java","Scala","C++"]), 
    ("Michael,Rose,", ["Spark","Java","C++"])
]
schema = StructType([
    StructField('name', StringType(), True),
    StructField('languages', ArrayType(StringType()), True)
])
-------------------------------------------------------------------------------------------
# view the dataframe
df.shape()
df.printSchema()
df.show(truncate=False)
df.limit(3)  #Returns a new Dataset by taking the first n rows.
df.take(2)  #Action, Return Array[T]  # Internally calls limit and collect
df.head(2)  #Return Array[T]
df.tail(2)  #Return Array[T]
df.collect()  # Return Array[T]
-------------------------------------------------------------------------------------------
# distinct
distinctDF = df.distinct()
n = df.count()
d = distinctDF.count()
dropDisDF = df.dropDuplicates(["department","salary"])

df.distinct()
df.count()
df.distinct().count()
df.select(countDistinct("Dept","Salary"))
-------------------------------------------------------------------------------------------
# select
df.select(df.gender)
df.select(df["gender"])
df.select(col("gender"))
df.select(df.property.hair) #if property column is having array of [hair, eyes, legs]
df.select("name.*")
df.select("name.firstname", "name.lastname")
-------------------------------------------------------------------------------------------
# converting to doubleType
df.withColumn("salary",df.salary.cast('double')).printSchema()    
df.withColumn("salary",df.salary.cast(DoublerType())).printSchema()  
-------------------------------------------------------------------------------------------
# sort the dataframe
df.sort("department","state").show(truncate=False)
df.sort(col("department"),col("state")).show(truncate=False)

df.orderBy("department","state").show(truncate=False)
df.orderBy(col("department"),col("state")).show(truncate=False)

df.sort(df.department.asc(),df.state.asc()).show(truncate=False)
df.sort(col("department").asc(),col("state").asc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").asc()).show(truncate=False)

-------------------------------------------------------------------------------------------
# using the sql like query
df.createOrReplaceTempView("EMP_VIEW")
df.createOrReplaceGlobalView("EMP_VIEW")

df.select("employee_name",asc("department"),desc("state"),"salary","age","bonus")
spark.sql("select employee_name,department,state,salary,age,bonus \
          from EMP ORDER BY department asc").show(truncate=False)

-------------------------------------------------------------------------------------------
#column operators
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show() 
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
df.select(df.col1 % df.col2).show()
df.select(df.col2 > df.col3).show()
df.select(df.col2 < df.col3).show()
df.select(df.col2 == df.col3).show()
-------------------------------------------------------------------------------------------
# add new column
df.withColumn("bonus_percent", lit(0.3))
df.withColumn("bonus_amount", df.salary*0.3)
df.withColumn("name", concat_ws(",","firstname",'lastname'))
df.withColumn("current_date", current_date())
df.select("firstname","salary", lit(0.3).alias("bonus"))
df.select("firstname","salary", lit(df.salary * 0.3))
spark.sql("select firstname,salary, '0.3' as bonus from EMP_VIEW")
df.withColumn("grade", \
   when((df.salary < 4000), lit("A")) \
     .when((df.salary >= 4000) & (df.salary <= 5000), lit("B")) \
     .otherwise(lit("C")) \
  )

df2 = df.withColumn("new_gender", when(df.gender == "M","Male") \
                                 .when(df.gender == "F","Female") \
                                 .when(df.gender.isNull() ,"") \
                                 .otherwise(df.gender))

# using getItem() to access elements of array
df.withColumn("hair", df.properties.getItem("hair")) \
  .withColumn("eyes", df.properties.getItem("eyes")) \
  .drop("properties")
-------------------------------------------------------------------------------------------
## drop column
df.drop(df.firstname)
df.drop('id', 'gender', 'age')

## drop null records
df.na.drop()
df.na.drop(how="any")
df.na.drop(subset=["population","type"])
df.dropna()

#handle null values
df.fillna(value=0)
df.fillna(value=0,subset=["population"])
df.na.fill(value=0)
df.na.fill(value=0,subset=["population"])
df.fillna({"city": "unknown", "type": "Old"})

-------------------------------------------------------------------------------------------
# rdd.map
StructField('properties', MapType(StringType(),StringType()),True)
df3=df.rdd.map(
    lambda x: (x.name, x.properties["hair"], x.properties["eye"])
).toDF(["name","hair","eye"])

-------------------------------------------------------------------------------------------
## aggregation
df.select(collect_list("salary"))
df.select(collect_set("salary"))
df.select(countDistinct("department", "salary"))
df.select(first("salary"))
df.select(last("salary"))
df.select(max("salary"))
df.select(min("salary"))
df.select(kurtosis("salary"))
df.select(mean("salary"))
df.select(median("salary"))
df.select(mode("salary"))
df.select(skewness("salary"))
df.select(sum("salary"))
df.select(stddev("salary"))
df.select(sumDistinct("salary"))
df.select(variance("salary"))

-------------------------------------------------------------------------------------------
# cast column
df2 = df.withColumn("age",col("age").cast(StringType())) \
    .withColumn("isGraduated",col("isGraduated").cast(BooleanType())) \
    .withColumn("jobStartDate",col("jobStartDate").cast(DateType()))
df4 = spark.sql("SELECT STRING(age),BOOLEAN(isGraduated),DATE(jobStartDate) from EMP_VIEW")

-------------------------------------------------------------------------------------------
## collect
dataCollect = deptDF.collect()
dataCollect = deptDF.select("dept_name").collect()
for row in dataCollect:
    print(row['dep_name'] + ',' + row['dep_id'])
    
-------------------------------------------------------------------------------------------
## filter
df.sort(df.fname.asc())
df.select(df.fname,df.id.cast("int"))
df.filter(df.id.between(100,300))
df.filter(df.fname.contains("Cruise"))
df.filter(df.fname.startswith("T"))
df.filter(col("name").startswith("J"))
df.filter(df.fname.endswith("Cruise"))
df.filter(df.lname.isNull())
df.filter(df.lname.isNotNull())
df.filter(df.fname.like("%om"))
df.select(df.fname.substr(1,2).alias("substr"))
df.filter(df.id.isin(my_list))

df.filter(df.state.isNull())
df.filter("state IS NULL AND gender IS NULL")
df.filter(df.state.isNull() & df.gender.isNull())
df.filter(df.state.isNotNull())

-------------------------------------------------------------------------------------------
## groupby
df.groupBy("department").count()
df.groupBy("department","state").sum("salary","bonus")
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
         avg("salary").alias("avg_salary"), \
         sum("bonus").alias("sum_bonus"), \
         max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000)

-------------------------------------------------------------------------------------------
## window function
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number", row_number().over(windowSpec))
df.withColumn("lag",lag("salary",2).over(windowSpec))
# can use rank(), dense_rank(), ntile(), lag, lead
df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max")

-------------------------------------------------------------------------------------------
# when otherwise
df2 = df.withColumn("new_gender", when(col("gender") == "M", "Male") \
                                 .when(col("gender") == "F", "Female") \
                                 .otherwise("Unknown")
                   )

-------------------------------------------------------------------------------------------
# expr: to use sql like expression within your pyspark code
df3 = df.withColumn("new_gender", expr("case when gender = 'M' then 'Male' " + 
                       "when gender = 'F' then 'Female' " +
                       "else 'Unknown' end"))

-------------------------------------------------------------------------------------------
# join dataframes
empDF.join(addDF, empDF["emp_id"] == addDF["emp_id"])
empDF.join(addDF,["emp_id"]) \
     .join(deptDF,empDF["emp_dept_id"] == deptDF["dept_id"])
empDF.join(deptDF).where(empDF["emp_dept_id"] == deptDF["dept_id"])

df = df1.join(df2, (df1.A1 == df2.B1) & (df1.A2 == df2.B2))
df = spark.sql("select * \
               from EMP e, DEPT d, ADD a \
               where e.emp_dept_id == d.dept_id and e.emp_id == a.emp_id")

# union
unionDF = df.union(df2)
unionDF = df.union(df2).distinct()
unionAllDF = df.unionAll(df2)

-------------------------------------------------------------------------------------------
## datetime
df.withColumn('date_type', col('observation_dt').cast('date'))
df.withColumn('date_type', to_timestamp('observation_dt').cast('date'))
df.withColumn("date_type",to_date("input_timestamp"))
df.withColumn("ts",to_timestamp(col("input_timestamp")))
df.withColumn('DiffInSeconds',col("end_timestamp").cast("long") - col('from_timestamp').cast("long"))
df.withColumn('end_timestamp', current_timestamp())

df.select(col("input"), to_date(col("input"),"MM-dd-yyyy").alias("date"))
df.select(to_timestamp(lit('06-24-2019 12:01:19.000'),'MM-dd-yyyy HH:mm:ss.SSSS'))

df2 = df.select(split(col("fullname"), ",").alias("NameArray")).drop("name")
-------------------------------------------------------------------------------------------
## collects

df.collect() returns Array of Row type.
df.collect()[0] returns the first element in an array (1st row).
df.collect[0][0] returns the value of the first row & first column.
states = df.rdd.map(lambda x: x[3]).collect()

# row
collData=rdd.collect()
print(collData)
for row in collData:
    print(row.name + "," +str(row.lang))
-------------------------------------------------------------------------------------------
# parallelize()  -to create rdd from list collection
sparkContext=spark.sparkContext
rdd = sparkContext.parallelize([1,2,3,4,5])
print(rdd.getNumPartitions(), rdd.first())

emptyRDD = sparkContext.emptyRDD()
emptyRDD = sparkContext.parallelize([])

-------------------------------------------------------------------------------------------
#RDD
rdd = spark.sparkContext.textFile("/path/textFile.txt")
rdd = spark.sparkContext.emptyRDD
rdd = spark.sparkContext.parallelize([],10) #This creates 10 partitions
rdd = sparkContext.parallelize([1,2,3,4,56,7,8,9,12,3], 10)

# repartition and coalesce
reparRdd = rdd.repartition(4)

-------------------------------------------------------------------------------------------
# transformations
rdd = spark.sparkContext.textFile("/tmp/test.txt")

rdd2 = rdd.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x,1))
rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
rdd4 = rdd3.filter(lambda x : 'an' in x[1])

#actions
rdd6.count()
rdd.first()
rdd.max()
rdd6.reduce(lambda a, b: (a[0]+b[0],a[1]))
data3 = rdd6.take(3)  #  Returns the record specified as an argument.
data = rdd6.collect() # Returns all data from RDD as an array.
rdd6.saveAsTextFile("/tmp/wordCount")

-------------------------------------------------------------------------------------------
# cache and persist

cachedRdd = rdd.cache()  # PySpark cache() method in RDD class internally calls persist() 
dfPersist = rdd.persist(pyspark.StorageLevel.MEMORY_ONLY)
rddPersist2 = rddPersist.unpersist()

-------------------------------------------------------------------------------------------
# conversions
dfFromRDD1 = rdd.toDF()  # Converts RDD to DataFrame
rdd = df.rdd  # Convert DataFrame to RDD

dfFromRDD2 = rdd.toDF("col1","col2")
df = spark.createDataFrame(rdd).toDF("col1","col2")

# list to df
dep = [Row("Finance",10), Row("Marketing",20), Row("Sales",30)]
rdd = spark.sparkContext.parallelize(dep)

-------------------------------------------------------------------------------------------
# read and write csv
schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("Zipcode",IntegerType(),True) \
      .add("ZipCodeType",StringType(),True)
df = spark.read.options(header='True', delimiter=',').csv("C:/resources/zipcodes.csv")
df.write.option("header", True).csv("/tmp/zipcodes123")

-------------------------------------------------------------------------------------------
# renaming column
df.withColumnRenamed("dob","DateOfBirth") \
  .withColumnRenamed("salary","salary_amount")

schema2 = StructType(.....)
df.select(col("name").cast(schema2))

df.select(col("name.firstname").alias("fname"),
          col("name.middlename").alias("mname"),
          col('salary'))

-------------------------------------------------------------------------------------------
# repartition - performs full shuffle, costly operation
newDF=df.repartition(3)
df2=df.repartition(3, "state")

newDF.write.option("header",True).mode("overwrite").csv("/tmp/zipcodes-state")

df = spark.range(0,20)
print(df.rdd.getNumPartitions())
spark.conf.set("spark.sql.shuffle.partitions", "500")


# SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('app.com') \
                    .getOrCreate()
sc = spark.sparkContext
name = sc.appName
master = sc.master
sparkSession3 = SparkSession.newSession
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------

"""parallelize is a function in SparkContext and is used to create an RDD from a list collection.
RDD, Resilient Distributed Datasets (RDD) is a fundamental data structure of PySpark, It is an immutable distributed collection of objects. Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster.
Partitions are basic units of parallelism in PySpark. RDDs in PySpark are a collection of partitions.
"""
-----------------------------------------------------------------------------------------------------------
"""
PySpark RDD/DataFrame collect() is an action operation that is used to retrieve all the elements of the dataset (from all nodes) as an Array of row type to the driver node. We should use the collect() on smaller dataset usually after filter(), group() e.t.c. Retrieving larger datasets results in OutOfMemory error.
Note that collect() is an action hence it does not return a DataFrame instead, it returns data in an Array to the driver. Once the data is in an array, you can use python for loop to process it further.
df.select() is a transformatoin whereas df.collect() is an action.
"""
-----------------------------------------------------------------------------------------------------------
"""
RDD (Resilient Distributed Dataset) is a fundamental building block of PySpark which is fault-tolerant, immutable distributed collections of objects. Immutable meaning once you create an RDD you cannot change it. Each record in RDD is divided into logical partitions, which can be computed on different nodes of the cluster. 
RDDs are a collection of objects similar to list in Python, with the difference being RDD is computed on several processes scattered across multiple physical servers also called nodes in a cluster while a Python collection lives and process in just one process.
"""
-----------------------------------------------------------------------------------------------------------
"""
PySpark loads the data from disk and process in memory and keeps the data in memory, this is the main difference between PySpark and Mapreduce (I/O intensive). In between the transformations, we can also cache/persists the RDD in memory to reuse the previous computations.

IMMUTABLE: once RDDs are created you cannot modify. When we apply transformations on RDD, PySpark creates a new RDD and maintains the RDD Lineage.
FAULT_TAULRENT: any RDD operation fails, it automatically reloads the data from other partitions. PySpark task failures are automatically recovered for a certain number of times.
LAZY_EVALUATION: it keeps the all transformations as it encounters(DAG) and evaluates the all transformation when it sees the first RDD action.
PARTITIONING:  by default partitions the elements in a RDD. By default it partitions to the number of cores available.
"""
-----------------------------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
spark:SparkSession = SparkSession.builder() \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()
"""
master() – If you are running it on the cluster you need to use your master name as an argument to master(). usually, it would be either yarn (Yet Another Resource Negotiator) or mesos depends on your cluster setup.

Use local[x] when running in Standalone mode. x should be an integer value and should be greater than 0; this represents how many partitions it should create when using RDD, DataFrame, and Dataset. Ideally, x value should be the number of CPU cores you have.
appName() – Used to set your application name.

getOrCreate() – This returns a SparkSession object if already exists, and creates a new one if not exist.
"""
-----------------------------------------------------------------------------------------------------------
"""
RDD’s are created primarily in two different ways,

parallelizing an existing collection and
referencing a dataset in an external storage system (HDFS, S3 and many more). 
"""
-----------------------------------------------------------------------------------------------------------
"""
Difference between Pyspark and MapReduce

"""
-----------------------------------------------------------------------------------------------------------
"""
Sometimes we may need to repartition the RDD, PySpark provides two ways to repartition; first using repartition() method which shuffles data from all nodes also called full shuffle and second coalesce() method which shuffle data from minimum nodes, for examples if you have data in 4 partitions and doing coalesce(2) moves data from just 2 nodes.  
Both of the functions take the number of partitions to repartition rdd as shown below.  Note that repartition() method is a very expensive operation as it shuffles data from all nodes in a cluster. 
The repartition() method is used to increase or decrease the number of partitions of an RDD or dataframe in spark. This method performs a full shuffle of data across all the nodes. It creates partitions of more or less equal in size. This is a costly operation given that it involves data movement all over the network.
"""
-----------------------------------------------------------------------------------------------------------
"""
RDD transformations – Transformations are lazy operations, instead of updating an RDD, these operations return another RDD. they don’t execute until you call an action on RDD.
RDD actions – operations that trigger computation and return RDD values.

list = [1,2,3,4,5,6,7,8]
rdd will be=>  partition1:[1,2], partition1:[3,4], partition1:[5,6], partition4:[7,8]

When we use parallelize() or textFile() or wholeTextFiles() methods of SparkContxt to initiate RDD, it automatically splits the data into partitions based on resource availability. when you run it on a laptop it would create partitions as the same number of cores available on your system.

Transformations on PySpark RDD returns another RDD and transformations are lazy meaning they don’t execute until you call an action on RDD. Some transformations on RDD’s are flatMap(), map(), reduceByKey(), filter(), sortByKey() and return new RDD instead of updating the current.
flatmap
map
reduceByKey
filter
sortByKey
"""
-----------------------------------------------------------------------------------------------------------
"""
https://sparkbyexamples.com/pyspark-rdd/ 
Types of RDD
    PairRDDFunctions or PairRDD – Pair RDD is a key-value pair This is mostly used RDD type, 
    ShuffledRDD – 
    DoubleRDD – 
    SequenceFileRDD – 
    HadoopRDD – 
    ParallelCollectionRDD – 
"""
-----------------------------------------------------------------------------------------------------------
"""
## RDD cache and persist
PySpark Cache and Persist are optimization techniques to improve the performance of the RDD jobs that are iterative and interactive.
Though PySpark provides computation 100 x times faster than traditional Map Reduce jobs, If you have not designed the jobs to reuse the repeating computations you will see degrade in performance. Hence, we need to look at the computations and use optimization techniques.

Using cache() and persist() methods, PySpark provides an optimization mechanism to store the intermediate computation of an RDD so they can be reused in subsequent actions.
When you persist or cache an RDD, each worker node stores it’s partitioned data in memory or disk and reuses them in other actions on that RDD. And Spark’s persisted data on nodes are fault-tolerant meaning if any partition is lost, it will automatically be recomputed using the original transformations that created it.

Advantages of persisting rdd:
Cost efficient – PySpark computations are very expensive hence reusing the computations are used to save cost.
Time efficient – Reusing the repeated computations saves lots of time.
Execution time – Saves execution time of the job which allows us to perform more jobs on the same cluster.

PySpark cache() method in RDD class internally calls persist() method which in turn uses sparkSession.sharedState.cacheManager.cacheQuery to cache the result set of RDD. 
"""
-----------------------------------------------------------------------------------------------------------

"""
PySpark Shared Variables: Broadcast and Accumulators

When PySpark executes transformation using map() or reduce() operations, It executes the transformations on a remote node by using the variables that are shipped with the tasks and these variables are not sent back to PySpark Driver hence there is no capability to reuse and sharing the variables across tasks. PySpark shared variables solve this problem using the below two techniques. PySpark provides two types of shared variables.
    Broadcast variables (read-only shared variable)
    Accumulator variables (updatable shared variables)
"""

-----------------------------------------------------------------------------------------------------------