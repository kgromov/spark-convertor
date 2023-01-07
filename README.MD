## Spark basics (Java based)

### Setup
*Environment variables*
* ``` System.setProperty("hadoop.home.dir", "C:\\apache\\spark\\winutils");```
* Same with HADOOP_HOME    
Setting above are not mandatory - just not to use local distributive instead of extracting from jar each time

*Spark config*
```java
SparkConf config = new SparkConf().setMaster("local").setAppName("Hello-World");
JavaSparkContext sparkContext = new JavaSparkContext(config);
```
_____

### Working with RDD
RDD - resilience distributed datasets
The easiest way - to read data from files:
```java
JavaRDD<String> userRDD = sparkContext.textFile("D:\\course_materials\\Hadoop\\ml-100k\\ml-100k\\u.user");
// then invoke some action operation like count() or collect()
List<String> rows = userRDD.collect();
```
____

### Dataset
JavaRDD is fundamental building block but it's more low level and not always proper for desired task.
Dataset - is a distributed collection of data organized into named columns.  
E.g. if we are working with SQL (NoSql) then Dataset is used instead. It's kind of superset on top of RDD which provides
more high level functions closed to SQL - aggregation, grouping, queries etc
#### Spark session
In order to work with Dataset it's require to init SparkSession first:
```java
 SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL")
                .getOrCreate();
```
And then get desired Dataset (which is generic) from SparkSession based on input of source of data.
E.g. for csv text file:
```java
 Dataset<Row> csvDS = sparkSession.read()
        .format("csv")
        .option("header", true)
        .option("delimiter", ";")
        .load("D:\\course_materials\\Hadoop\\ml-100k\\ml-100k\\u.csv");
```
or JDBC:
```java
Dataset<Row> movies = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/test")
                .option("user", "root")
                .option("password", "admin")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", "movie_ratings")
                .load();
        Dataset<Row> ratings = movies.select("rating").distinct().orderBy("rating");
```
What is interesting it's easy to convert text-based source Dataset to SQL-based:
```java
SQLContext sqlContext = csvDS.sqlContext();
Dataset<Row> csvDF = sqlContext.sql("select movie_id from movie_rating");
```

### Issues and troubleshooting 
* Export API to file system does not work - just fails with root cause:   
```java
    Caused by: java.io.IOException: Cannot run program "\bin\winutils.exe": CreateProcess error=216, This version of %1 is not compatible with the version of Windows you're running.
```
The only workaround - write by yourself with java.io/nio or do not use Windows :)
* Unable to serialize to pojo as is from Dataset<String> neither with javaSerialization nor beanEncoders.   
Workaround - extract serializer to a separate serializeable class (the whole tree of  fields should be serializeable as well)
* Latest ``mongo-spark-connector`` just does not provide API documented in previous versions [docs](ttps://www.mongodb.com/docs/spark-connector/v2.3/java/read-from-mongodb/).   
Some others API (e.g. Read/WriteConfig) became restricted for creation.

