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

