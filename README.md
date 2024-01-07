<center><h1>Spark</h1></center>
<center><h5>Big Data</h5></center>
<center><h5>EL hyani Asmae</h5></center>
<ol type="I">
<h2><li>HDFS Commands</li></h2>
<ul type="square">
<h3><li>Version</li></h3>
<h5>hadoop version</h5>
<h3><li>Help</li></h3>
<h5>hadoop fs –help cat</h5>
<h3><li>Start HDFS</li></h3>
<h5>start-dfs.sh </h5>
<h5>start-yarn.sh</h5>
<h5>jps</h5>
<h3><li>Create directory /enset/sdia</li></h3>
<h5>hdfs dfs -mkdir /enset</h5>
<h5>hdfs dfs -mkdir /enset/sdia</h5>
<h3><li>Create File</li></h3>
<h5>hdfs dfs –touchz /enset/sdia/File1</h5>
<h3><li>Display the content of directory</li></h3>
<h5>hdfs dfs –ls</h5>
<h3><li>Display the content of a File</li></h3>
<h5>hdfs dfs -cat [path_file]</h5>
<h6>---------------------HDFS----------------------</h6>
<p>[-f] -> Force</p>
<h3><li>Copy Files in HDFS</li></h3>
<h5>hdfs dfs -cp [-f] [HDFS src] [HDFS dest]</h5>
<h3><li>Move Files in HDFS</li></h3>
<h5>hdfs dfs -mv [HDFS src] [HDFS dest]</h5>
<h3><li>Remove Files in HDFS</li></h3>
<h5>hdfs dfs -rm [-skipTrash] [Path]</h5>
<h3><li>Remove Files and Directories Recursively in HDFS</li></h3>
<h5>hdfs dfs -rmr [-skipTrash] [path]</h5>
<h3><li>Empties the trash in HDFS</li></h3>
<h5>hdfs dfs -expunge</h5>
<h3><li>Display Disk Usage of a Path in HDFS</li></h3>
<h5>hdfs dfs -du [path]</h5>
<h6>---------------------Local/HDFS----------------------</h6>
<h3><li>Copy/move from Local to HDFS</li></h3>
<h5>hdfs dfs -copyFromLocal [-f] [local src] [HDFS dst]</h5>
<h5>hdfs dfs -moveFromLocal [local src] [HDFS dst]</h5>
<h3><li>Copy from HDFS to Local</li></h3>
<h5>hdfs dfs -copyToLocal [HDFS src] [local dst]</h5>
<h3><li>Put Local Files in HDFS</li></h3>
<h5>hdfs dfs -put [local src] [HDFS dst]</h5>
<h3><li>Get Files from HDFS to Local</li></h3>
<h5>hdfs dfs -get [hdfs src] [local dst]</h5>

</ul>


<h2><li>Dependencies</li></h2>

<h3>Spark core</h3>
<pre>
 &lt;dependency&gt;
    &lt;groupId&gt;org.apache.spark&lt;/groupId&gt;
    &lt;artifactId&gt;spark-core_2.13&lt;/artifactId&gt;
    &lt;version&gt;3.4.1&lt;/version&gt;
&lt;/dependency&gt;
</pre>

<h3>Spark SQL</h3>
<pre>
    &lt;!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql --&gt;
    &lt;dependency&gt;
        &lt;groupId&gt;org.apache.spark&lt;/groupId&gt;
        &lt;artifactId&gt;spark-sql_2.13&lt;/artifactId&gt;
        &lt;version&gt;3.4.1&lt;/version&gt;
    &lt;/dependency&gt;
</pre>

<h3>Spark MySQL Connector</h3>
<pre>
    &lt;!-- https://mvnrepository.com/artifact/mysql/mysql-connector-java --&gt;
    &lt;dependency&gt;
        &lt;groupId&gt;mysql&lt;/groupId&gt;
        &lt;artifactId&gt;mysql-connector-java&lt;/artifactId&gt;
        &lt;version&gt;8.0.33&lt;/version&gt;
    &lt;/dependency&gt;
</pre>
<h3>Spark Streaming</h3>
<pre>
    &lt;!-- https://mvnrepository.com/artifact/org.apache.spark/spark-streaming --&gt;
    &lt;dependency&gt;
        &lt;groupId&gt;org.apache.spark&lt;/groupId&gt;
        &lt;artifactId&gt;spark-streaming_2.13&lt;/artifactId&gt;
        &lt;version&gt;3.4.1&lt;/version&gt;
    &lt;/dependency&gt;
</pre>
<h3>Mlib</h3>
<pre>
    &lt;dependency&gt;
        &lt;groupId&gt;org.apache.spark&lt;/groupId&gt;
        &lt;artifactId&gt;spark-mllib_2.13&lt;/artifactId&gt;
        &lt;version&gt;3.5.0&lt;/version&gt;
    &lt;/dependency&gt;
</pre>


<h2><li>Spark Streaming</li></h2>
<h5>C:\Program Files (x86)\Nmap>ncat.exe -lvp 64256  [Windows]</h5>
<h5>nc -lk localhost (port)  [Linux]</h5>
<h3>-> DStream API -Words Count- </h3>
<p>Sockets as a source</p>
<pre>
public class App2 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("TP Streaming").setMaster("local[*]");
        JavaStreamingContext sc=new JavaStreamingContext(conf, Durations.seconds(8));
        JavaReceiverInputDStream&lt;String> dStream=sc.socketTextStream("localhost",64256);
        JavaDStream&lt;String> dStreamWord=dStream.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        //RDDs
        JavaPairDStream&lt;String,Integer> dPairStream=dStreamWord.mapToPair(m-> new Tuple2&lt;>(m,1));
        JavaPairDStream&lt;String,Integer> dStreamWordsCount=dPairStream.reduceByKey((a,b)->a+b);
        dStreamWordsCount.print();
        sc.start();
        sc.awaitTermination();
    }
}
</pre>

<p>HDFS as a source </p>
<pre>
public static void main(String[] args) throws InterruptedException {
        //API DStram
        SparkConf conf=new SparkConf().setAppName("TP Streaming").setMaster("local[*]");
        JavaStreamingContext sc=new JavaStreamingContext(conf, Durations.seconds(8));
        //listening to rep1 if we add a file in rep1 this app will trait it
        JavaDStream&lt;String> dStream=sc.textFileStream("hdfs://localhost:9000/rep1");
        JavaDStream&lt;String> dStreamWord=dStream.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        //RDDs
        JavaPairDStream&lt;String,Integer> dPairStream=dStreamWord.mapToPair(m-> new Tuple2<>(m,1));
        JavaPairDStream&lt;String,Integer> dStreamWordsCount=dPairStream.reduceByKey((a,b)->a+b);
        dStreamWordsCount.print();
        sc.start();
        sc.awaitTermination();
    }
</pre>
<h3>-> Structured Streaming </h3>
<h5>nc -lk localhost (port)  [Linux]</h5>
<pre>
public static void main(String[] args) throws Exception {
    SparkSession ss=SparkSession.builder().appName("Structured Streaming").master("local[*]").getOrCreate();
    Dataset&lt;Row> inputTable=ss.readStream()
            .format("socket")
            .option("host","localhost")
            .option("port",64256)
            .load();
    Dataset&lt;String> words=inputTable.as(Encoders.STRING())
            .flatMap((FlatMapFunction&lt;String,String>)line-> Arrays.asList(line.split(" ")).iterator(),Encoders.STRING() );
    Dataset&lt;Row> resultTable=words.groupBy("value").count();
    StreamingQuery query=resultTable.writeStream()
            .format("console")
            .outputMode("complete") //pour utiliser complete if faut utiliser une aggregation dans query
            //append, complete, update
            .start();
    query.awaitTermination();
    }
</pre>


<h2><li>Spark SQL</li></h2>
<h4>Read from csv File</h4>
<pre>
public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();
        Dataset&lt;Row> dframe1=ss.read().option("header",true).option("inferSchema",true).csv("incidents.csv");
        //dframe1.show();
        //dframe1.printSchema();
        //dframe1.select(col("price").plus(2000)).show();


        //1. Afficher le nombre d’incidents par service.
        dframe1.groupBy("service").count().show();
        //2. Afficher les deux années où il a y avait plus d’incidents.
        dframe1.groupBy(year(col("date")).alias("year")).count().orderBy(col("count").desc()).limit(2).show();

    }
</pre>
<h4>From DataBase</h4>
<pre>
SparkSession ss=SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();
        //Les noms des tables dans mon base de donnee : patients, 	medecins,consultations

        //- Afficher le nombre de consultations par jour.
        Dataset&lt;Row> df1=ss.read().format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/mastersdia")
                .option("user","root")
                //.option("dbtable","patients")
                .option("query","SELECT `DATE_CONSULTATION`,COUNT(*) as consultations " +
                        " FROM `consultations` GROUP BY `DATE_CONSULTATION`  ")
                .option("password","")
                .load();
        df1.show();
</pre>
<h4>From Json File</h4>
<pre>
    Dataset&lt;Row> df1=ss.read().option("multiline",true).json("products.json");
    df1.show();
    df1.printSchema();
    df1.select("name").show();
    df1.select(col("name").alias("Name of product")).show();
    df1.orderBy(col("name").asc()).show();
    df1.groupBy(col("name")).count().show();
    df1.limit(2).show();
    df1.filter(col("price").gt(19000)).show();
    df1.filter(col("name").equalTo("Dell").and(col("price").gt(17000))).show();
    df1.filter("name like 'Dell' and price>17000").show();
    //------------->Create a view
    df1.createOrReplaceTempView("products");
    ss.sql("select * from products ").show();
</pre>



<h2><li>PySpark</li></h2>
<p>Linux first command</p>
<p></p>


<h2><li>Machine Learning with Spark</li></h2>
<h4>Kmeans</h4>
<pre>
public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("Kmeans app").master("local[*]").getOrCreate();
        Dataset<Row> data=ss.read().option("inferSchema",true).option("header",true).csv("Mall_Customers.csv");
        VectorAssembler assembler=new VectorAssembler().setInputCols(
                new String[]{
                        "Age","Annual Income (k$)","Spending Score (1-100)"
                }
        ).setOutputCol("features");
        Dataset<Row> assembledDF=assembler.transform(data);
        //normalization
        MinMaxScaler scaler=new MinMaxScaler().setInputCol("features").setOutputCol("normalizedFeatures");
        Dataset<Row> normalizedDF =scaler.fit(assembledDF).transform(assembledDF);

        KMeans kMeans=new KMeans().setK(3)
                .setSeed(123)
                .setFeaturesCol("normalizedFeatures")
                .setPredictionCol("Cluster");
        KMeansModel model=kMeans.fit(normalizedDF);
        Dataset<Row> predictions=model.transform(normalizedDF);
        predictions.show(100);
        ClusteringEvaluator evaluator=new ClusteringEvaluator();
        double score=evaluator.evaluate(predictions);
        System.out.println("Score "+score);

    }
</pre>
<h4>Kmeans</h4>


<h2><li>Spark with Docker</li></h2>
<h4>Command to run the jar</h4>
<h5>docker exec -it spark-master spark-submit --class org.example.App2  /bitnami/Spark_Docker-1.0-SNAPSHOT.jar</h5>
<h4>App</h4>
<pre>
SparkSession ss=SparkSession.builder().appName("TP SPARK SQL").master("spark://spark-master:7077").getOrCreate();
Dataset&lt;Row> dframe1=ss.read().option("header",true).option("inferSchema",true).csv("/bitnami/incidents.csv");
</pre>


<h2><li>SQOOP</li></h2>
<h5>:/opt/lampp$   sudo ./manager-linux-x64.run</h5>
<h5>:/opt/lampp$   start-dfs.sh</h5>
<h5>:/opt/lampp$   start-yarn.sh</h5>
<h4>Import</h4>
<p>sqoop import --connect jdbc:mysql://localhost/spark_db --username "root" --password "" --table employees --target-dir /sqoop  {sqoop file in hdfs}</p>
<h4>Export</h4>
<p>sqoop export --connect jdbc:mysql://localhost/spark_db --username "root" --password "" --table "employees"  --export-dir "/sqoop_data" --input-fields-terminated-by ',' --input-lines-terminated-by '\n' </p>


<h2><li></li></h2>
</ol>

