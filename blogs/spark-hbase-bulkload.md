项目中有需求将Hive的表存储在HBase中。通过Spark访问Hive表，通过一定ETL过程生成HFile，并通知HBase进行bulk load。实测这是导数最快的手段。

## 环境：

CDH : 5.7.0

Hadoop : 2.6.0-cdh5.7.0

Spark : 1.6.0-cdh5.7.0

Hive : 1.1.0-cdh5.7.0

HBase : 1.2.0-cdh5.7.0

## pom

Hadoop项目里面，最坑的就是依赖关系复杂，然后经常会发现一些冲突包。。。下面是呕心沥血整理出无冲突的POM。

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>your.group.id</groupId>
    <artifactId>your.artifact.id</artifactId>
    <version>your.version</version>
    <packaging>jar</packaging>

    <properties>
        <slf4j.version>1.7.5</slf4j.version>
        <scala.version>2.10</scala.version>
        <mysql.version>5.1.38</mysql.version>
        <logback.version>1.0.13</logback.version>

        <!-- Hadoop eco base version -->
        <hadoop.version>2.6.0</hadoop.version>
        <hbase.version>1.2.0</hbase.version>
        <zookeeper.version>3.4.5</zookeeper.version>
        <spark.version>1.6.0</spark.version>
        <hive.version>1.1.0</hive.version>

        <!-- Hadoop eco cdh version -->
        <cdh.version>5.7.0</cdh.version>
        <hadoop-cdh.version>${hadoop.version}-cdh${cdh.version}</hadoop-cdh.version>
        <hbase-cdh.version>${hbase.version}-cdh${cdh.version}</hbase-cdh.version>
        <zookeeper-cdh.version>${zookeeper.version}-cdh${cdh.version}</zookeeper-cdh.version>
        <spark-cdh.version>${spark.version}-cdh${cdh.version}</spark-cdh.version>
        <hive-cdh.version>${hive.version}-cdh${cdh.version}</hive-cdh.version>

        <!-- Hadoop eco current version -->
        <hadoop.current.version>${hadoop-cdh.version}</hadoop.current.version>
        <hbase.current.version>${hbase-cdh.version}</hbase.current.version>
        <zookeeper.current.version>${zookeeper-cdh.version}</zookeeper.current.version>
        <spark.current.version>${spark-cdh.version}</spark.current.version>
        <hive.current.version>${hive-cdh.version}</hive.current.version>
    </properties>


    <dependencies>
        <!-- zookeeper -->
        <dependency>
            <groupId>org.apache.zookeeper</groupId>
            <artifactId>zookeeper</artifactId>
            <version>${zookeeper.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>org.jboss.netty</groupId>
                    <artifactId>netty</artifactId>
                </exclusion>
            </exclusions>
            <!--<exclusions>-->
            <!--<exclusion>-->
            <!--<groupId>log4j</groupId>-->
            <!--<artifactId>log4j</artifactId>-->
            <!--</exclusion>-->
            <!--<exclusion>-->
            <!--<groupId>org.slf4j</groupId>-->
            <!--<artifactId>slf4j-log4j12</artifactId>-->
            <!--</exclusion>-->
            <!--</exclusions>-->
        </dependency>

        <!-- Hadoop & HBase -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>javax.servlet</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.eclipse.jetty.orbit</groupId>
                    <artifactId>javax.servlet</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>${hadoop.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>javax.servlet</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.eclipse.jetty.orbit</groupId>
                    <artifactId>javax.servlet</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-client</artifactId>
            <version>${hbase.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-server</artifactId>
            <version>${hbase.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>javax.servlet</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.eclipse.jetty.orbit</groupId>
                    <artifactId>javax.servlet</artifactId>
                </exclusion>
                <exclusion>
                    <artifactId>servlet-api-2.5</artifactId>
                    <groupId>org.mortbay.jetty</groupId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- Spark -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>apache-log4j-extras</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming-flume_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming-kafka_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-mllib_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-graphx_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-yarn_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-assembly_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-launcher_${scala.version}</artifactId>
            <version>${spark.current.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- hive -->
        <!--<dependency>-->
            <!--<groupId>org.spark-project.hive</groupId>-->
            <!--<artifactId>hive-cli</artifactId>-->
            <!--<version>${hive.current.version}</version>-->
            <!--<exclusions>-->
                <!--<exclusion>-->
                    <!--<groupId>org.jboss.netty</groupId>-->
                    <!--<artifactId>netty</artifactId>-->
                <!--</exclusion>-->
            <!--</exclusions>-->
        <!--</dependency>-->

        <!-- log for java-->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>log4j-over-slf4j</artifactId>
            <version>${slf4j.version}</version>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>${logback.version}</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-core</artifactId>
            <version>${logback.version}</version>
        </dependency>

        <!-- jdbc -->
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>${mysql.version}</version>
        </dependency>

    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>2.3.2</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <encoding>utf-8</encoding>
                    <showDeprecation>true</showDeprecation>
                    <showWarnings>true</showWarnings>
                </configuration>
            </plugin>
        </plugins>
    </build>

    <repositories>
        <repository>
            <id>aliyun-r</id>
            <name>aliyun maven repo remote</name>
            <url>http://maven.aliyun.com/nexus/content/repositories/central/</url>
        </repository>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
    </repositories>

</project>
```

## 要领

### 1.通过Spark访问Hive的表，导为DataFrame

这里要求你的Spark环境支持访问Hive。CDH已经帮你做好了

当然你不需要访问Hive则无视

### 2. 将DataFrame flatMap为 JavaRDD<ImmutableBytesWritable, KeyValue>

ImmutableBytesWritable其实是行键的包装类，KeyValue则是HBase里的一个单元格（Cell）

所以应该是有多个RDD记录共同构建成逻辑上HBase的一行

# 重点！！！！

生成HFile要求Key有序。开始是以为只要行键有序，即map之后，sortByKey就ok，后来HFileOutputFormat一直报后值比前值小（即未排序）。翻了很多鬼佬网站，才发现，这里的行键有序，是要求rowKey+列族+列名整体有序！！！
也就是说，你要管好列族+列名的排序。。。
（这就是折磨我一整个通宵的天坑啊啊啊啊。。。。）

demo: 现在一个表有两个列族 a 有列 a1 a2 ， b 有列 b1 b2
flatMap的时候，行键可以不管，输出的时候必须保证：

rowKey a a1 值
rowKey a a2 值
rowKey b b1 值
rowKey b b2 值

如果

rowKey a a2 值
rowKey a a1 值
rowKey b b2 值
rowKey b b1 值

则GG思密达（巨坑！）

行键再通过rdd.sortByKey() 以实现整体有序

### 3. HBase API对HBase进行操作，要以hbase用户身份进行。

这点可以通过
```java
    System.setProperty("user.name","hbase");
    System.setProperty("HADOOP_USER_NAME", "hbase");
```
来实现，也可以以hbase用户身份进行spark-submit。我调试是在windows下直接idea debug，所以就酱。

### 4. 把Hadoop/Hive/HBase的相关配置xml添加到resources目录

这样可以避免各种显式的在代码里进行配置

## 代码

根据自己需要进行修改

```java
public static void main(String args[]) throws Exception {
        String ip = "your.ip";

        String hdfsOutputDir = "/tmp/bulkload/" + System.currentTimeMillis(); // saveAsNewAPIHadoopFile 要求HDFS目录是不存在的，它自己创建

        System.setProperty("user.name","hbase");
        System.setProperty("HADOOP_USER_NAME", "hbase");
        System.setProperty("SPARK_LOCAL_HOSTNAME", ip);

        Class<?> classess[] = {
                ImmutableBytesWritable.class,
                KeyValue.class,
                Put.class,
                ImmutableBytesWritable.Comparator.class
        };

        SparkConf sparkConf = new SparkCont().setAppName("")
                .set("spark.sql.hive.metastore.jars", "builtin")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.referenceTracking", "false")
                .registerKryoClasses(classess); // 必须要用Kryo来序列化这些没实现序列化接口的类，否则你懂的

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        HiveContext hctx = new HiveContext(ctx);
        hctx.sql("use YOUR_HIVE_DATABASE");

        String tableName = "YOUR_HIVE_TABLE";

        DataFrame table = hctx.table(tableName);

            JavaPairRDD<ImmutableBytesWritable, KeyValue> hbaseData = table.limit(1000).javaRDD().flatMapToPair(new PairFlatMapFunction<Row, ImmutableBytesWritable, KeyValue>() {
            private static final long serialVersionUID = 3750223032740257913L;

            @Override
            public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Row row) throws Exception {
                List<Tuple2<ImmutableBytesWritable, KeyValue>> kvs = new ArrayList<>();

                for(xxxx) { // 你的逻辑

                    // 在这里控制 列族+列名的排序
                    kvs.add(new Tuple2<>(rk, new KeyValue(rkBytes, "a".getBytes(), "a1".getBytes(),value)));
                    kvs.add(new Tuple2<>(rk, new KeyValue(rkBytes, "a".getBytes(), "a2".getBytes(),value)));
                    kvs.add(new Tuple2<>(rk, new KeyValue(rkBytes, "b".getBytes(), "b1".getBytes(),value)));
                    kvs.add(new Tuple2<>(rk, new KeyValue(rkBytes, "b".getBytes(), "b2".getBytes(),value)));
                }

                return kvs;
            }
        }).sortByKey(); // 这里让Spark去控制行键的排序


        Configuration conf = HBaseConfiguration.create();
        conf.set(TableOutputFormat.OUTPUT_TABLE,"your.hbase.table");
        Job job = Job.getInstance();
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        Connection conn = ConnectionFactory.createConnection(conf);
        TableName hbTableName = TableName.valueOf("your.hbase.namespace".getBytes(), "your.hbase.table".getBytes());
        HRegionLocator regionLocator = new  HRegionLocator(hbTableName, (ClusterConnection) conn);
        Table realTable = conn.getTable(hbTableName);

        HFileOutputFormat2.configureIncrementalLoad(job,realTable,regionLocator);

        hbaseData.saveAsNewAPIHadoopFile(hdfsOutputDir,ImmutableBytesWritable.class,KeyValue.class,HFileOutputFormat2.class,job.getConfiguration());

        // bulk load start
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        Admin admin = conn.getAdmin();
        loader.doBulkLoad(new Path(hdfsOutputDir),admin,realTable,regionLocator);

        ctx.stop();
   }
```

# have fun