### Spark已更新至2.x，DataFrame归DataSet管了，因此API也相应统一。本文不再适用2.0.0及以上版本。

--------------------


DataFrame原生支持直接输出到JDBC，但如果目标表有自增字段（比如id），那么DataFrame就不能直接进行写入了。因为DataFrame.write().jdbc()要求DataFrame的schema与目标表的表结构必须完全一致（甚至字段顺序都要一致），否则会抛异常，当然，如果你SaveMode选择了Overwrite，那么Spark删除你原有的表，然后根据DataFrame的Schema生成一个。。。。字段类型会非常非常奇葩。。。。
于是我们只能通过DataFrame.collect()，把整个DataFrame转成List<Row>到Driver上，然后通过原生的JDBC方法进行写入。但是如果DataFrame体积过于庞大，很容易导致Driver OOM（特别是我们一般不会给Driver配置过高的内存）。这个问题真的很让人纠结。
翻看Spark的JDBC源码，发现实际上是通过foreachPartition方法，在DataFrame每一个分区中，对每个Row的数据进行JDBC插入，那么为什么我们就不能直接用呢？

Spark JdbcUtils.scala部分源码：

      def saveTable(df: DataFrame,url: String,table: String,properties: Properties = new Properties()) {
        val dialect = JdbcDialects.get(url)
        val nullTypes: Array[Int] = df.schema.fields.map { field =>
          dialect.getJDBCType(field.dataType).map(_.jdbcNullType).getOrElse(
            field.dataType match {
              case IntegerType => java.sql.Types.INTEGER
              case LongType => java.sql.Types.BIGINT
              case DoubleType => java.sql.Types.DOUBLE
              case FloatType => java.sql.Types.REAL
              case ShortType => java.sql.Types.INTEGER
              case ByteType => java.sql.Types.INTEGER
              case BooleanType => java.sql.Types.BIT
              case StringType => java.sql.Types.CLOB
              case BinaryType => java.sql.Types.BLOB
              case TimestampType => java.sql.Types.TIMESTAMP
              case DateType => java.sql.Types.DATE
              case t: DecimalType => java.sql.Types.DECIMAL
              case _ => throw new IllegalArgumentException(
                s"Can't translate null value for field $field")
            })
        }

        val rddSchema = df.schema
        val driver: String = DriverRegistry.getDriverClassName(url)
        val getConnection: () => Connection = JDBCRDD.getConnector(driver, url, properties)
        // ****************** here ******************
        df.foreachPartition { iterator =>
          savePartition(getConnection, table, iterator, rddSchema, nullTypes)
        }
      }

嗯。。。既然Scala能实现，那么作为他的爸爸，Java也应该能玩！
我们看看foreachPartition的方法原型：

    def foreachPartition(f: Iterator[Row] => Unit)

又是函数式语言最爱的匿名函数。。。非常讨厌写lambda，所以我们还是实现个匿名类吧。要实现的抽象类为：
scala.runtime.AbstractFunction1<Iterator<Row>,BoxedUnit> 两个模板参数，第一个很直观，就是Row的迭代器,作为函数的参数。第二个BoxedUnit，是函数的返回值。不熟悉Scala的可能会很困惑，其实这就是Scala的void。由于Scala函数式编程的特性，代码块的末尾必须返回点什么，于是他们就搞出了个unit来代替本应什么都没有的void（解释得可能不是很准确，我是这么理解的）。对于Java而言，我们可以直接使用BoxedUnit.UNIT，来得到这个“什么都没有”的东西。
来玩耍一下吧！

    df.foreachPartition(new AbstractFunction1<Iterator<Row>, BoxedUnit>() {
        @Override
        public BoxedUnit apply(Iterator<Row> it) {
            while (it.hasNext()){
                System.out.println(it.next().toString());
            }
            return BoxedUnit.UNIT;
        }
    });

嗯，maven complete一下，spark-submit看看~
好勒~抛异常了
org.apache.spark.SparkException: Task not serializable
Task不能被序列化
嗯哼，想想之前实现UDF的时候，UDF1/2/3/4...各接口，都extends Serializable，也就是说，在Spark运行期间，Driver会把UDF接口实现类序列化，并在Executor中反序列化，执行call方法。。。这就不难理解了，我们foreachPartition丢进去的类，也应该implements Serializable。这样，我们就得自己搞一个继承AbstractFunction1<Iterator<Row>, BoxedUnit>，又实现Serializable的抽象类，给我们这些匿名类去实现！

    import org.apache.spark.sql.Row;
    import scala.runtime.AbstractFunction1;
    import scala.runtime.BoxedUnit;

    import java.io.Serializable;

    public abstract class JavaForeachPartitionFunc extends AbstractFunction1<Iterator<Row>, BoxedUnit> implements Serializable {
    }
可是每次都要return BoxedUnit.UNIT 搞得太别扭了，没一点Java的风格。

    import org.apache.spark.sql.Row;
    import scala.collection.Iterator;
    import scala.runtime.AbstractFunction1;
    import scala.runtime.BoxedUnit;

    import java.io.Serializable;

    public abstract class JavaForeachPartitionFunc extends AbstractFunction1<Iterator<Row>, BoxedUnit> implements Serializable {
        @Override
        public BoxedUnit apply(Iterator<Row> it) {
            call(it);
            return BoxedUnit.UNIT;
        }

        public abstract void call(Iterator<Row> it);
    }

于是我们可以直接Override call方法，就可以用满满Java Style的代码去玩耍了！

    df.foreachPartition(new JavaForeachPartitionFunc() {
        @Override
        public void call(Iterator<Row> it) {
            while (it.hasNext()){
                System.out.println(it.next().toString());
            }
        }
    });

注意！我们实现的匿名类的方法，实际上是在executor上执行的，所以println是输出到executor机器的stdout上。这个我们可以通过Spark的web ui，点击具体Application的Executor页面去查看（调试用的虚拟机集群，手扶拖拉机一样的配置，别吐槽了~）

![](https://github.com/LinkSe7en/blog-draft/blob/master/resources/testing-spark-cluster.jpg)

至于foreach方法同理。只不过把Iterator<Row> 换成 Row。具体怎么搞，慢慢玩吧~~~
have fun~

