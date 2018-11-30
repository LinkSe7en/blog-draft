Apache Zeppelin是ASF的一个孵化项目，实现了基于web的在线代码编辑与数据可视化。有点类似Spark-Shell的REPL。其结果可以直接用图表来展示，解决了前端白痴的苦逼。

[Zeppelin官网][1]

我们可以直接下载 zeppelin-0.5.6-incubating-bin-all.tgz ，然后部署到服务器上，个人感觉部署到Spark Master节点上会更好。修改好配置文件，然后输入命令 $ZEPPELIN_HOME/bin/zeppelin-daemon.sh start启动Zeppelin，就可以在浏览器上操作Zeppelin了。
当然你也可以下载源码到本地通过maven编译，不过老是有些依赖下载不下来，导致编译失败，最后还是放弃了。。。

配置项：

Zeppelin环境变量：
```bash
    cp $ZEPPELIN_HOME/conf/zeppelin-env.sh.template $ZEPPELIN_HOME/conf/zeppelin-env.sh
    vi $ZEPPELIN_HOME/conf/zeppelin-env.sh
```

在文件末尾增加如下配置项：

```bash
    export JAVA_HOME= # JDK目录
    export MASTER= # spark://master:port
    export ZEPPELIN_JAVA_OPTS= #启动Zeppelin的参数。
    export ZEPPELIN_NOTEBOOK_DIR="$ZEPPELIN_HOME/notebook" # 保存Zeppelin notbook的目录，notebook可以理解为spark的application

    export SPARK_HOME= # Spark目录
    export SPARK_SUBMIT_OPTIONS= #启动spark application的参数，同spark-submit
    export HADOOP_CONF_DIR="$HADOOP_PERFIX/etc/hadoop" # hadoop配置文件的目录
```

zeppelin-site.xml

```bash
    cp $ZEPPELIN_HOME/conf/zeppelin-site.xml.template $ZEPPELIN_HOME/conf/zeppelin-site.xml
    vi $ZEPPELIN_HOME/conf/zeppelin-site.xml
```

修改如下

```xml
    <property>
      <name>zeppelin.server.addr</name>
      <value>192.168.1.123</value> <!-- 建议修改为本地的内网ip -->
      <description>Server address</description>
    </property>

    <property>
      <name>zeppelin.server.port</name>
      <value>28080</value> <!-- 8080容易冲突，建议修改，我是28080 -->
      <description>Server port.</description>
    </property>
```

 通过如下命令启动Zeppelin

```bash
    $ZEPPELIN_HOME/bin/zeppelin-daemon.sh start
```

未完待续。。。

  [1]: http://zeppelin.apache.org/