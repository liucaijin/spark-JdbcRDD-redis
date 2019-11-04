# spark-JdbcRDD-redis
scala maven环境
重写JdbcRDD类,更加方便的sql查询方式

问题1：导入到eclipse中，是由于eclipse默认加载src/main/java Source下的包，所有src/main/scala下的包需要手动添加到工程路径下
问题2：需要手动选择与spark项目匹配的scala版本，如spark1.6匹配的项目版本为2.10.6
<dependency>
	<groupId>com.microsoft.sqlserver</groupId>
	<artifactId>sqljdbc</artifactId>
	<version>4</version>
	</dependency>

<dependency>
	<groupId>spark</groupId>
	<artifactId>sparkredis </artifactId>
	<version>1.0</version>
	</dependency>
以上两个依赖的jar包需要我们自己收到添加到本地maven仓库（微软一贯这样）,sparkredis是我从https://github.com/RedisLabs/spark-redis
编译下来的jar包，提供了直接存储RDD数据结构到redis的方法。111
