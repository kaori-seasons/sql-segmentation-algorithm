#### SQL依赖拆分算法

##### 需求来源:
```
由于离线调度任务采用dolphinscheduler划分依赖 ，组件比较重。且不太适合流式任务。所以需要设计一套sql依赖拆分算法.帮助业务方拆解依赖
目的是为了保证原来的旧任务在使用原本的sql(不使用多级视图，不使用非常规查询)的情况下做子任务的切分适配 在保证不破坏计算语义的前提下完成切分
如果有需要业务方改造sql的case,那么case by case按需分析或改造
```

##### 拆分算法设计理念:
```
无论是数仓侧还是平台侧编写sql的习惯都是基于顺序执行 
目前的实现方式是基于二叉搜索树+单向链表 实现多条单源最短路的sql子图切分 所谓的单源最短路 就是判断从起点到终点 判断中间的节点是否可以被访问 如果可以被访问 则加入路径比较边权 但是在sql切分里面 原本就不存在边权的概念 只是一个单纯的DAG有向无环图.  且是一个起点终点都是Insert into语句的成环图
分为两个阶段 解析阶段和重建阶段
   解析阶段从起点到终点的过程中 会依次收集所需要的表上下文 建立一张图 然后顺序遍历达到insert into语句后 标记为Final节点 装载上下文结束遍历，多条insert into语句以此类推
   在重建阶段 将以insert into语句作为根节点 看做一棵树 将各个表名作为叶子节点 从叶子节点回溯到根
多条insert into语句的树/图 相互隔离
```

##### 注意事项
```
1.在方言切换的情况下 所有建表都要使用 CREATE TABLE IF NOT EXISTS语法 否则切分将会出现混乱(已支持，可选择是否加上)
2.不允许使用多级视图 ，比如从view1当中创建view2 会造成上下文切分失败
3.insert into table1 from table2, insert into table1 from table2 ，出现insert into 同一个目标表的时候， 一定需要使用 CREATE TABLE IF NOT EXISTS语法, 否则会造成切分混乱(已支持，可选择是否加上)
4.已经建好的表，无论是物理表还是临时表 , 最好都加上CREATE TABLE IF NOT EXISTS,给出表定义(已支持，可选择是否加上)
5.方言切换的dialect一定要保证使用一类方言创建的上下文结束后，才能使用另一类方言创建表(已支持，建议最好按照规范来）
6.如果要用视图 只支持单一视图，并且只能使用hive视图。 create view view1 as select * from table1可以 但是create view view2 as select * from view1坚决不行
7.语法树校验(未支持)
8.支持使用子查询，order by .但explain语法不支持
9.如果要建hive表必须要加上
set table.sql-dialect = hive;进行方言切换
default数据库的表 直接 CREATE TABLE xxx即可， 所有语句(select , create, insert不允许出现default.xxx 会导致语法关键字冲突)
非hive数据库的表 需要加上database前缀，即 dmp.yyyy
10.执行所有insert into 或者insert override语句之前 一定要加上
set table.sql-dialect = default;切换回默认方言,hive语法会执行错误
```


##### 演示用例



case1:   原始SQL1(union all + 嵌套子查询 + 多条insert语句)
```
CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog; CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'; CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.uniondev_202_343; CREATE TABLE tag.uniondev_202_343(tagkey String, newvalue String); INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 ); set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_202_343; CREATE TABLE tag.tagdev_es_sink_202_343 ( id String, my_tj_json_field String, PRIMARY KEY (id) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_130' ); INSERT INTO tag.tagdev_es_sink_202_343 SELECT tagkey, tagToJson(tagkey, '@@', tagTotal, '') AS my_tj_json_field FROM ( select tagkey, collectSet( newvalue || '@@' || 'v00006' || '@@' || 'bq_zh(yqgpz)' || '@@' || '' ) AS tagTotal FROM tag.uniondev_202_343 GROUP BY tagkey ) AS t; set table.sql-dialect = hive; CREATE TABLE tag.tagdev_result_202_343_1686129546001( tagKey String, tagValue String, tagVersion String ); INSERT INTO tag.tagdev_result_202_343_1686129546001 SELECT tagkey AS tagKey, newvalue AS tagValue, 'v00006' AS tagVersion FROM tag.uniondev_202_343; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_202; CREATE TABLE tag.tagdev_result_202( tagKey String, tagValue String, tagVersion String ); INSERT INTO tag.tagdev_result_202 SELECT tagKey, tagValue, tagVersion FROM tag.tagdev_result_202_343_1686129546001;
```


解析结果

sql血缘解析统计图
```
{
  "tag.tagdev_result_198": {
    "connector": "redefine",
    "tableName": "tag.tagdev_result_198",
    "next": [
      {
        "connector": "insert",
        "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
        "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      },
      {
        "connector": "insert",
        "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
        "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      },
      {
        "connector": "insert",
        "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
        "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      }
    ],
    "drop": false
  },
  "tag.tagdev_result_199": {
    "connector": "redefine",
    "tableName": "tag.tagdev_result_199",
    "next": [
      {
        "connector": "insert",
        "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
        "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      },
      {
        "connector": "insert",
        "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
        "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      },
      {
        "connector": "insert",
        "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
        "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      }
    ],
    "drop": false
  },
  "tag.uniondev_202_343": {
    "connector": "hive",
    "tableName": "tag.uniondev_202_343",
    "next": [
      {
        "connector": "insert",
        "tableName": "tag.uniondev_202_343,tag.tagdev_es_sink_202_343",
        "statement": " INSERT INTO tag.tagdev_es_sink_202_343 SELECT tagkey, tagToJson(tagkey, '@@', tagTotal, '') AS my_tj_json_field FROM ( select tagkey, collectSet( newvalue || '@@' || 'v00006' || '@@' || 'bq_zh(yqgpz)' || '@@' || '' ) AS tagTotal FROM tag.uniondev_202_343 GROUP BY tagkey ) AS t",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      }
    ],
    "statement": " CREATE TABLE tag.uniondev_202_343(tagkey String, newvalue String)",
    "drop": true
  },
  "tag.tagdev_result_197": {
    "connector": "redefine",
    "tableName": "tag.tagdev_result_197",
    "next": [
      {
        "connector": "insert",
        "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
        "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      },
      {
        "connector": "insert",
        "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
        "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      },
      {
        "connector": "insert",
        "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
        "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      }
    ],
    "drop": false
  },
  "hiveCatalog": {
    "connector": "hive",
    "tableName": "catalog",
    "statement": "CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' )",
    "drop": false
  },
  "Final#1": {
    "connector": "insert",
    "tableName": "tag.tagdev_result_197, tag.tagdev_result_199, tag.tagdev_result_198,tag.uniondev_202_343",
    "statement": " INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 )",
    "drop": false,
    "functionList": {
      "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
      "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
    }
  },
  "Final#2": {
    "connector": "insert",
    "tableName": "tag.uniondev_202_343,tag.tagdev_es_sink_202_343",
    "statement": " INSERT INTO tag.tagdev_es_sink_202_343 SELECT tagkey, tagToJson(tagkey, '@@', tagTotal, '') AS my_tj_json_field FROM ( select tagkey, collectSet( newvalue || '@@' || 'v00006' || '@@' || 'bq_zh(yqgpz)' || '@@' || '' ) AS tagTotal FROM tag.uniondev_202_343 GROUP BY tagkey ) AS t",
    "drop": false,
    "functionList": {
      "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
      "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
    }
  },
  "Final#3": {
    "connector": "insert",
    "tableName": ",tag.tagdev_result_202_343_1686129546001",
    "statement": " INSERT INTO tag.tagdev_result_202_343_1686129546001 SELECT tagkey AS tagKey, newvalue AS tagValue, 'v00006' AS tagVersion FROM tag.uniondev_202_343",
    "drop": false,
    "functionList": {
      "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
      "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
    }
  },
  "tag.tagdev_es_sink_202_343": {
    "connector": "elasticsearch-7",
    "tableName": "tag.tagdev_es_sink_202_343",
    "next": [
      {
        "connector": "insert",
        "tableName": "tag.uniondev_202_343,tag.tagdev_es_sink_202_343",
        "statement": " INSERT INTO tag.tagdev_es_sink_202_343 SELECT tagkey, tagToJson(tagkey, '@@', tagTotal, '') AS my_tj_json_field FROM ( select tagkey, collectSet( newvalue || '@@' || 'v00006' || '@@' || 'bq_zh(yqgpz)' || '@@' || '' ) AS tagTotal FROM tag.uniondev_202_343 GROUP BY tagkey ) AS t",
        "drop": false,
        "functionList": {
          "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
          "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
        }
      }
    ],
    "statement": " CREATE TABLE tag.tagdev_es_sink_202_343 ( id String, my_tj_json_field String, PRIMARY KEY (id) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_130' )",
    "drop": true
  },
  "tag.tagdev_result_202_343_1686129546001": {
    "connector": "hive",
    "tableName": "tag.tagdev_result_202_343_1686129546001",
    "next": [],
    "statement": " CREATE TABLE tag.tagdev_result_202_343_1686129546001( tagKey String, tagValue String, tagVersion String )",
    "drop": false
  },
  "Final#4": {
    "connector": "insert",
    "tableName": ",tag.tagdev_result_202",
    "statement": " INSERT INTO tag.tagdev_result_202 SELECT tagKey, tagValue, tagVersion FROM tag.tagdev_result_202_343_1686129546001",
    "drop": false,
    "functionList": {
      "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
      "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
    }
  },
  "tag.tagdev_result_202": {
    "connector": "hive",
    "tableName": "tag.tagdev_result_202",
    "next": [],
    "statement": " CREATE TABLE tag.tagdev_result_202( tagKey String, tagValue String, tagVersion String )",
    "drop": true
  }
}
{
  "tagToJson": " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'",
  "collectSet": " CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'"
}
```
拆分结果

```
 [
  "CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' );set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.uniondev_202_343 CREATE TABLE tag.uniondev_202_343(tagkey String, newvalue String); INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 );",
  " CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'; CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' );set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.uniondev_202_343 CREATE TABLE tag.uniondev_202_343(tagkey String, newvalue String);CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' );set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_202_343 CREATE TABLE tag.tagdev_es_sink_202_343 ( id String, my_tj_json_field String, PRIMARY KEY (id) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_130' ); INSERT INTO tag.tagdev_es_sink_202_343 SELECT tagkey, tagToJson(tagkey, '@@', tagTotal, '') AS my_tj_json_field FROM ( select tagkey, collectSet( newvalue || '@@' || 'v00006' || '@@' || 'bq_zh(yqgpz)' || '@@' || '' ) AS tagTotal FROM tag.uniondev_202_343 GROUP BY tagkey ) AS t;",
  "CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' );set table.sql-dialect = hive;  CREATE TABLE tag.tagdev_result_202_343_1686129546001( tagKey String, tagValue String, tagVersion String ); INSERT INTO tag.tagdev_result_202_343_1686129546001 SELECT tagkey AS tagKey, newvalue AS tagValue, 'v00006' AS tagVersion FROM tag.uniondev_202_343;",
  "CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' );set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_202 CREATE TABLE tag.tagdev_result_202( tagKey String, tagValue String, tagVersion String ); INSERT INTO tag.tagdev_result_202 SELECT tagKey, tagValue, tagVersion FROM tag.tagdev_result_202_343_1686129546001;"
]
```
case2： 原始SQL（子查询+单条insert）
```
CREATE FUNCTION imtm as 'com.meiya.whale.flink.clean.format.udf.UdfImtm'; CREATE CATALOG myhive WITH ( 'type' = 'hive', 'default-database' = 'test', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' ); USE CATALOG myhive;  CREATE TABLE if not exists kafka_sink ( id STRING, name STRING, age INT, addr STRING, createtime TIMESTAMP ) WITH ( 'connector' = 'kafka', 'topic' = 'test_user_from_hive', 'properties.bootstrap.servers' = '10.30.50.249:21007,10.30.50.250:21007,10.30.50.251:21007', 'properties.group.id' = 'group_topic01', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'properties.security.protocol' = 'SASL_PLAINTEXT', 'properties.sasl.kerberos.service.name' = 'kafka', 'properties.kerberos.domain.name' = 'hadoop.hadoop.com' ); create table test_user_id32 ( id STRING, name STRING, age INT, addr STRING ) WITH ( 'connector' = 'kafka', 'topic' = 'test_user_from_hive', 'properties.bootstrap.servers' = '10.30.50.249:21007,10.30.50.250:21007,10.30.50.251:21007', 'properties.group.id' = 'group_topic01', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'properties.security.protocol' = 'SASL_PLAINTEXT', 'properties.sasl.kerberos.service.name' = 'kafka', 'properties.kerberos.domain.name' = 'hadoop.hadoop.com' ); insert into kafka_sink select id, name, age, addr, imtm() as createtime from (select * from test_user_id32 );
```

SQL血缘解析
```
{
  "kafka_sink": {
    "connector": "kafka",
    "tableName": "kafka_sink",
    "next": [],
    "statement": "  CREATE TABLE if not exists kafka_sink ( id STRING, name STRING, age INT, addr STRING, createtime TIMESTAMP ) WITH ( 'connector' = 'kafka', 'topic' = 'test_user_from_hive', 'properties.bootstrap.servers' = '10.30.50.249:21007,10.30.50.250:21007,10.30.50.251:21007', 'properties.group.id' = 'group_topic01', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'properties.security.protocol' = 'SASL_PLAINTEXT', 'properties.sasl.kerberos.service.name' = 'kafka', 'properties.kerberos.domain.name' = 'hadoop.hadoop.com' )",
    "drop": false
  },
  "myhive": {
    "connector": "hive",
    "tableName": "catalog",
    "statement": " CREATE CATALOG myhive WITH ( 'type' = 'hive', 'default-database' = 'test', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' )",
    "drop": false
  },
  "test_user_id32": {
    "connector": "kafka",
    "tableName": "test_user_id32",
    "statement": " create table test_user_id32 ( id STRING, name STRING, age INT, addr STRING ) WITH ( 'connector' = 'kafka', 'topic' = 'test_user_from_hive', 'properties.bootstrap.servers' = '10.30.50.249:21007,10.30.50.250:21007,10.30.50.251:21007', 'properties.group.id' = 'group_topic01', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'properties.security.protocol' = 'SASL_PLAINTEXT', 'properties.sasl.kerberos.service.name' = 'kafka', 'properties.kerberos.domain.name' = 'hadoop.hadoop.com' )",
    "drop": false
  },
  "Final#1": {
    "connector": "insert",
    "tableName": ",kafka_sink",
    "statement": " insert into kafka_sink select id, name, age, addr, imtm() as createtime from (select * from test_user_id32 )",
    "drop": false,
    "functionList": {
      "imtm": "CREATE FUNCTION imtm as 'com.meiya.whale.flink.clean.format.udf.UdfImtm'"
    }
  }
}
{
  "imtm": "CREATE FUNCTION imtm as 'com.meiya.whale.flink.clean.format.udf.UdfImtm'"
}
```
拆分结果

```
[
  "CREATE FUNCTION imtm as 'com.meiya.whale.flink.clean.format.udf.UdfImtm'; CREATE CATALOG myhive WITH ( 'type' = 'hive', 'default-database' = 'test', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' );set table.sql-dialect = default;   CREATE TABLE if not exists kafka_sink ( id STRING, name STRING, age INT, addr STRING, createtime TIMESTAMP ) WITH ( 'connector' = 'kafka', 'topic' = 'test_user_from_hive', 'properties.bootstrap.servers' = '10.30.50.249:21007,10.30.50.250:21007,10.30.50.251:21007', 'properties.group.id' = 'group_topic01', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'properties.security.protocol' = 'SASL_PLAINTEXT', 'properties.sasl.kerberos.service.name' = 'kafka', 'properties.kerberos.domain.name' = 'hadoop.hadoop.com' ); insert into kafka_sink select id, name, age, addr, imtm() as createtime from (select * from test_user_id32 );"
]
```
case3: 视图+函数+ 多条insert into + 切换方言

```
CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', " +
            "'hadoop-conf-dir'='/home/lakehouse/flink/conf' );" +

            " USE CATALOG hiveCatalog;" +
            "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';" +
            "CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';" +
            "DROP VIEW IF EXISTS stream_mod_x7_col_we; " +
            "CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive;" +
            "DROP TABLE IF EXISTS tag.tagdev_es_sink_220_364;" +
            "CREATE TABLE tag.tagdev_es_sink_220_364 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );" +
            "INSERT INTO tag.tagdev_es_sink_220_364 SELECT kk,tagToJson(kk,'@@',tagTotal,'') AS my_tj_json_field FROM" +
            " (select kk, collectSet(vv || '@@' || 'v00002' || '@@' || 'bq_test0608' || '@@' || '' || '@@' || 'nm:test0608'|| '@@' || 'id:220'|| '@@' || 'tagType:基础标签') AS " +
            " tagTotal FROM stream_mod_x7_col_we GROUP BY kk) AS t; set table.sql-dialect = hive;" +
            "CREATE TABLE tag.tagdev_result_220_364_1687331953048(tagKey String, tagValue String, tagVersion String);" +
            " set table.sql-dialect = default; " +
            "INSERT INTO tag.tagdev_result_220_364_1687331953048 SELECT kk AS tagKey, vv AS tagValue, 'v00001' AS tagVersion FROM stream_mod_x7_col_we; " +
            "set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_220; CREATE TABLE tag.tagdev_result_220(tagKey String, tagValue String, tagVersion String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_220 SELECT tagKey, tagValue, tagVersion FROM tag.tagdev_result_220_364_1687331953048;
```
拆分结果
```
[
  "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP VIEW IF EXISTS stream_mod_x7_col_we ; CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive;set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_220_364 ;CREATE TABLE tag.tagdev_es_sink_220_364 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );INSERT INTO tag.tagdev_es_sink_220_364 SELECT kk,tagToJson(kk,'@@',tagTotal,'') AS my_tj_json_field FROM (select kk, collectSet(vv || '@@' || 'v00002' || '@@' || 'bq_test0608' || '@@' || '' || '@@' || 'nm:test0608'|| '@@' || 'id:220'|| '@@' || 'tagType:基础标签') AS  tagTotal FROM stream_mod_x7_col_we GROUP BY kk) AS t;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive;  CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive;CREATE TABLE tag.tagdev_result_220_364_1687331953048(tagKey String, tagValue String, tagVersion String); INSERT INTO tag.tagdev_result_220_364_1687331953048 SELECT kk AS tagKey, vv AS tagValue, 'v00001' AS tagVersion FROM stream_mod_x7_col_we;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; CREATE TABLE tag.tagdev_result_220_364_1687331953048(tagKey String, tagValue String, tagVersion String);DROP TABLE IF EXISTS tag.tagdev_result_220 ; CREATE TABLE tag.tagdev_result_220(tagKey String, tagValue String, tagVersion String); INSERT INTO tag.tagdev_result_220 SELECT tagKey, tagValue, tagVersion FROM tag.tagdev_result_220_364_1687331953048;"
]
```
case4:
```
CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); " +
            "USE CATALOG hiveCatalog;" +
            "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';" +
            "CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; " +
            "set table.sql-dialect = hive;" +
            " CREATE TABLE tag.tagdev_result_220_20230629110648(tag_key String, tag_value String, tag_version String); " +
            " set table.sql-dialect = default;" +
            " set table.sql-dialect = hive;DROP VIEW IF EXISTS stream_mod_x7_col_we; " +
            "CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive;" +
            "set table.sql-dialect = default;" +
            " INSERT INTO tag.tagdev_result_220_20230629110648 SELECT kk AS tagKey, vv AS tagValue, 'v00003' AS tagVersion FROM stream_mod_x7_col_we;" +
            "set table.sql-dialect = hive;" +
            "DROP VIEW IF EXISTS stream_mod_x7_col_we999;" +
            "CREATE VIEW stream_mod_x7_col_we999 AS SELECT xzz AS kk, zzsy AS vv FROM syrk_kfk_hive;" +
            "set table.sql-dialect = default; " +
            "INSERT INTO tag.tagdev_result_220_20230629110648 SELECT kk AS tagKey, vv AS tagValue, 'v00003' AS tagVersion FROM stream_mod_x7_col_we999; " +
            "set table.sql-dialect = hive; " +
            "DROP TABLE IF EXISTS tag.tagdev_result_220; " +
            "CREATE TABLE tag.tagdev_result_220(tag_key String, tag_value String, tag_version String); " +
            "set table.sql-dialect = default; " +
            "INSERT INTO tag.tagdev_result_220 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_220_20230629110648;" +
            "DROP TABLE IF EXISTS tag.tagdev_es_sink_220;" +
            "CREATE TABLE tag.tagdev_es_sink_220 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );" +
            "INSERT INTO tag.tagdev_es_sink_220 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00003' || '@@' || 'bq_test0608' || '@@' || '' || '@@' || 'nm:test0608'|| '@@' || 'id:220'|| '@@' || 'tagType:基础标签') AS  tagTotal FROM tag.tagdev_result_220 GROUP BY tag_key) AS t;
```
拆分结果
```
[
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP VIEW IF EXISTS stream_mod_x7_col_we ; CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive; CREATE TABLE tag.tagdev_result_220_20230629110648(tag_key String, tag_value String, tag_version String); INSERT INTO tag.tagdev_result_220_20230629110648 SELECT kk AS tagKey, vv AS tagValue, 'v00003' AS tagVersion FROM stream_mod_x7_col_we;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP VIEW IF EXISTS stream_mod_x7_col_we999 ;CREATE VIEW stream_mod_x7_col_we999 AS SELECT xzz AS kk, zzsy AS vv FROM syrk_kfk_hive; INSERT INTO tag.tagdev_result_220_20230629110648 SELECT kk AS tagKey, vv AS tagValue, 'v00003' AS tagVersion FROM stream_mod_x7_col_we999;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_220 ; CREATE TABLE tag.tagdev_result_220(tag_key String, tag_value String, tag_version String); INSERT INTO tag.tagdev_result_220 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_220_20230629110648;",
  "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_220 ;CREATE TABLE tag.tagdev_es_sink_220 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );INSERT INTO tag.tagdev_es_sink_220 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00003' || '@@' || 'bq_test0608' || '@@' || '' || '@@' || 'nm:test0608'|| '@@' || 'id:220'|| '@@' || 'tagType:鍩虹鏍囩') AS  tagTotal FROM tag.tagdev_result_220 GROUP BY tag_key) AS t;"
]
```


case5: 多次切换方言，并使用单一视图做关联
```
CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;\n" +
            "create function if not exists test as 'com.meiya.whale.flink.udaf.TestAggregateFunction';\n" +
            "DROP TABLE IF EXISTS stream_mod_y9_col_130;\n" +
            "CREATE TABLE IF NOT EXISTS stream_mod_y9_col_130(column_xm String, column_zjhm String, column_xb String, proctime as procTime()) WITH ('connector' = 'kafka','properties.bootstrap.servers' = '10.30.49.17:6667','topic' = 'topic0629','format' = 'json');\n" +
            "DROP TABLE IF EXISTS stream_mod_y9_col_133;\n" +
            "CREATE TABLE IF NOT EXISTS stream_mod_y9_col_133(column_xm String, column_zjhm String, column_xb String) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://10.30.49.14:3306/dmp','table-name' = 'tb_test_0629','username' = 'pico','password' = 'pico-nf-8100');\n" +
            "set table.sql-dialect = hive;\n" +
            "DROP VIEW IF EXISTS stream_mod_y9_col_134;  \n" +
            "CREATE VIEW stream_mod_y9_col_134 AS SELECT col_130.column_xm AS column_xm, col_130.column_zjhm AS column_zjhm, col_130.column_xb AS column_xb, col_130.proctime AS proctime FROM stream_mod_y9_col_130 AS col_130 left join stream_mod_y9_col_133 AS col_133 ON (col_130.column_xm = col_133.column_xm) AND (col_130.column_xm IS NOT NULL) AND (col_133.column_xm IS NOT NULL);\n" +
            "DROP VIEW IF EXISTS stream_mod_y9_col_135;  \n" +
            "CREATE VIEW stream_mod_y9_col_135 AS SELECT col_134.column_xm AS column_xm, col_134.column_zjhm AS column_zjhm, col_134.column_xb AS column_xb, col_134.proctime AS proctime FROM stream_mod_y9_col_134 AS col_134 WHERE ((col_134.column_xm is not null AND TRIM(col_134.column_xm) <> ''));\n" +
            "set table.sql-dialect = default;\n" +
            "DROP TABLE IF EXISTS stream_mod_y9_col_118;\n" +
            "CREATE TABLE IF NOT EXISTS stream_mod_y9_col_118(xh String, text String) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://10.30.49.14:3306/dmp','table-name' = 'zhuzl_0616','username' = 'pico','password' = 'pico-nf-8100');\n" +
            "INSERT INTO stream_mod_y9_col_118 SELECT column_xm,column_zjhm FROM stream_mod_y9_col_135 AS col_135;
```
拆分结果
```
[
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive;  CREATE TABLE tag.tagdev_result_243_20230630094623(tag_key String, tag_value String, tag_version String);set table.sql-dialect = default;   INSERT INTO tag.tagdev_result_243_20230630094623 SELECT tagKey AS tagKey, tagvalue  AS tagValue, 'v00001' AS tagVersion FROM tagdev_result_202;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_243 ; CREATE TABLE tag.tagdev_result_243(tag_key String, tag_value String, tag_version String); INSERT INTO tag.tagdev_result_243 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_243_20230630094623;",
  "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_243 ;CREATE TABLE tag.tagdev_es_sink_243 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_132' );INSERT INTO tag.tagdev_es_sink_243 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00001' || '@@' || 'bq_sg_1' || '@@' || '' || '@@' || 'nm:韬珮'|| '@@' || 'id:243') AS  tagTotal FROM tag.tagdev_result_243 GROUP BY tag_key) AS t;"
]
```

case6: hive方言切换为default方言一次+insert into多条sql
```
CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql-dialect = hive; CREATE TABLE tag.tagdev_result_243_20230630094623(tag_key String, tag_value String, tag_version String);  set table.sql-dialect = default;  INSERT INTO tag.tagdev_result_243_20230630094623 SELECT tagKey AS tagKey, tagvalue  AS tagValue, 'v00001' AS tagVersion FROM tagdev_result_202; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_243; CREATE TABLE tag.tagdev_result_243(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_243 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_243_20230630094623;DROP TABLE IF EXISTS tag.tagdev_es_sink_243;CREATE TABLE tag.tagdev_es_sink_243 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_132' );INSERT INTO tag.tagdev_es_sink_243 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00001' || '@@' || 'bq_sg_1' || '@@' || '' || '@@' || 'nm:身高'|| '@@' || 'id:243') AS  tagTotal FROM tag.tagdev_result_243 GROUP BY tag_key) AS t;
```

拆分结果
```
[
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive;  CREATE TABLE tag.tagdev_result_243_20230630094623(tag_key String, tag_value String, tag_version String);set table.sql-dialect = default;   INSERT INTO tag.tagdev_result_243_20230630094623 SELECT tagKey AS tagKey, tagvalue  AS tagValue, 'v00001' AS tagVersion FROM tagdev_result_202;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_243 ; CREATE TABLE tag.tagdev_result_243(tag_key String, tag_value String, tag_version String); INSERT INTO tag.tagdev_result_243 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_243_20230630094623;",
  "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_243 ;CREATE TABLE tag.tagdev_es_sink_243 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_132' );INSERT INTO tag.tagdev_es_sink_243 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00001' || '@@' || 'bq_sg_1' || '@@' || '' || '@@' || 'nm:韬珮'|| '@@' || 'id:243') AS  tagTotal FROM tag.tagdev_result_243 GROUP BY tag_key) AS t;"
]
```

case7: hive方言 + 子查询 + 多表join
```
CREATE CATALOG hiveCatalog WITH('type'='hive','default-database'='default','hive-conf-dir' = '/home/lakehouse/flink/conf','hadoop-conf-dir' = '/home/lakehouse/flink/conf');\n" +
            "  USE CATALOG hiveCatalog;\n" +
            "  DROP TABLE IF EXISTS stream_mod_yu_col_12i;\n" +
            "  CREATE TABLE IF NOT EXISTS stream_mod_yu_col_12i(sex String) WITH ('connector' = 'kafka','topic' = 'topicSex','properties.bootstrap.servers' = '10.30.49.112:9092','format' = 'json','scan.startup.mode' = 'latest-offset', 'properties.group.id' = 'group_topic01');\n" +
            "  set table.sql-dialect = default;\n" +
            "  DROP TABLE IF EXISTS stream_mod_yu_col_12n;\n" +
            "  CREATE TABLE IF NOT EXISTS stream_mod_yu_col_12n(sex String, nm String) WITH ('connector' = 'kafka','topic' = 'topicTest','properties.bootstrap.servers' = '10.30.49.112:9092','format' = 'json');\n" +
            "  INSERT INTO stream_mod_yu_col_12n SELECT sex,nm FROM ( SELECT col_12i.sex, col_12l.nm FROM stream_mod_yu_col_12i  col_12i " +
            "left join dmp.col_12k_rr_z2  col_12l on stream_mod_yu_col_12i.sex = col_12l.id)  AS col_12m;
```
拆分结果:
```
[
  "CREATE CATALOG hiveCatalog WITH('type'='hive','default-database'='default','hive-conf-dir' = '/home/lakehouse/flink/conf','hadoop-conf-dir' = '/home/lakehouse/flink/conf'); USE CATALOG hiveCatalog;set table.sql-dialect = default; DROP TABLE IF EXISTS stream_mod_yu_col_12i ;\n  CREATE TABLE IF NOT EXISTS stream_mod_yu_col_12i(sex String) WITH ('connector' = 'kafka','topic' = 'topicSex','properties.bootstrap.servers' = '10.30.49.112:9092','format' = 'json','scan.startup.mode' = 'latest-offset', 'properties.group.id' = 'group_topic01');DROP TABLE IF EXISTS stream_mod_yu_col_12n ;\n  CREATE TABLE IF NOT EXISTS stream_mod_yu_col_12n(sex String, nm String) WITH ('connector' = 'kafka','topic' = 'topicTest','properties.bootstrap.servers' = '10.30.49.112:9092','format' = 'json');\n  INSERT INTO stream_mod_yu_col_12n SELECT sex,nm FROM ( SELECT col_12i.sex, col_12l.nm FROM stream_mod_yu_col_12i  col_12i left join dmp.col_12k_rr_z2  col_12l on stream_mod_yu_col_12i.sex = col_12l.id)  AS col_12m;"
]
```

case8: 来回切换hive与default方言 且使用视图
```
CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); " +
            "USE CATALOG hiveCatalog;" +
            "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';" +
            "CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; " +
            "set table.sql-dialect = hive;" +
            " CREATE TABLE tag.tagdev_result_220_20230629110648(tag_key String, tag_value String, tag_version String); " +
            " set table.sql-dialect = default;" +
            " set table.sql-dialect = hive;DROP VIEW IF EXISTS stream_mod_x7_col_we; " +
            "CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive;" +
            "set table.sql-dialect = default;" +
            " INSERT INTO tag.tagdev_result_220_20230629110648 SELECT kk AS tagKey, vv AS tagValue, 'v00003' AS tagVersion FROM stream_mod_x7_col_we;" +
            "set table.sql-dialect = hive;" +
            "DROP VIEW IF EXISTS stream_mod_x7_col_we999;
```
拆分结果
```
[
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP VIEW IF EXISTS stream_mod_x7_col_we ; CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive; CREATE TABLE tag.tagdev_result_220_20230629110648(tag_key String, tag_value String, tag_version String); INSERT INTO tag.tagdev_result_220_20230629110648 SELECT kk AS tagKey, vv AS tagValue, 'v00003' AS tagVersion FROM stream_mod_x7_col_we;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP VIEW IF EXISTS stream_mod_x7_col_we999 ;CREATE VIEW stream_mod_x7_col_we999 AS SELECT xzz AS kk, zzsy AS vv FROM syrk_kfk_hive; INSERT INTO tag.tagdev_result_220_20230629110648 SELECT kk AS tagKey, vv AS tagValue, 'v00003' AS tagVersion FROM stream_mod_x7_col_we999;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_220 ; CREATE TABLE tag.tagdev_result_220(tag_key String, tag_value String, tag_version String); INSERT INTO tag.tagdev_result_220 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_220_20230629110648;",
  "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_220 ;CREATE TABLE tag.tagdev_es_sink_220 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );INSERT INTO tag.tagdev_es_sink_220 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00003' || '@@' || 'bq_test0608' || '@@' || '' || '@@' || 'nm:test0608'|| '@@' || 'id:220'|| '@@' || 'tagType:鍩虹鏍囩') AS  tagTotal FROM tag.tagdev_result_220 GROUP BY tag_key) AS t;"
]
```
case9: 反复切换hive方言 + 单一视图 + 使用子查询
```
"CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); " +
            "USE CATALOG hiveCatalog;" +
            "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';" +
            "CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';" +
            "set table.sql-dialect = hive;" +
            "DROP VIEW IF EXISTS stream_mod_x7_col_we; " +
            "CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive; " +
            "set table.sql-dialect = hive;CREATE TABLE tag.tagdev_result_220_20230627121108(tag_key String, tag_value String, tag_version String); " +
            "set table.sql-dialect = default; INSERT INTO tag.tagdev_result_220_20230627121108 SELECT kk AS tagKey, vv AS tagValue, 'v00001' AS tagVersion FROM stream_mod_x7_col_we;" +
            " set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_220; " +
            "CREATE TABLE tag.tagdev_result_220(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_220 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_220_20230627121108;DROP TABLE IF EXISTS tag.tagdev_es_sink_220;CREATE TABLE tag.tagdev_es_sink_220 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );INSERT INTO tag.tagdev_es_sink_220 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00001' || '@@' || 'bq_test0608' || '@@' || '' || '@@' || 'nm:test0608'|| '@@' || 'id:220'|| '@@' || 'tagType:基础标签') AS  tagTotal FROM tag.tagdev_result_220 GROUP BY tag_key) AS t;
```
拆分结果
```
[
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP VIEW IF EXISTS stream_mod_x7_col_we ; CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive;CREATE TABLE tag.tagdev_result_220_20230627121108(tag_key String, tag_value String, tag_version String); INSERT INTO tag.tagdev_result_220_20230627121108 SELECT kk AS tagKey, vv AS tagValue, 'v00001' AS tagVersion FROM stream_mod_x7_col_we;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_220 ; CREATE TABLE tag.tagdev_result_220(tag_key String, tag_value String, tag_version String); INSERT INTO tag.tagdev_result_220 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_220_20230627121108;",
  "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_220 ;CREATE TABLE tag.tagdev_es_sink_220 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );INSERT INTO tag.tagdev_es_sink_220 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00001' || '@@' || 'bq_test0608' || '@@' || '' || '@@' || 'nm:test0608'|| '@@' || 'id:220'|| '@@' || 'tagType:鍩虹鏍囩') AS  tagTotal FROM tag.tagdev_result_220 GROUP BY tag_key) AS t;"
]
```

case10:标签平台组合打标签场景
```
  CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql-dialect = hive; CREATE TABLE IF NOT EXISTS tag.tagdev_result_251_20230704150216(tag_key String, tag_value String, tag_version String);  set table.sql-dialect = default;  set table.sql-dialect = hive;  DROP VIEW IF EXISTS vvvvvv0704;CREATE VIEW vvvvvv0704 AS SELECT tag_key AS kk,  tag_value AS vv FROM tag.dh0704;set table.sql-dialect = default; INSERT INTO tag.tagdev_result_251_20230704150216 SELECT kk AS tagKey, vv AS tagValue, 'v00002' AS tagVersion FROM vvvvvv0704; set table.sql-dialect = hive; CREATE TABLE IF NOT EXISTS tag.tagdev_result_251(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_251_20230704150216 SELECT a.* FROM tag.tagdev_result_251 AS a LEFT JOIN tag.tagdev_result_251_20230704150216 AS b ON a.tag_key=b.tag_key WHERE b.tag_key IS NULL; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_251; CREATE TABLE tag.tagdev_result_251(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_251 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_251_20230704150216;DROP TABLE IF EXISTS tag.tagdev_es_sink_251;CREATE TABLE tag.tagdev_es_sink_251 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );INSERT INTO tag.tagdev_es_sink_251 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00002' || '@@' || 'bq_test0704' || '@@' || '' || '@@' || 'nm:test0704'|| '@@' || 'id:251'|| '@@' || 'tagType:基础标签') AS  tagTotal FROM tag.tagdev_result_251 GROUP BY tag_key) AS t;
```

拆分结果
```
[
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP VIEW IF EXISTS vvvvvv0704 ;CREATE VIEW vvvvvv0704 AS SELECT tag_key AS kk,  tag_value AS vv FROM tag.dh0704; CREATE TABLE IF NOT EXISTS tag.tagdev_result_251_20230704150216(tag_key String, tag_value String, tag_version String);set table.sql-dialect = default;  INSERT INTO tag.tagdev_result_251_20230704150216 SELECT kk AS tagKey, vv AS tagValue, 'v00002' AS tagVersion FROM vvvvvv0704;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_251 ; CREATE TABLE IF NOT EXISTS tag.tagdev_result_251(tag_key String, tag_value String, tag_version String); CREATE TABLE IF NOT EXISTS tag.tagdev_result_251_20230704150216(tag_key String, tag_value String, tag_version String);set table.sql-dialect = default;  INSERT INTO tag.tagdev_result_251_20230704150216 SELECT a.* FROM tag.tagdev_result_251 AS a LEFT JOIN tag.tagdev_result_251_20230704150216 AS b ON a.tag_key=b.tag_key WHERE b.tag_key IS NULL;",
  "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive;  CREATE TABLE IF NOT EXISTS tag.tagdev_result_251_20230704150216(tag_key String, tag_value String, tag_version String);set table.sql-dialect = default;  INSERT INTO tag.tagdev_result_251 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_251_20230704150216;",
  "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;set table.sql-dialect = hive;  CREATE TABLE IF NOT EXISTS tag.tagdev_result_251(tag_key String, tag_value String, tag_version String);set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_251 ;CREATE TABLE IF NOT EXISTS tag.tagdev_es_sink_251 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );INSERT INTO tag.tagdev_es_sink_251 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00002' || '@@' || 'bq_test0704' || '@@' || '' || '@@' || 'nm:test0704'|| '@@' || 'id:251'|| '@@' || 'tagType:基础标签') AS  tagTotal FROM tag.tagdev_result_251 GROUP BY tag_key) AS t;"
]
```
case11: hive跨库查询
```
CREATE CATALOG hiveCatalog WITH('type'='hive','default-database'='default','hive-conf-dir' = '/home/lakehouse/flink/conf','hadoop-conf-dir' = '/home/lakehouse/flink/conf');USE CATALOG hiveCatalog;set table.sql-dialect = hive;CREATE TABLE IF NOT EXISTS syrk_kfk_hive(zzsydm String, swrq String, my_uuid_hash String, my_value_md5 String);CREATE TABLE IF NOT EXISTS dmp.test_zhouwb(aa String, bb String, cc String, dd String); set table.sql-dialect = default; INSERT INTO dmp.test_zhouwb SELECT zzsydm,swrq,my_uuid_hash,my_value_md5 FROM (SELECT col_vy.zzsydm AS zzsydm, col_vy.swrq AS swrq, col_vy.my_uuid_hash AS my_uuid_hash, col_vy.my_value_md5 AS my_value_md5 FROM (SELECT col_vx.zzsydm AS zzsydm, col_vx.swrq AS swrq, col_vx.my_uuid_hash AS my_uuid_hash, col_vx.my_value_md5 AS my_value_md5 FROM syrk_kfk_hive AS col_vx WHERE ((col_vx.zzsydm is not null AND TRIM(col_vx.zzsydm) <> ''))) AS col_vy WHERE ((col_vy.my_value_md5 is not null AND TRIM(col_vy.my_value_md5) <> ''))) AS col_vz
```
拆分结果
```
  CREATE CATALOG hiveCatalog WITH('type'='hive','default-database'='default','hive-conf-dir' = '/home/lakehouse/flink/conf','hadoop-conf-dir' = '/home/lakehouse/flink/conf'); USE CATALOG hiveCatalog;set table.sql-dialect = hive; CREATE TABLE IF NOT EXISTS syrk_kfk_hive(zzsydm String, swrq String, my_uuid_hash String, my_value_md5 String);CREATE TABLE IF NOT EXISTS dmp.test_zhouwb(aa String, bb String, cc String, dd String);INSERT INTO dmp.test_zhouwb SELECT zzsydm,swrq,my_uuid_hash,my_value_md5 FROM (SELECT col_vy.zzsydm AS zzsydm, col_vy.swrq AS swrq, col_vy.my_uuid_hash AS my_uuid_hash, col_vy.my_value_md5 AS my_value_md5 FROM (SELECT col_vx.zzsydm AS zzsydm, col_vx.swrq AS swrq, col_vx.my_uuid_hash AS my_uuid_hash, col_vx.my_value_md5 AS my_value_md5 FROM syrk_kfk_hive AS col_vx WHERE ((col_vx.zzsydm is not null AND TRIM(col_vx.zzsydm) <> ''))) AS col_vy WHERE ((col_vy.my_value_md5 is not null AND TRIM(col_vy.my_value_md5) <> ''))) AS col_vz;
```
