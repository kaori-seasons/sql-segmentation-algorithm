package com.complone;

import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SqlStatementParseTest {


    public static final String case1 = "CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog; CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'; CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.uniondev_202_343; CREATE TABLE tag.uniondev_202_343(tagkey String, newvalue String); INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 ); set table.sql-dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_202_343; CREATE TABLE tag.tagdev_es_sink_202_343 ( id String, my_tj_json_field String, PRIMARY KEY (id) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_130' ); INSERT INTO tag.tagdev_es_sink_202_343 SELECT tagkey, tagToJson(tagkey, '@@', tagTotal, '') AS my_tj_json_field FROM ( select tagkey, collectSet( newvalue || '@@' || 'v00006' || '@@' || 'bq_zh(yqgpz)' || '@@' || '' ) AS tagTotal FROM tag.uniondev_202_343 GROUP BY tagkey ) AS t; set table.sql-dialect = hive; CREATE TABLE tag.tagdev_result_202_343_1686129546001( tagKey String, tagValue String, tagVersion String ); INSERT INTO tag.tagdev_result_202_343_1686129546001 SELECT tagkey AS tagKey, newvalue AS tagValue, 'v00006' AS tagVersion FROM tag.uniondev_202_343; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_202; CREATE TABLE tag.tagdev_result_202( tagKey String, tagValue String, tagVersion String ); INSERT INTO tag.tagdev_result_202 SELECT tagKey, tagValue, tagVersion FROM tag.tagdev_result_202_343_1686129546001;";

    public static final String case2 = " CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog; CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'; CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql - dialect = hive; DROP TABLE IF EXISTS tag.uniondev_202_343; CREATE TABLE tag.uniondev_202_343(tagkey String, newvalue String); INSERT INTO tag.uniondev_202_343 SELECT tagkey, newvalue FROM ( SELECT tagkey, '有钱' newvalue FROM tag.tagdev_result_199 WHERE tag.tagdev_result_199.tagvalue >= 100 UNION ALL SELECT tagkey, '高' newvalue FROM tag.tagdev_result_198 WHERE tag.tagdev_result_198.tagvalue >= 150 UNION ALL SELECT tagkey, '肥' newvalue FROM tag.tagdev_result_197 WHERE tag.tagdev_result_197.tagvalue >= 222 );";

    public static final String case3 = "CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog; CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'; CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql - dialect = default; DROP TABLE IF EXISTS tag.tagdev_es_sink_202_343; CREATE TABLE tag.tagdev_es_sink_202_343 ( id String, my_tj_json_field String, PRIMARY KEY (id) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_130' ); INSERT INTO tag.tagdev_es_sink_202_343 SELECT tagkey, tagToJson(tagkey, '@@', tagTotal, '') AS my_tj_json_field FROM ( select tagkey, collectSet( newvalue || '@@' || 'v00006' || '@@' || 'bq_zh(yqgpz)' || '@@' || '' ) AS tagTotal FROM tag.uniondev_202_343 GROUP BY tagkey ) AS t;";

    public static final String case4 = "CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog; CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'; CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql - dialect = hive; CREATE TABLE tag.tagdev_result_202_343_1686129546001( tagKey String, tagValue String, tagVersion String ); INSERT INTO tag.tagdev_result_202_343_1686129546001 SELECT tagkey AS tagKey, newvalue AS tagValue, 'v00006' AS tagVersion FROM tag.uniondev_202_343;";

    public static final String case5 = "CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog; CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf'; CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql - dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_202; CREATE TABLE tag.tagdev_result_202( tagKey String, tagValue String, tagVersion String ); INSERT INTO tag.tagdev_result_202 SELECT tagKey, tagValue, tagVersion FROM tag.tagdev_result_202_343_1686129546001;";


    public static final String case0 = "CREATE FUNCTION imtm as 'com.meiya.whale.flink.clean.format.udf.UdfImtm'; CREATE CATALOG myhive WITH ( 'type' = 'hive', 'default-database' = 'test', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' ); USE CATALOG myhive;  CREATE TABLE if not exists kafka_sink ( id STRING, name STRING, age INT, addr STRING, createtime TIMESTAMP ) WITH ( 'connector' = 'kafka', 'topic' = 'test_user_from_hive', 'properties.bootstrap.servers' = '10.30.50.249:21007,10.30.50.250:21007,10.30.50.251:21007', 'properties.group.id' = 'group_topic01', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'properties.security.protocol' = 'SASL_PLAINTEXT', 'properties.sasl.kerberos.service.name' = 'kafka', 'properties.kerberos.domain.name' = 'hadoop.hadoop.com' ); create table test_user_id32 ( id STRING, name STRING, age INT, addr STRING ) WITH ( 'connector' = 'kafka', 'topic' = 'test_user_from_hive', 'properties.bootstrap.servers' = '10.30.50.249:21007,10.30.50.250:21007,10.30.50.251:21007', 'properties.group.id' = 'group_topic01', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'properties.security.protocol' = 'SASL_PLAINTEXT', 'properties.sasl.kerberos.service.name' = 'kafka', 'properties.kerberos.domain.name' = 'hadoop.hadoop.com' ); insert into kafka_sink select id, name, age, addr, imtm() as createtime from test_user_id32; ";

    public static final String case6 = "CREATE FUNCTION imtm as 'com.meiya.whale.flink.clean.format.udf.UdfImtm'; CREATE CATALOG myhive WITH ( 'type' = 'hive', 'default-database' = 'test', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' ); USE CATALOG myhive;  CREATE TABLE if not exists kafka_sink ( id STRING, name STRING, age INT, addr STRING, createtime TIMESTAMP ) WITH ( 'connector' = 'kafka', 'topic' = 'test_user_from_hive', 'properties.bootstrap.servers' = '10.30.50.249:21007,10.30.50.250:21007,10.30.50.251:21007', 'properties.group.id' = 'group_topic01', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'properties.security.protocol' = 'SASL_PLAINTEXT', 'properties.sasl.kerberos.service.name' = 'kafka', 'properties.kerberos.domain.name' = 'hadoop.hadoop.com' ); create table test_user_id32 ( id STRING, name STRING, age INT, addr STRING ) WITH ( 'connector' = 'kafka', 'topic' = 'test_user_from_hive', 'properties.bootstrap.servers' = '10.30.50.249:21007,10.30.50.250:21007,10.30.50.251:21007', 'properties.group.id' = 'group_topic01', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'properties.security.protocol' = 'SASL_PLAINTEXT', 'properties.sasl.kerberos.service.name' = 'kafka', 'properties.kerberos.domain.name' = 'hadoop.hadoop.com' ); insert into kafka_sink select id, name, age, addr, imtm() as createtime from (select * from test_user_id32 );";

    public static final String old_test = "CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' );" +
            " USE CATALOG hiveCatalog; " +
            " create database test; " +
            " set table.sql-dialect = hive; " +
            " create table if not exists a (xx string, yy string, zz string, primary key(x,z) disable novalidate rely ); " +
            " create table if not exists b ( xx string, yy string, zz string, primary key(x,z) disable novalidate rely ); " +
            " set table.sql-dialect = default; " +
            " create table if not exists c (asss string, cvvv string, bfs string) with ( 'connector' = 'kafka', 'scan.mode.inital' = 'inital' ); " +
            " insert into `default_catalog`.`default_database`.`c` select * from a left join b on `hiveCatalog`.`test`.`a`.id = `hiveCatalog`.`test`.`b`.id;";


    public static final String temp_test = "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', " +
            "'hadoop-conf-dir'='/home/lakehouse/flink/conf' );" +

            " USE CATALOG hiveCatalog;" +
            "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';" +
            "CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';" +
            "set table.sql-dialect = hive;" +
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
            "set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_220; CREATE TABLE tag.tagdev_result_220(tagKey String, tagValue String, tagVersion String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_220 SELECT tagKey, tagValue, tagVersion FROM tag.tagdev_result_220_364_1687331953048;";


    public static final String sql4 = "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); " +
            "USE CATALOG hiveCatalog;" +
            "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';" +
            "CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet';" +
            "set table.sql-dialect = hive;" +
            "DROP VIEW IF EXISTS stream_mod_x7_col_we; " +
            "CREATE VIEW stream_mod_x7_col_we AS SELECT xzz AS kk, concat(zzsy,'abc') AS vv FROM syrk_kfk_hive; " +
            "set table.sql-dialect = hive;CREATE TABLE tag.tagdev_result_220_20230627121108(tag_key String, tag_value String, tag_version String); " +
            "set table.sql-dialect = default; INSERT INTO tag.tagdev_result_220_20230627121108 SELECT kk AS tagKey, vv AS tagValue, 'v00001' AS tagVersion FROM stream_mod_x7_col_we;" +
            " set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_220; " +
            "CREATE TABLE tag.tagdev_result_220(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_220 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_220_20230627121108;DROP TABLE IF EXISTS tag.tagdev_es_sink_220;CREATE TABLE tag.tagdev_es_sink_220 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );INSERT INTO tag.tagdev_es_sink_220 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00001' || '@@' || 'bq_test0608' || '@@' || '' || '@@' || 'nm:test0608'|| '@@' || 'id:220'|| '@@' || 'tagType:基础标签') AS  tagTotal FROM tag.tagdev_result_220 GROUP BY tag_key) AS t;";

    public static final String sql5 =  "CREATE CATALOG hiveCatalog WITH('type'='hive','default-database'='default','hive-conf-dir' = '/home/lakehouse/flink/conf','hadoop-conf-dir' = '/home/lakehouse/flink/conf');\n" +
            "  USE CATALOG hiveCatalog;\n" +
            "  DROP TABLE IF EXISTS stream_mod_yu_col_12i;\n" +
            "  CREATE TABLE IF NOT EXISTS stream_mod_yu_col_12i(sex String) WITH ('connector' = 'kafka','topic' = 'topicSex','properties.bootstrap.servers' = '10.30.49.112:9092','format' = 'json','scan.startup.mode' = 'latest-offset', 'properties.group.id' = 'group_topic01');\n" +
            "  set table.sql-dialect = default;\n" +
            "  DROP TABLE IF EXISTS stream_mod_yu_col_12n;\n" +
            "  CREATE TABLE IF NOT EXISTS stream_mod_yu_col_12n(sex String, nm String) WITH ('connector' = 'kafka','topic' = 'topicTest','properties.bootstrap.servers' = '10.30.49.112:9092','format' = 'json');\n" +
            "  INSERT INTO stream_mod_yu_col_12n SELECT sex,nm FROM ( SELECT col_12i.sex, col_12l.nm FROM stream_mod_yu_col_12i  col_12i " +
            "left join dmp.col_12k_rr_z2  col_12l on stream_mod_yu_col_12i.sex = col_12l.id)  AS col_12m;";

    public static final String sql6 = "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); " +
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
            "INSERT INTO tag.tagdev_es_sink_220 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00003' || '@@' || 'bq_test0608' || '@@' || '' || '@@' || 'nm:test0608'|| '@@' || 'id:220'|| '@@' || 'tagType:基础标签') AS  tagTotal FROM tag.tagdev_result_220 GROUP BY tag_key) AS t;";



    public static final String sql7 = "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;\n" +
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
            "INSERT INTO stream_mod_y9_col_118 SELECT column_xm,column_zjhm FROM stream_mod_y9_col_135 AS col_135;";


    public static final String sql8 = "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql-dialect = hive; CREATE TABLE tag.tagdev_result_243_20230630094623(tag_key String, tag_value String, tag_version String);  set table.sql-dialect = default;  INSERT INTO tag.tagdev_result_243_20230630094623 SELECT tagKey AS tagKey, tagvalue  AS tagValue, 'v00001' AS tagVersion FROM tagdev_result_202; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_243; CREATE TABLE tag.tagdev_result_243(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_243 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_243_20230630094623;DROP TABLE IF EXISTS tag.tagdev_es_sink_243;CREATE TABLE tag.tagdev_es_sink_243 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_132' );INSERT INTO tag.tagdev_es_sink_243 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00001' || '@@' || 'bq_sg_1' || '@@' || '' || '@@' || 'nm:身高'|| '@@' || 'id:243') AS  tagTotal FROM tag.tagdev_result_243 GROUP BY tag_key) AS t;";


    public static final String sql9 = "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql-dialect = hive; CREATE TABLE IF NOT EXISTS tag.tagdev_result_243_20230629183106(tag_key String, tag_value String, tag_version String);  INSERT INTO tag.tagdev_result_243_20230629183106 SELECT tagKey AS tagKey, tagvalue  AS tagValue, 'v00001' AS tagVersion FROM tagdev_result_202; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_243; CREATE TABLE tag.tagdev_result_243(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_243 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_243_20230629183106;DROP TABLE IF EXISTS tag.tagdev_es_sink_243;CREATE TABLE tag.tagdev_es_sink_243 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_132' );INSERT INTO tag.tagdev_es_sink_243 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00001' || '@@' || 'bq_sg_1' || '@@' || '' || '@@' || 'nm:身高'|| '@@' || 'id:243') AS  tagTotal FROM tag.tagdev_result_243 GROUP BY tag_key) AS t;";


    public static final String sql10 = "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog;CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; set table.sql-dialect = hive; CREATE TABLE IF NOT EXISTS tag.tagdev_result_243_20230629183106(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_243_20230629183106 SELECT tagKey AS tagKey, tagvalue  AS tagValue, 'v00001' AS tagVersion FROM tag.tagdev_result_202; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_243; CREATE TABLE tag.tagdev_result_243(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_243 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_243_20230629183106;DROP TABLE IF EXISTS tag.tagdev_es_sink_243;CREATE TABLE tag.tagdev_es_sink_243 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_30190388_132' );INSERT INTO tag.tagdev_es_sink_243 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00001' || '@@' || 'bq_sg_1' || '@@' || '' || '@@' || 'nm:身高'|| '@@' || 'id:243') AS  tagTotal FROM tag.tagdev_result_243 GROUP BY tag_key) AS t;";


    public static final String sql11 = "CREATE TABLE performance(id Long, area String, city String, amount Decimal, curr_date String) WITH('connector'='jdbc','url'='jdbc:mysql://10.30.150.28:3306/test','table-name'='performance','username'='root','password'='root@2022');CREATE TABLE t1(id Long, area String, city String, amount Decimal, curr_date String) WITH ('connector'='jdbc','url'='jdbc:mysql://10.30.150.28:13306/test','table-name'='metrics_permance','username'='root','password'='root@2022');insert into t1 select id, area, city, amount, curr_date from performance;";


    public static final String sql12 = "CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' );" +
            " USE CATALOG hiveCatalog;" +
            "CREATE FUNCTION IF NOT EXISTS tagToJson AS 'com.meiya.whale.flink.key.udf.tag.TagToJsonUdf';" +
            "CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; " +
            "set table.sql-dialect = hive; CREATE TABLE IF NOT EXISTS tag.tagdev_result_251_20230704150216(tag_key String, tag_value String, tag_version String);  set table.sql-dialect = default;  set table.sql-dialect = hive;  DROP VIEW IF EXISTS vvvvvv0704;CREATE VIEW vvvvvv0704 AS SELECT tag_key AS kk,  tag_value AS vv FROM tag.dh0704;set table.sql-dialect = default; INSERT INTO tag.tagdev_result_251_20230704150216 SELECT kk AS tagKey, vv AS tagValue, 'v00002' AS tagVersion FROM vvvvvv0704  set table.sql-dialect = hive; CREATE TABLE IF NOT EXISTS tag.tagdev_result_251(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_251_20230704150216 SELECT a.* FROM tag.tagdev_result_251 AS a LEFT JOIN tag.tagdev_result_251_20230704150216 AS b ON a.tag_key=b.tag_key WHERE b.tag_key IS NULL; set table.sql-dialect = hive; DROP TABLE IF EXISTS tag.tagdev_result_251; CREATE TABLE tag.tagdev_result_251(tag_key String, tag_value String, tag_version String); set table.sql-dialect = default; INSERT INTO tag.tagdev_result_251 SELECT tag_key, tag_value, tag_version FROM tag.tagdev_result_251_20230704150216;DROP TABLE IF EXISTS tag.tagdev_es_sink_251;CREATE TABLE tag.tagdev_es_sink_251 ( id String, my_tj_json_field String, PRIMARY KEY ( id ) NOT ENFORCED ) WITH ( 'connector' = 'elasticsearch-7', 'hosts' = 'https://10.30.50.250:24100;https://10.30.50.251:24100', 'index' = 'tagdev_index_temp_107097_67' );INSERT INTO tag.tagdev_es_sink_251 SELECT tag_key,tagToJson(tag_key,'@@',tagTotal,'') AS my_tj_json_field FROM (select tag_key, collectSet(tag_value || '@@' || 'v00002' || '@@' || 'bq_test0704' || '@@' || '' || '@@' || 'nm:test0704'|| '@@' || 'id:251'|| '@@' || 'tagType:基础标签') AS  tagTotal FROM tag.tagdev_result_251 GROUP BY tag_key) AS t;";

    public static final String SELECT_STATEMENT = "CREATE CATALOG hiveCatalog WITH('type'='hive','default-database'='default','hive-conf-dir' = '/home/lakehouse/flink/conf','hadoop-conf-dir' = '/home/lakehouse/flink/conf');USE CATALOG hiveCatalog; CREATE TABLE IF NOT EXISTS syrk_kfk_hive(zzsydm String, swrq String, my_uuid_hash String, my_value_md5 String);CREATE TABLE IF NOT EXISTS dmp.test_zhouwb(aa String, bb String, cc String, dd String);INSERT INTO dmp.test_zhouwb SELECT zzsydm,swrq,my_uuid_hash,my_value_md5 FROM (SELECT col_vy.zzsydm AS zzsydm, col_vy.swrq AS swrq, col_vy.my_uuid_hash AS my_uuid_hash, col_vy.my_value_md5 AS my_value_md5 FROM (SELECT col_vx.zzsydm AS zzsydm, col_vx.swrq AS swrq, col_vx.my_uuid_hash AS my_uuid_hash, col_vx.my_value_md5 AS my_value_md5 FROM syrk_kfk_hive AS col_vx WHERE ((col_vx.zzsydm is not null AND TRIM(col_vx.zzsydm) <> ''))) AS col_vy WHERE ((col_vy.my_value_md5 is not null AND TRIM(col_vy.my_value_md5) <> ''))) AS col_vz";

    public static final String sql13 = "CREATE CATALOG hiveCatalog WITH('type'='hive','default-database'='default','hive-conf-dir' = '/home/lakehouse/flink/conf','hadoop-conf-dir' = '/home/lakehouse/flink/conf');USE CATALOG hiveCatalog;CREATE CATALOG hiveCatalog WITH('type'='hive','default-database'='default','hive-conf-dir' = '/home/lakehouse/flink/conf','hadoop-conf-dir' = '/home/lakehouse/flink/conf');USE CATALOG hiveCatalog;CREATE TABLE IF NOT EXISTS syrk_kfk_hive(zzsydm String, swrq String, my_uuid_hash String, my_value_md5 String);CREATE TABLE IF NOT EXISTS dmp.test_zhouwb(aa String, bb String, cc String, dd String);INSERT INTO dmp.test_zhouwb SELECT zzsydm,swrq,my_uuid_hash,my_value_md5 FROM (SELECT col_vy.zzsydm AS zzsydm, col_vy.swrq AS swrq, col_vy.my_uuid_hash AS my_uuid_hash, col_vy.my_value_md5 AS my_value_md5 FROM (SELECT col_vx.zzsydm AS zzsydm, col_vx.swrq AS swrq, col_vx.my_uuid_hash AS my_uuid_hash, col_vx.my_value_md5 AS my_value_md5 FROM syrk_kfk_hive AS col_vx WHERE ((col_vx.zzsydm is not null AND TRIM(col_vx.zzsydm) <> ''))) AS col_vy WHERE ((col_vy.my_value_md5 is not null AND TRIM(col_vy.my_value_md5) <> ''))) AS col_vz";

    public static final String sql14 = "DROP TABLE IF EXISTS stream_mod_2ye_col_1pdg;CREATE TABLE IF NOT EXISTS stream_mod_2ye_col_1pdg(xm String, zjhm String, xb String, jg String, chinese String, math String, english String) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://10.30.49.8:3306/test','table-name' = 'mark_zhong2023','username' = 'pico','password' = 'pico-nf-8100'); CREATE CATALOG hiveCatalog WITH ('type'='hive', 'default-database'='default','hive-conf-dir'='/home/lakehouse/flink/conf', 'hadoop-conf-dir'='/home/lakehouse/flink/conf' ); USE CATALOG hiveCatalog; set table.sql-dialect = hive;  DROP VIEW IF EXISTS stream_mod_2ye_col_1pdh;  CREATE VIEW stream_mod_2ye_col_1pdh AS SELECT col_1pdg.xm AS xm, col_1pdg.zjhm AS zjhm, col_1pdg.xb AS xb, col_1pdg.jg AS jg, col_1pdg.chinese AS chinese, col_1pdg.math AS math, col_1pdg.english AS english FROM stream_mod_2ye_col_1pdg AS col_1pdg WHERE ((col_1pdg.zjhm is not null AND TRIM(col_1pdg.zjhm) <> '')); set table.sql-dialect = default;  DROP TABLE IF EXISTS stream_mod_2ye_col_1pdw;CREATE TABLE IF NOT EXISTS stream_mod_2ye_col_1pdw(xm String, zjhm String, xb String, jg String, chinese String, math String, english String) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://10.30.49.8:3306/test','table-name' = 'chengji_table_test','username' = 'pico','password' = 'pico-nf-8100');INSERT INTO stream_mod_2ye_col_1pdw SELECT xm,zjhm,xb,jg,chinese,math,english FROM stream_mod_2ye_col_1pdh AS col_1pdh";
    @Test
    public void parse() throws Exception {
        String statement = "CREATE FUNCTION IF NOT EXISTS collectSet AS 'com.meiya.whale.flink.aggregate.udaf.CollectSet'; create table source ( stuid string, stuname string, age INT ) with ( 'connector' = 'hive', 'xxx' = 'xxx' ); CREATE TABLE if not exists sink AS SELECT collectSet( 'xxxxx' || '@@' || 'v00006' || '@@' || 'bq_zh(yqgpz)' || '@@' || '' ) AS tagTotal from source ;";
        List<String> operationList =  SqlStatemenetParser.parseSqlStatement(statement);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    //呈依赖关系解析sql的执行顺序
    @Test
    public void case1Parse() {
        List<String> operationList =SqlStatemenetParser.parseSqlStatement(case0);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void testParse() {
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(old_test);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void splitSql() {
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(case1);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void testMutliLevelQuery() {
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(case6);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test1(){
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(temp_test);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test4() {
        Map<String, TableInsert> result =  SqlStatemenetParser.analysisOutput(case1);
        System.out.println(result);
    }

    @Test
    public void test5() {
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql4);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test6(){
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql5);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test7() {
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql6);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test8() {
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(SELECT_STATEMENT);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test9(){
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql7);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }


    @Test
    public void test10(){
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql8);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test11() {
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql9);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test12() {
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql10);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test13(){
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql11);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }


    @Test
    public void test14(){
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql12);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

    @Test
    public void test15() {
        List<String> operationList = SqlStatemenetParser.parseSqlStatement(sql14);
        System.out.println(GsonUtils.getGson().toJson(operationList));
    }

}
