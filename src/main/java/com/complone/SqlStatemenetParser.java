package com.complone;


import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.ddl.*;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dql.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.complone.Constants.*;
import static com.complone.StatementParser.*;

public class SqlStatemenetParser {


    public static Map<String, TableInsert> analysisOutput(String statement) {
        Map<String, String> functionMap = new HashMap<>();
        Map<String, TableLineager> markDataMap =  recordLineagerRelation(statement, functionMap);
        Map<String, TableInsert> columnsMap = new HashMap<>();
        markDataMap.entrySet().stream().filter(x -> x.getKey().contains(FINAL_INSERT_NODE)).forEach(x -> columnsMap.put( x.getKey(), new TableInsert(x.getValue().getStatement(), extractColumns(x.getValue().getStatement()).toString())));
        return columnsMap;
    }


    public static List<String> extractColumns(String statement) {
        SqlParser.Config config = createSqlParserConfig(true);
        SqlParser sqlParser = SqlParser.create(statement, config);
        List<String> columnList = new ArrayList<>();
        SqlNodeList sqlNodes = null;
        try {
            sqlNodes = sqlParser.parseStmtList();
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            specialStrategy(statement, true, e);
        }
        SqlNode node = sqlNodes.get(0);
        RichSqlInsert insertNode = (RichSqlInsert) node;
        SqlSelect sqlSelect = (SqlSelect) insertNode.getSource();
        List<SqlNode> nodeList = sqlSelect.getSelectList().getList();
        for (SqlNode item: nodeList) {
            if (item instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier)item;
                columnList.add(identifier.getSimple());
            } else if (item instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) item;
                List<SqlNode> sqlNodeList = Arrays.asList(basicCall.getOperands()).stream().filter(x -> x instanceof SqlIdentifier).collect(Collectors.toList());
                SqlNode asNode = sqlNodeList.get(0);
                SqlIdentifier asIdentifier = (SqlIdentifier) asNode;
                columnList.add(asIdentifier.getSimple());
            }
        }
        return columnList;
    }

    /**
     * 将完整的SQL语句，拆分成独立的小任务
     *
     * @param sqlStatement
     * @return
     */
    public static List<String> parseSqlStatement(String sqlStatement) {

        Map<String, String> functionList = new HashMap<>();
        Map<String, TableLineager> markTableMap = recordLineagerRelation(sqlStatement, functionList);
        // 遍历收集的SqlOperation为 subSqlJob
        // 根据insert语句倒序遍历重建字符串
        // 插入方言时 需要维护顺序 在建hive表的语句前插入
        // 如果方言产生切换, 说明存在目标数据源与源端数据源catalog不一致的问题
        // 重建子SQL的实现思路是从insert语句开始，根据name保存的源端表反向推理出源表依赖路径上
        // 存在方言切换和视图语法的别名
        // 所以为了重建上下文， 需要先对connector类型分组
        // 在第一个hive语句和第一个default语句前插入方言,
        // 将所需要依赖的udf放在最前面

        return rebuildSqlStatement(markTableMap, functionList);
    }


    public static Map<String, TableLineager> recordLineagerRelation(String sqlStatement, Map<String, String> functionList) {
        List<String> statementList = generateStatementList(sqlStatement);
        List<SqlOperation> sqlOperationList = new ArrayList<>();
        Map<String, Object> result = new HashMap<>();
        AtomicReference<Boolean> isFlinkParser = new AtomicReference<>(true);
        Map<String, TableLineager> markTableMap = new HashMap<>();
        AtomicInteger insertSqlCount = new AtomicInteger();
        //顺序遍历
        for( int i =0; i<statementList.size(); i++) {
            String statement = statementList.get(i);
            isFlinkParser.set(judgeFlinkParser(statement, isFlinkParser.get()));
            //递归遍历之前 先找到join两端的表连接器
            Optional<SqlOperation> optionalStatement = parseStatement(statement, isFlinkParser.get());
            markTableMap = JobOptimizer.analysisLineager(optionalStatement.get(), markTableMap, isFlinkParser.get(), functionList, insertSqlCount);
            sqlOperationList.add(optionalStatement.get());
        }
        return markTableMap;
    }

    /**
     * 拆分大SQL为子SQL
     * @param markDataMap
     * @param functionMap 此处的functionList在遍历的时候应当可以从上下文获取到
     * 但需要在解析阶段判断udf节点,暂时先定义全部udf
     */
    public static List<String> rebuildSqlStatement(Map<String, TableLineager> markDataMap, Map<String, String> functionMap) {
        //分为四类节点 建表的hive与非hive语句,catalog,以及insert into语句

        //如果catalog分多类切换,且存在多个的时候，需要分离map，取决于insert into语句是否存在.两级符号
        List<String> subSqlList = new ArrayList<>();
        Map<String, TableLineager> insertSqlNode = markDataMap.entrySet().stream().filter(x -> x.getKey().contains(FINAL_INSERT_NODE)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, TableLineager> catalogSqlMap = markDataMap.entrySet().stream().filter(x -> x.getValue().getIsCatalog()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, TableLineager> createTableMap = markDataMap.entrySet().stream().filter(x -> Objects.nonNull(x.getValue().getNext())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, TableLineager> hiveTableMap = Optional.of(createTableMap).orElse(new HashMap<>()).entrySet().stream().filter(x -> x.getValue().getConnector().contains(HIVE_STRING)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, TableLineager> defaultTableMap = Optional.of(createTableMap).orElse(new HashMap<>()).entrySet().stream().filter(x -> !x.getValue().getConnector().contains(HIVE_STRING)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        //需要解析所有的Final节点 遍历Final减少时间复杂度
        String hiveDialect = "set table.sql-dialect = hive; ";
        String defaultDialect = "set table.sql-dialect = default; ";

        AtomicInteger repeatTable = new AtomicInteger(0);
        Map<String, AtomicInteger> repeatTableStats = new HashMap<>();
        //解析其中一个节点,找到匹配的同步任务列表
        for (Map.Entry<String, TableLineager> insert : insertSqlNode.entrySet()) {
            String insertStatement = insert.getValue().getStatement();
            String syncList = insert.getValue().getTableName();
            List<String> sources = Arrays.asList(syncList.split(","));
            AtomicInteger hiveSwitch = new AtomicInteger(0);
            AtomicInteger defaultSwitch = new AtomicInteger(0);
            //用于统计一个catalog下面 不同方言 比如用hIve的catalog创建了两个表 这两个表维护各自的方言切换统计器，
            // 但是如果需要将各自的方言维护, 则会发生在一个方言当中已经统计过的表出现在另一个方言的方言切换统计器当中
            AtomicInteger catalogSwitch = new AtomicInteger(0);
            StringBuilder functionBuilder = new StringBuilder();
            // 根据每个subsql拆分udf定义, 挂载UDF到insertStatement当中
            for (Map.Entry<String, String> item: functionMap.entrySet()) {
                String key = item.getKey();
                if (insertStatement.contains(key)){
                    functionBuilder.append(item.getValue()).append(";");
                }
            }

            StringBuilder totalBuilder = new StringBuilder();
            StringBuilder hiveBuilder = new StringBuilder();
            StringBuilder defaultBuilder = new StringBuilder();
            String functionStr = functionBuilder.toString();
            totalBuilder.append(functionStr);
            //当前insert语句中需要切换的catalog
            TableLineager catalog = new TableLineager();
            for (String tableName : sources) {
                //TODO 根据表的出现次数维护sql输出顺序
                assemableSingleStatement(hiveBuilder, hiveTableMap, catalogSqlMap, catalog, tableName, hiveDialect, hiveSwitch, HIVE_STRING, catalogSwitch, repeatTableStats);
                assemableSingleStatement(defaultBuilder, defaultTableMap, catalogSqlMap, catalog, tableName, defaultDialect, defaultSwitch, DEFAULT_STRING, catalogSwitch, repeatTableStats);
            }
            totalBuilder.append(hiveBuilder.toString()).append(defaultBuilder.toString()).append(insertStatement).append(";");
            subSqlList.add(totalBuilder.toString());
        }
        return subSqlList;
    }

    /**
     * 根据不同方言填充语句
     * @param sb
     * @param dialect
     * @param catalogSqlMap
     * @param catalog
     * @param tableName
     * @param dialectStatement
     * @return
     */
    public static StringBuilder assemableSingleStatement(StringBuilder sb, Map<String, TableLineager> dialect, Map<String, TableLineager> catalogSqlMap,TableLineager catalog, String tableName, String dialectStatement,
                                                         AtomicInteger dialectSwitch, String dialectStr, AtomicInteger catalogSwitch,Map<String, AtomicInteger> repeatTable ){


        tableName = tableName.trim();
        TableLineager item = dialect.get(tableName);



        //此处还需要根据catalog解析不同的hive表
        String defaultDatabase  = "";
        if (tableName.split("\\.").length == 3){
            String[] fullTablePath =  tableName.split("\\.");
            catalog = catalogSqlMap.get(fullTablePath[0]);
        } else if (tableName.split("\\.").length == 2) {
            String[] fullTablePath =  tableName.split("\\.");
            defaultDatabase = fullTablePath[0];
        }

        if (catalogSqlMap.size() == 1) {
            List<TableLineager> list = new ArrayList<>(catalogSqlMap.values());
            catalog = list.get(0);
        }

        if (dialectStr.equals(DEFAULT_STRING)) {
            //没有catalog的话 就是默认方言
            catalog = new TableLineager();
            catalog.setTableName("catalog");
            catalog.setConnector("default");
        }


        if (Objects.isNull(catalog.getConnector()) || Objects.isNull(catalog.getTableName())) {
            return sb;
        }

        // 1. 如果catalog是针对单一关系的强关联使用(比如表能通过hive的connector找到,
        // 或者能通过其他关系识别) 则从上下文当中恢复
        // 2. 如果catalog是针对单一关系的弱关联使用 (比如在insert语句的重建阶段,
        // 根据表的database不能解析出与catalog的强绑定关系):
        // 通过约定的方式解决: 即不存在跨catalog同步的情况,如果出现跨database的情况下 一定是要在同一个catalog的情况下才能进行
        //方言只需要切换一次
        if (dialectSwitch.get() < 1 && catalog.getConnector().equals(dialectStr)) {
            //如果hive类型的表都是从同一个catalog创建的 则需要判断建表的时候如何切换方言
            if(StringUtils.isNotBlank(defaultDatabase) && catalogSwitch.get() < 1) {
                List<TableLineager> catalogList = new ArrayList<>(catalogSqlMap.values());
                catalog = catalogList.get(0);
                sb.append(catalog.getStatement()).append(";");
                sb.append(String.format(" USE CATALOG %s;", catalog.getTableName()));
                catalogSwitch.getAndIncrement();
            } else if (catalog.getIsUse()) {
                sb.append(catalog.getStatement()).append(";");
                sb.append(String.format(" USE CATALOG %s;", catalog.getTableName()));
            }

            if (dialect.size() !=0) {
                dialectSwitch.getAndIncrement();
                sb.append(dialectStatement);
            }
        }

        if (Objects.isNull(item)) {
            return sb;
        }


        //如果表在之前执行的时候已经定义了，则没有必要添加
        if (item.getConnector().equals(REDEFINE_CREATE_TABLE_NODE)) {
            return sb;
        }

        if (item.isDrop()){
            if (item.getIsView()) {
                sb.append(String.format("DROP VIEW IF EXISTS %s ", tableName)).append(";");
            } else {
                sb.append(String.format("DROP TABLE IF EXISTS %s ", tableName)).append(";");
            }
            item.setDrop(false);
            dialect.put(tableName, item);
        }

        //解析视图的血缘关系
        if (CollectionUtils.isNotEmpty(item.getVirualTableList())) {
            List<String> virualTableList = item.getVirualTableList();
            virualTableList.stream().forEach(x -> {
                TableLineager tableLineager = dialect.get(x);
                if (Objects.isNull(tableLineager)) {
                    return;
                }
                sb.append(optimizerSql(tableLineager.getStatement(), "CREATE VIEW", "CREATE VIEW IF NOT EXISTS")).append(";");
                dialect.put(x, item);

            });
        }

        sb.append(optimizerSql(item.getStatement() , "CREATE TABLE", "CREATE TABLE IF NOT EXISTS"));
        dialect.put(tableName, item);
        sb.append(";");


        return sb;
    }

    public static String optimizerSql(String statement, String originSql, String replaceSql) {
        Pattern replace = Pattern.compile(replaceSql);

        if (replace.matcher(statement).find()) {
            return statement;
        }
        Pattern pattern = Pattern.compile(originSql, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(statement);
        return matcher.replaceAll(replaceSql);
    }

    /**
     * 解析sql
     * 修改返回值为 Map<String, SqlOperation> 为<方言, sql父亲节点>的映射关系
     * @param statement
     * @param isFlinkParser
     * @return
     */
    public static Optional<SqlOperation> parseStatement(String statement, boolean isFlinkParser) {
        SqlParser.Config config = createSqlParserConfig(isFlinkParser);
        SqlParser sqlParser = SqlParser.create(statement, config);
        SqlNodeList sqlNodes;
        try {
            sqlNodes = sqlParser.parseStmtList();
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            return StatementUtil.processSqlOperationException(statement, isFlinkParser, e);
        }
        if (sqlNodes.size() != 1) {
            throw new SqlParseException("Only single statement is supported now");
        }


        //TODO 判断子节点的类型递归深度是否到达operandList.size
        final String[] operands;
        final StatementParser.SqlCommand cmd;
        SqlNode node = sqlNodes.get(0);
        SqlOperation sqlOperation = null;
        if (node instanceof SqlOrderBy) {
            cmd = StatementParser.SqlCommand.SELECT;
            SqlOrderBy sqlOrderBy = (SqlOrderBy) node;
            SqlSelect sqlSelect = (SqlSelect) sqlOrderBy.query;
            if (sqlSelect.getFrom() instanceof SqlJoin) {
                SqlJoin sqlJoin = (SqlJoin) sqlSelect.getFrom();
                operands = new String[]{sqlJoin.getKind().toString(), statement};
            } else {
                operands = new String[]{sqlSelect.getFrom().toString(), statement};
            }
        } else if (node instanceof SqlWith) {
            cmd = StatementParser.SqlCommand.WITH;
            SqlWith sqlWith = (SqlWith) node;
            SqlSelect sqlSelect = (SqlSelect) sqlWith.body;
            operands = new String[]{sqlSelect.getFrom().toString(), statement};
        } else if (node.getKind().belongsTo(SqlKind.QUERY)) {
            cmd = StatementParser.SqlCommand.SELECT;
            SqlSelect sqlSelect = (SqlSelect) node;
            if (sqlSelect.getFrom() instanceof SqlJoin) {
                SqlJoin sqlJoin = (SqlJoin) sqlSelect.getFrom();
                operands = new String[]{sqlJoin.getKind().toString(), statement};
            } else if (sqlSelect.getFrom() instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) sqlSelect.getFrom();
                operands = new String[]{basicCall.operands[0].toString(), statement};
            } else if (sqlSelect.getFrom() instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) sqlSelect.getFrom();
                sqlOperation = new SqlOperation(null, SqlOperationEnum.TABLE, statement, null, identifier.names);
            } else {
                throw new RuntimeException("select operation translate failed!");
            }
        } else if (node instanceof RichSqlInsert) {
            Set<String> sourceTableList = new HashSet<>();
            RichSqlInsert insertNode = (RichSqlInsert) node;
            if (insertNode.getSource() instanceof SqlSelect) {
                SqlSelect sqlSelect = (SqlSelect) insertNode.getSource();
                //UDF的使用不仅仅在select.. from也会发生在group by...
                System.out.println(sqlSelect.getSelectList());
                if (sqlSelect.getFrom() instanceof SqlJoin) {
                    SqlJoin sqlJoin = (SqlJoin) sqlSelect.getFrom();
                    // insert into xxx select * from yyy join zzz on yyy.id = zzz.id
                    // 语句join时 yyy表和zzz表的连接器类型
                    SqlNode leftNode = sqlJoin.getLeft();
                    SqlNode rightNode = sqlJoin.getRight();
                    sourceTableList = StatementUtil.processJoinQuery(leftNode, sourceTableList);
                    sourceTableList = StatementUtil.processJoinQuery(rightNode, sourceTableList);
                } else if (sqlSelect.getFrom() instanceof SqlBasicCall) {
                    SqlBasicCall basicCall = (SqlBasicCall) sqlSelect.getFrom();
                    sourceTableList = StatementUtil.processJoinQuery(basicCall, sourceTableList);
                } else if (sqlSelect.getFrom() instanceof SqlIdentifier) {
                    SqlIdentifier identifier = (SqlIdentifier) sqlSelect.getFrom();
                    sourceTableList.add(identifier.toString());
                }
            }

            SqlIdentifier targetTableCall = (SqlIdentifier)insertNode.getTargetTable();

            String targetTable = targetTableCall.toString();
            // 如果源表存在join关系 需要递归收集sourceList
            cmd = insertNode.isOverwrite() ? StatementParser.SqlCommand.INSERT_OVERWRITE : StatementParser.SqlCommand.INSERT_INTO;
            operands = new String[]{targetTable, statement};
            sqlOperation = new SqlOperation(targetTable, SqlOperationEnum.INSERT, statement, null,  new ArrayList<>(sourceTableList));
        } else if (node instanceof SqlShowTables) {
            cmd = StatementParser.SqlCommand.SHOW_TABLES;
            operands = new String[]{statement};
        } else if (node instanceof SqlCreateTable) {
            SqlCreateTable createTableNode = (SqlCreateTable) node;
            cmd = StatementParser.SqlCommand.CREATE_TABLE;
            // 当用户切换方言 比如
            // CREATE CATALOG hiveCatalog WITH ( 'type' = 'hive', 'default-database' = 'default', 'hive-conf-dir' = '/home/lakehouse/flink/conf', 'hadoop-conf-dir' = '/home/lakehouse/flink/conf' );
            // USE CATALOG hiveCatalog;
            // set dialect = hive
            // create table a (xx string, yy string, zz string) with ( 'connector' = 'hive', 'scan.mode.inital' = 'inital' ),
            // create table c ( xx string, yy string, zz string) with ( 'connector' = 'hive', 'scan.mode.inital' = 'inital' )
            // set dialect = default;
            // create table b (asss string, cvvv string, bfs string)
            // insert into b select * from a left join b on a.id = b.id

            // 由于一个大作业里面会包含多个子作业 且多个子作业之中是乱序的
            // 化为子作业的依据在于 存在一个上游表对应多个下游表
            // 或者 一个上游表对应一个下游表
            // 所以这个问题 可以简化为 从源表集合到目标表集合的匹配问题
            // 即二分图匹配
            operands = new String[]{
                    createTableNode.getTableName().toString(),
                    statement
            };
            sqlOperation = new SqlOperation(createTableNode.getTableName().toString(), SqlOperationEnum.TABLE, statement, null, null);
        } else if (node instanceof SqlCreateDatabase){
            cmd = SqlCommand.CREATE_DATABASE;
            SqlCreateDatabase createDatabase = (SqlCreateDatabase) node;
            sqlOperation = new SqlOperation(createDatabase.getDatabaseName().toString(), SqlOperationEnum.DATABASE, statement , null ,null);
        } else if (node instanceof SqlDropTable) {
            cmd = StatementParser.SqlCommand.DROP_TABLE;
            SqlDropTable sqlDropTable = (SqlDropTable) node;
            operands = new String[]{sqlDropTable.getTableName().toString(), statement};
            sqlOperation = new SqlOperation(sqlDropTable.getTableName().toString(), SqlOperationEnum.TABLE, statement, null, null);
        } else if (node instanceof SqlAlterTable) {
            cmd = StatementParser.SqlCommand.ALTER_TABLE;
            SqlAlterTable sqlAlterTable = (SqlAlterTable) node;
            operands = new String[]{sqlAlterTable.getTableName().toString(), statement};
        } else if (node instanceof SqlCreateView) {

            SqlCreateView createViewNode = (SqlCreateView) node;
            if (!isFlinkParser) {
                cmd = StatementParser.SqlCommand.CREATE_VIEW;
                operands = new String[]{createViewNode.getViewName().toString(), statement, HIVE_SELECT_VIEW};
            } else {
                cmd = StatementParser.SqlCommand.CREATE_VIEW;
                operands = new String[]{createViewNode.getViewName().toString(), createViewNode.getQuery().toString()};
                throw new UnsupportedOperationException("无法从视图中解析连接器类型, Flink视图不支持");
            }
            List<String> sourceList = new ArrayList<>();
            SqlNode sqlNode = createViewNode.getQuery();
            SqlSelect select = (SqlSelect) sqlNode;
            SqlNode from = select.getFrom();
            if (select.getFrom() instanceof SqlJoin) {
                SqlJoin join = (SqlJoin) from;
                if (join.getLeft() instanceof  SqlIdentifier && join.getRight() instanceof  SqlIdentifier) {
                    SqlIdentifier left = (SqlIdentifier) join.getLeft();
                    SqlIdentifier right = (SqlIdentifier) join.getRight();
                    sourceList.add(left.toString());
                    sourceList.add(right.toString());
                } else if (join.getLeft() instanceof  SqlBasicCall && join.getRight() instanceof  SqlBasicCall) {
                    SqlBasicCall left = (SqlBasicCall) join.getLeft();
                    SqlBasicCall right = (SqlBasicCall) join.getRight();
                    SqlIdentifier leftNode = transfer(left);
                    SqlIdentifier rightNode = transfer(right);
                    sourceList.add(leftNode.getSimple());
                    sourceList.add(rightNode.getSimple());
                }
            } else if (from instanceof SqlBasicCall) {
                String tableName = StatementUtil.analysisSqlNode(from);
                sourceList.add(tableName);
            } else {
                SqlIdentifier identifier = (SqlIdentifier) from;
                sourceList.add(identifier.getSimple());
            }
            sqlOperation = new SqlOperation(createViewNode.getViewName().toString(), SqlOperationEnum.VIEW, statement, null, sourceList);
        } else if (node instanceof SqlDropView) {
            SqlDropView dropViewNode = (SqlDropView) node;
            cmd = StatementParser.SqlCommand.DROP_VIEW;
            operands = new String[]{dropViewNode.getViewName().toString(), statement};
            sqlOperation = new SqlOperation(dropViewNode.getViewName().toString(), SqlOperationEnum.VIEW, statement, null, null);
        } else if (node instanceof SqlShowViews) {
            cmd = StatementParser.SqlCommand.SHOW_VIEWS;
            operands = new String[]{statement};
        } else if (node instanceof SqlShowDatabases) {
            cmd = StatementParser.SqlCommand.SHOW_DATABASES;
            operands = new String[]{statement};
        } else if (node instanceof SqlCreateDatabase) {
            SqlCreateDatabase createDatabase = (SqlCreateDatabase) node;
            cmd = StatementParser.SqlCommand.CREATE_DATABASE;
            operands = new String[]{
                    createDatabase.getDatabaseName().toString(),
                    statement
            };
        } else if (node instanceof SqlCreateCatalog) {
            SqlCreateCatalog createCatalog = (SqlCreateCatalog) node;
            cmd = StatementParser.SqlCommand.CREATE_CATALOG;
            operands = new String[]{
                    createCatalog.getCatalogName().toString(),
                    statement
            };
            sqlOperation = new SqlOperation(createCatalog.getCatalogName().toString(), SqlOperationEnum.CATALOG, statement, null, null);
        } else if (node instanceof SqlDropDatabase) {
            SqlDropDatabase dropDatabase = (SqlDropDatabase) node;
            cmd = StatementParser.SqlCommand.DROP_DATABASE;
            operands = new String[]{
                    dropDatabase.getDatabaseName().toString(),
                    statement
            };
        } else if (node instanceof SqlAlterDatabase) {
            SqlAlterDatabase alterDatabase = (SqlAlterDatabase) node;
            cmd = StatementParser.SqlCommand.ALTER_DATABASE;
            operands = new String[]{
                    alterDatabase.getDatabaseName().toString(),
                    statement
            };
        } else if (node instanceof SqlShowCatalogs) {
            cmd = StatementParser.SqlCommand.SHOW_CATALOGS;
            operands = new String[]{statement};
        } else if (node instanceof SqlShowFunctions) {
            cmd = StatementParser.SqlCommand.SHOW_FUNCTIONS;
            operands = new String[]{statement};
        } else if (node instanceof SqlCreateFunction) {
            cmd = StatementParser.SqlCommand.CREATE_FUNCTIONS;
            SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) node;
            operands = new String[]{sqlCreateFunction.getFunctionIdentifier()[0], sqlCreateFunction.getFunctionClassName().toString()};
            sqlOperation = new SqlOperation(sqlCreateFunction.getFunctionClassName().toString(), SqlOperationEnum.FUNCTION, statement, null, null);
        } else if (node instanceof SqlShowCurrentCatalog) {
            cmd = StatementParser.SqlCommand.SHOW_CURRENT_CATALOG;
            operands = new String[]{statement};
        } else if (node instanceof SqlShowCurrentDatabase) {
            cmd = StatementParser.SqlCommand.SHOW_CURRENT_DATABASE;
            operands = new String[]{statement};
        } else if (node instanceof SqlUseCatalog) {
            SqlUseCatalog useCatalog = (SqlUseCatalog) node;
            cmd = StatementParser.SqlCommand.USE_CATALOG;
            operands = new String[]{useCatalog.getCatalogName().toString(), statement};
            sqlOperation = new SqlOperation(useCatalog.getCatalogName().toString(), SqlOperationEnum.CATALOG, statement, null, null);
        } else if (node instanceof SqlUseDatabase) {
            cmd = StatementParser.SqlCommand.USE;
            operands = new String[]{((SqlUseDatabase) node).getDatabaseName().toString()};
        } else if (node instanceof SqlRichDescribeTable) {
            cmd = StatementParser.SqlCommand.DESCRIBE_TABLE;
            String[] fullTableName = ((SqlRichDescribeTable) node).fullTableName();
            String escapedName =
                    Stream.of(fullTableName).map(s -> "`" + s + "`").collect(Collectors.joining("."));
            operands = new String[]{escapedName, statement};
        } else if (node instanceof SqlUpdate) {
            SqlUpdate sqlUpdate = (SqlUpdate) node;
            cmd = StatementParser.SqlCommand.UPDATE;
            operands = new String[]{String.valueOf(sqlUpdate.getTargetTable()), statement};
        } else if (node instanceof SqlSetOption) {
            SqlSetOption setNode = (SqlSetOption) node;
            String key = StringUtils.remove(setNode.getName().toString(), "'");
            String value = StringUtils.remove(setNode.getValue().toString(), "'");
            if (setNode.getValue() != null) {
                cmd = StatementParser.SqlCommand.SET;
                operands = new String[]{key, value};
            } else {
                cmd = StatementParser.SqlCommand.RESET;
                if (Asserts.equalsIgnoreCase(setNode.getName().toString(), "ALL")) {
                    operands = new String[0];
                } else {
                    operands = new String[]{key};
                }
            }
        } else {
            cmd = null;
            operands = new String[0];
        }

        if (cmd == null) {
            return Optional.empty();
        } else {
            return Optional.of(sqlOperation);
        }
    }

    private static Optional<SqlOperation> specialStrategy(String stmt, boolean isFlinkParser, org.apache.calcite.sql.parser.SqlParseException e) {
        ServiceLoader<SqlSpecailParseStrategy> loader = ServiceLoader.load(SqlSpecailParseStrategy.class);
        Iterator<SqlSpecailParseStrategy> iterator = loader.iterator();
        while (iterator.hasNext()) {
            SqlSpecailParseStrategy strategy = iterator.next();
            if (strategy.validate(stmt, isFlinkParser)) {
                return strategy.apply(stmt, isFlinkParser);
            }
        }
        throw new SqlParseException(String.format("Failed to parse statement: %s ", stmt), e);
    }


    private static SqlIdentifier transfer(SqlBasicCall basicCall) {
        List<SqlNode> sqlNodeList = Arrays.asList(basicCall.getOperands()).stream().filter(x -> x instanceof SqlIdentifier).collect(Collectors.toList());
        SqlNode asNode = sqlNodeList.get(0);
        SqlIdentifier asIdentifier = (SqlIdentifier) asNode;
        return asIdentifier;
    }
}
