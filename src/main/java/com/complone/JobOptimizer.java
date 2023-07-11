package com.complone;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.ddl.*;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.complone.Constants.CONNECTOR_STRING;
import static com.complone.Constants.HIVE_STRING;
import static com.complone.Constants.REDEFINE_CREATE_TABLE_NODE;
import static com.complone.Constants.TYPE_STRING;
import static com.complone.Constants.FINAL_INSERT_NODE;
/**
 * 本类存在两种用法 一种是在易用性优化里遍历语法树时标记源表
 * 一种是记录源表集合到目标表集合的依赖关系
 */
public class JobOptimizer {

    /**
     * 解析的规则为 解析出与insert into有关的语句
     * 无依赖的语句放到一个单独的集合维护(这个集合需要打标hive和非hive的语句集)
     * 函数集合单独维护, 剩下的只有切换方言，创建catalog以及视图的上下文
     * 视图可以暂时不支持，创建catalog可以单独收集(根据顺序收集)
     * 优先支持切换方言
     * @param operation
     * @param tableLineageMap
     * @param isFlinkParser
     * @param functionList
     * @return
     */
    public static Map<String, TableLineager> analysisLineager(SqlOperation operation, Map<String, TableLineager> tableLineageMap, Boolean isFlinkParser, Map<String, String> functionList, AtomicInteger insertSqlCount) {
        //保存子节点到父节点的映射
        String statement = operation.getStatement();
        SqlParser.Config config = StatementParser.createSqlParserConfig(isFlinkParser);
        SqlParser sqlParser = SqlParser.create(statement, config);
        SqlNodeList sqlNodes = null;
        SqlNode node = null;
        try {

            sqlNodes = sqlParser.parseStmtList();
            if (sqlNodes.size() != 1) {
                throw new SqlParseException("Only single statement is supported now");
            }
            node = sqlNodes.get(0);

        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            Optional<SqlOperation> sqlOperation = StatementUtil.specialStrategy(statement, isFlinkParser, e);
            if (sqlOperation.isPresent()) {
                return tableLineageMap;
            } else {
                throw new UnsupportedOperationException("当前SQL语句解析失败!, 请检查是否符合sql规范");
            }
        }

        if (node instanceof SqlOrderBy) {
            // order by的情况下 join需要从order by 节点获取
            SqlOrderBy sqlOrderBy = (SqlOrderBy) node;
            SqlSelect sqlSelect = (SqlSelect) sqlOrderBy.query;
            Set<String> fromList = new HashSet<>();
            if (sqlSelect.getFrom() instanceof SqlJoin) {
                SqlNode leftNode = ((SqlJoin) sqlSelect.getFrom()).getLeft();
                SqlNode rightNode = ((SqlJoin) sqlSelect.getFrom()).getRight();
                fromList = StatementUtil.processJoinQuery(leftNode, fromList);
                fromList = StatementUtil.processJoinQuery(rightNode, fromList);
            } else {
                fromList.add(sqlSelect.getFrom().toString());
            }
            String sourceListStr = fromList.toString();
            insertSqlCount.getAndIncrement();
            TableLineager tableLineager = new TableLineager();
            tableLineager.setStatement(statement);
            tableLineager.setTableName(sourceListStr.substring(1, sourceListStr.length() - 1));
            tableLineager.setConnector("select");
            tableLineageMap.put(FINAL_INSERT_NODE + insertSqlCount.get(), tableLineager);

        } else if (node.getKind().belongsTo(SqlKind.QUERY)) {
            Set<String> fromList = new HashSet<>();
            SqlSelect sqlSelect = (SqlSelect) node;
            if (sqlSelect.getFrom() instanceof SqlJoin) {
                SqlJoin sqlJoin = (SqlJoin) sqlSelect.getFrom();
                SqlNode leftNode = sqlJoin.getLeft();
                SqlNode rightNode = sqlJoin.getRight();
                fromList = StatementUtil.processJoinQuery(leftNode, fromList);
                fromList = StatementUtil.processJoinQuery(rightNode, fromList);
            } else if (sqlSelect.getFrom() instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) sqlSelect.getFrom();
                fromList.add(basicCall.operands[0].toString());
            } else if (sqlSelect.getFrom() instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) sqlSelect.getFrom();
                fromList.add(identifier.getSimple());
            }


            //查询暂时不需要反向回溯 如果需要反向回溯，则补全next信息
            String sourceListStr = fromList.toString();
            insertSqlCount.getAndIncrement();
            TableLineager tableLineager = new TableLineager();
            tableLineager.setStatement(statement);
            tableLineager.setTableName(sourceListStr.substring(1, sourceListStr.length() - 1));
            tableLineager.setConnector("select");
            tableLineageMap.put(FINAL_INSERT_NODE + insertSqlCount.get(), tableLineager);
        } else if (node instanceof RichSqlInsert) {
            RichSqlInsert insertNode = (RichSqlInsert) node;
            Set<String> deepSourceSet = new HashSet<>();
            if (insertNode.getSource() instanceof SqlSelect) {
                SqlSelect sqlSelect = (SqlSelect) insertNode.getSource();
                if (sqlSelect.getFrom() instanceof SqlJoin) {
                    SqlJoin sqlJoin = (SqlJoin) sqlSelect.getFrom();
                    SqlNode leftNode = sqlJoin.getLeft();
                    SqlNode rightNode = sqlJoin.getRight();
                    deepSourceSet = StatementUtil.processJoinQuery(leftNode, deepSourceSet);
                    deepSourceSet = StatementUtil.processJoinQuery(rightNode, deepSourceSet);
                    // insert into xxx select * from yyy join zzz on yyy.id = zzz.id
                } else if (sqlSelect.getFrom() instanceof SqlBasicCall) {
                    SqlBasicCall basicCall = (SqlBasicCall) sqlSelect.getFrom();
                    //需要判断多union子句的拆分问题 递归深度为operands不为2
                    deepSourceSet = StatementUtil.queryDeepSql(basicCall, deepSourceSet);

                } else if (sqlSelect.getFrom() instanceof SqlIdentifier) {
                    SqlIdentifier identifier = (SqlIdentifier) sqlSelect.getFrom();
                    deepSourceSet.add(identifier.toString());
                }
                deepSourceSet = deepSourceSet.stream().filter(StringUtils::isNotBlank).collect(Collectors.toSet());
            }
            List<String> sourceList = operation.getSourceTableList();
            String targetTable = operation.getName();
            // 解析insert into 语句
            // 开始记录表的血缘关系
            // insert into xxx select * from yyy join zzz on yyy.id = zzz.id
            // 其中statement代表 表第一次出现的statement语句
            // 这样方便从子节点查找到父节点的时候向上递归
            // 比如TableLineager中 xxx 归属于 create table xxx ,yyy和zzz归属于 create table yyy
            // 和create table zzz。
            // 由于sql执行顺序是乱序的 也就意味着 上下文的遍历顺序可能会被打乱
            // 所以节点的关系为 zzz(create table zzz) -> xxx (create table xxx)
            // yyy(create table yyy) -> xxx (create table xxx)
            // 如果是insert 语句 connector默认标记为insert，遇到insert语句时，之前保存过的
            // 语句上下文会根据insert语句胡表名列表(next) 判断是否需要补全相关胡源表
            List<TableLineager> nextList = new ArrayList<>();
            //构造insert节点
            TableLineager insertFinalNode = new TableLineager();
            insertFinalNode.setConnector("insert");
            insertFinalNode.setStatement(statement);
            insertFinalNode.setFunctionList(functionList);

            //如果子查询存在union all的合并sql情况
            if (sourceList.size() <= 1 || deepSourceSet.size() >= 2){
                sourceList = new ArrayList<>(deepSourceSet);
                if (sourceList.size()> 0) {
                    insertFinalNode = StatementUtil.targetRepeatJudge(sourceList, targetTable, insertFinalNode);
                }
                //在union all的情况下 由于多个sql合并 所以别名作为的表为影子表,即不是真实存在的表
                // 但是如果insert into ..select当中创建的源表是视图，这种情况下 视图会存在多级血缘
                // 但sql切分算法这类dfs的目的在于, 我们需要从顺序上下文中收集到的所有视图，表信息传递给insert into语句
                // 然后再重建阶段，通过insert into携带的信息反向推到收集到的相关表信息
                // 所以insert into的tableName属性有些独特，他的tableName属性代表着这条链路上所有表的集合
                // 故这里如果存在sourceList为1的情况，
                // 必定是create view as select ... from a 的这种情况(后面left join b也一样)
                else if (sourceList.size() == 1){
                    insertFinalNode = StatementUtil.targetRepeatJudge(sourceList, targetTable, insertFinalNode);
                }
                for (String source : deepSourceSet) {
                    nextList.add(insertFinalNode);
                    TableLineager tableLineager = tableLineageMap.get(source);
                    //如果为空 则说明表已经预先定义过，通过 <database>.<table> 即可访问
                    if (Objects.isNull(tableLineager)) {
                        //标记为重定义
                        tableLineager = new TableLineager();
                        tableLineager.setTableName(source);
                        tableLineager.setConnector(REDEFINE_CREATE_TABLE_NODE);
                    }
                    tableLineager.setNext(nextList);
                    //更新源表节点后继
                    tableLineageMap.put(source, tableLineager);
                }

            }
            //连表查询 目前只支持两表查询
            else if(sourceList.size() == 2) {
                insertFinalNode = StatementUtil.targetRepeatJudge(sourceList, targetTable, insertFinalNode);
                String leftTable = sourceList.get(0);
                TableLineager leftParentNode = tableLineageMap.get(leftTable);
                leftParentNode.setNext(nextList);
                //连表查询节点后继 目前只允许两表连查
                nextList.add(insertFinalNode);
                if (sourceList.size() > 1) {
                    String rightTable = sourceList.get(1);
                    TableLineager rightParentNode = tableLineageMap.get(rightTable);
                    rightParentNode.setNext(nextList);
                    tableLineageMap.put(rightTable, rightParentNode);
                }
                //更新源表的后继节点为insert语句,便于在重建SQL阶段还原上下文
                tableLineageMap.put(leftTable, leftParentNode);
            }

            //更新目标表后继节点
            TableLineager next = tableLineageMap.get(targetTable);
            next.setNext(nextList);
            //如果这张表已经是target表了 那么只要不使用drop语句 就是物理表
            // 如果使用drop语句,则需要判定上下文中是否存在这张表的依赖关系
            //假设如果一张表只是暂时作为临时表 之后都作为物理表建议区分开来
            tableLineageMap.put(targetTable, next);
            insertSqlCount.getAndIncrement();
            tableLineageMap.put(FINAL_INSERT_NODE + insertSqlCount.get(), insertFinalNode);
        } else if (node instanceof SqlCreateCatalog){
            SqlCreateCatalog createCatalog = (SqlCreateCatalog) node;
            Map<String, String> optionMap = new HashMap<>();
            createCatalog.getPropertyList().forEach(sqlNode -> {
                SqlTableOption option = (SqlTableOption) sqlNode;
                if (Asserts.isNotBlank(option.getValueString())) {
                    optionMap.put(option.getKeyString(), option.getValueString());
                }
            });
            TableLineager catalog = new TableLineager();
            catalog.setStatement(statement);
            catalog.setTableName(createCatalog.getCatalogName().toString());
            catalog.setConnector(optionMap.get(TYPE_STRING));
            catalog.setIsCatalog(true);
            tableLineageMap.put(createCatalog.getCatalogName().toString(), catalog);
            return tableLineageMap;
        } else if(node instanceof SqlDropView) {
            SqlDropView sqlDropView = (SqlDropView) node;
            //目前仅考虑hive视图
            TableLineager tableLineager = new TableLineager();
            tableLineager.setNext(new ArrayList<>());
            //在建立DL语句之前创建标识
            tableLineager.setDrop(true);
            tableLineageMap.put(sqlDropView.getViewName().toString(), tableLineager);
        } else if (node instanceof SqlCreateView) {
            SqlCreateView createViewNode = (SqlCreateView) node;
            SqlSelect sqlSelect = (SqlSelect) createViewNode.getQuery();
            if (sqlSelect.getFrom() instanceof SqlJoin) {
                SqlJoin sqlJoin = (SqlJoin) sqlSelect.getFrom();
                List<String> sourceList = operation.getSourceTableList();

            } else if (sqlSelect.getFrom() instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) sqlSelect.getFrom();
            } else if (sqlSelect.getFrom() instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) sqlSelect.getFrom();
            }
            //目前仅考虑hive视图
            String sourceTableName = createViewNode.getViewName().toString();
            TableLineager tableLineager = new TableLineager();
            //判断是否需要移除表的操作
            if (Objects.nonNull(tableLineageMap.get(sourceTableName))) {
                tableLineager = tableLineageMap.get(sourceTableName);
            }
            //先记录表血缘之间的关联关系 然后在insert 语句遍历的时候补全后继节点的信息
            tableLineager.setConnector(HIVE_STRING);
            tableLineager.setTableName(sourceTableName);
            tableLineager.setStatement(statement);
            tableLineager.setVirualTableList(operation.getSourceTableList());
            tableLineager.setIsView(true);
            tableLineageMap.put(sourceTableName, tableLineager);
        } else if (node instanceof SqlCreateTable) {
            SqlCreateTable createTable = (SqlCreateTable) node;
            String sourceTableName = createTable.getTableName().toString();
            TableLineager tableLineager = new TableLineager();
            //判断是否需要移除表的操作
            if (Objects.nonNull(tableLineageMap.get(sourceTableName))) {
                tableLineager = tableLineageMap.get(sourceTableName);
            }
            List<SqlNode> sqlNodeList = createTable.getPropertyList().getList();
            List<SqlTableOption> list = sqlNodeList.stream().map(x -> (SqlTableOption)x).collect(Collectors.toList());
            list = list.stream().filter(x -> x.getKeyString().contains(CONNECTOR_STRING)).collect(Collectors.toList());
            String connector = "";
            if (list.size() > 0) {
                connector = list.get(0).getValueString();
            } else {
                connector = "hive";
            }
            //先记录表血缘之间的关联关系 然后在insert 语句遍历的时候补全后继节点的信息

            tableLineager.setConnector(connector);
            tableLineager.setTableName(sourceTableName);
            tableLineager.setStatement(statement);
            tableLineageMap.put(sourceTableName, tableLineager);

        } else if (node instanceof SqlCreateFunction) {
            SqlCreateFunction createFunction = (SqlCreateFunction) node;
            TableLineager functionNode = tableLineageMap.get(createFunction.getFunctionIdentifier()[0]);
            if (functionNode == null) {
                functionNode = new TableLineager();
                functionNode.setTableName(createFunction.getFunctionIdentifier()[0]);
                functionNode.setStatement(statement);
                functionNode.setConnector("function");
            }
            functionList.put(functionNode.getTableName(), functionNode.getStatement());
        } else if (node instanceof SqlDropTable) {
            SqlDropTable dropTable = (SqlDropTable) node;
            TableLineager tableLineager = new TableLineager();
            tableLineager.setNext(new ArrayList<>());
            //在建立DL语句之前创建标识
            tableLineager.setDrop(true);
            tableLineageMap.put(dropTable.getTableName().toString(), tableLineager);
        } else if (node instanceof SqlUseDatabase) {
            SqlUseDatabase use = (SqlUseDatabase) node;
            String database = use.getDatabaseName().toString();
            TableLineager tableLineager = new TableLineager();
            tableLineager.setIsUse(true);
            tableLineageMap.put(database, tableLineager);
        } else if (node instanceof  SqlUseCatalog) {
            SqlUseCatalog use = (SqlUseCatalog) node;
            String catalogName = use.getCatalogName().toString();
            TableLineager tableLineager = tableLineageMap.get(catalogName);
            tableLineager.setIsUse(true);
            tableLineageMap.put(catalogName, tableLineager);
        }
        return tableLineageMap;
    }
}
