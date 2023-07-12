package com.complone;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;

import java.util.*;
import java.util.stream.Collectors;

import static com.complone.Constants.DEFAULT_STRING;
import static com.complone.Constants.FLINK_TABLE_SQL_DIALECT;
import static com.complone.Constants.HIVE_STRING;
import static com.complone.Constants.*;

@Slf4j
public class StatementUtil {

    private static final String CREATE_TABLE = "create table ";
    private static final String CREATE_TABLE_IF_NOT_EXISTS = "create table if not exists ";
    private static final String AS = "as";
    private static final String BLANK = " ";


    /**
     * 从statement中提取表名
     *
     * @param statement
     * @return
     */
    public static String extractTableNamesFromStatement(String statement) {
        List<String> statementList = StatementParser.generateStatementList(StatementParser.formatStatement(statement));
        Boolean isFlinkParser = true;
        Set<String> tableSet = new HashSet<>();
        String sql;
        for (int i = 0; i < statementList.size(); i++) {
            sql = statementList.get(i);
            if (StringUtils.containsIgnoreCase(sql, FLINK_TABLE_SQL_DIALECT) && StringUtils.containsIgnoreCase(sql, HIVE_STRING)) {
                isFlinkParser = false;
            } else if (StringUtils.containsIgnoreCase(sql, FLINK_TABLE_SQL_DIALECT) && StringUtils.containsIgnoreCase(sql, DEFAULT_STRING)) {
                isFlinkParser = true;
            }
            SqlParser.Config config = StatementParser.createSqlParserConfig(isFlinkParser);
            SqlParser sqlParser = SqlParser.create(sql, config);
            SqlNodeList sqlNodes;
            try {
                sqlNodes = sqlParser.parseStmtList();
                sqlNodes.getList().stream().forEach(node -> {
                    if (node instanceof SqlCreateTable) {
                        tableSet.add(((SqlCreateTable) node).getTableName().toString());
                    } else if (node instanceof SqlCreateView) {
                        tableSet.add(((SqlCreateView) node).getViewName().toString());
                        extractSourceTableInSql(((SqlCreateView) node).getQuery(), false, tableSet);
                    } else if (node instanceof SqlInsert) {
                        SqlInsert insertNode = (SqlInsert) node;
                        extractSourceTableInSql(insertNode, false, tableSet);
                        if (insertNode.getTargetTable() instanceof SqlIdentifier) {
                            tableSet.add(((SqlIdentifier) insertNode.getTargetTable()).toString());
                        }
                    } else if (node instanceof SqlSelect) {
                        extractSourceTableInSql(node, false, tableSet);
                    }
                });
            } catch (Exception e) {
                if (!isFlinkParser) {
                    specialHiveCreateTable(sql.toLowerCase().trim(), tableSet);
                }
            }
        }
        return tableSet.isEmpty() ? "" : String.join(",", tableSet);
    }

    private static void specialHiveCreateTable(String sql, Set<String> tableSet) {
        //hivesql 提取create table if not exists xxx () as select
        if (StringUtils.containsIgnoreCase(sql, CREATE_TABLE) && StringUtils.containsIgnoreCase(sql, AS)) {
            try {
                if (sql.startsWith(CREATE_TABLE_IF_NOT_EXISTS)) {
                    tableSet.add(StringUtils.substringBetween(sql, CREATE_TABLE_IF_NOT_EXISTS, BLANK).trim());
                } else {
                    tableSet.add(StringUtils.substringBetween(sql, CREATE_TABLE, BLANK).trim());
                }
            } catch (Exception e) {

            }
        }
    }


    private static void extractSourceTableInSql(SqlNode sqlNode, Boolean fromOrJoin, Set<String> tables) {
        if (sqlNode == null) {
            return;
        } else {
            switch (sqlNode.getKind()) {
                case SELECT:
                    SqlSelect selectNode = (SqlSelect) sqlNode;
                    extractSourceTableInSql(selectNode.getFrom(), true, tables);
                    List<SqlNode> selectList = selectNode.getSelectList().getList().stream().filter(
                            t -> t instanceof SqlCall
                    ).collect(Collectors.toList());
                    for (SqlNode select : selectList) {
                        extractSourceTableInSql(select, false, tables);
                    }
                    extractSourceTableInSql(selectNode.getWhere(), false, tables);
                    extractSourceTableInSql(selectNode.getHaving(), false, tables);
                    break;
                case JOIN:
                    extractSourceTableInSql(((SqlJoin) sqlNode).getLeft(), true, tables);
                    extractSourceTableInSql(((SqlJoin) sqlNode).getRight(), true, tables);
                    break;
                case AS:
                    if (((SqlCall) sqlNode).operandCount() >= 2) {
                        extractSourceTableInSql(((SqlCall) sqlNode).operand(0), fromOrJoin, tables);
                    }
                    break;
                case IDENTIFIER:
                    if (fromOrJoin) {
                        tables.add(((SqlIdentifier) sqlNode).toString());
                    }
                    break;
                default:
                    if (sqlNode instanceof SqlCall) {
                        for (SqlNode node : ((SqlCall) sqlNode).getOperandList()) {
                            extractSourceTableInSql(node, false, tables);
                        }
                    }
            }
        }
    }


    /**
     * 查找union all 或者 多层嵌套子查询的sql并解析基表 返回基表集合
     *
     * @param basicCall
     */
    public static Set<String> queryDeepSql(SqlBasicCall basicCall, Set<String> baseTableList) {
        SqlNode[] sqlNodesList = basicCall.getOperands();
        if (sqlNodesList.length < 2) {
            return baseTableList;
        }

        //如果是union all语句
        // 彼此之间都是平行的所以第一个元素一般是拆分单个select后的sql
        // 第二个元素是拆分出来的sql
        SqlNode nextNode = sqlNodesList[1];
        SqlNode prevNode = sqlNodesList[0];

        //如果子查询当中存在别名 特别处理
        if (prevNode instanceof  SqlIdentifier) {
            SqlIdentifier node = (SqlIdentifier) prevNode;
            baseTableList.add(node.toString());
        } else if (nextNode instanceof SqlIdentifier && prevNode instanceof SqlSelect) {
            SqlSelect prevSelect = (SqlSelect) prevNode;
            SqlNode node = prevSelect.getFrom();
            if (node instanceof SqlJoin) {
                SqlJoin join = (SqlJoin) node;
                SqlNode left = join.getLeft();
                SqlNode right = join.getRight();
                String leftSource = analysisSqlNode(left);
                String rightSource = analysisSqlNode(right);

                baseTableList.add(leftSource);
                baseTableList.add(rightSource);
            } else if (node instanceof SqlBasicCall){
                baseTableList = queryDeepSql((SqlBasicCall) node, baseTableList);
            } else {
                String finalTable = node.toString();
                baseTableList.add(finalTable);
            }
            return baseTableList;
        }

        if (prevNode instanceof SqlBasicCall) {
            SqlSelect nextSelect = (SqlSelect) nextNode;
            baseTableList.add(nextSelect.getFrom().toString());
            queryDeepSql((SqlBasicCall) prevNode, baseTableList);
        } else if (prevNode instanceof SqlSelect) {
            SqlSelect prevSelect = (SqlSelect) prevNode;
            SqlSelect nextSelect = (SqlSelect) nextNode;
            String prevTableName = prevSelect.getFrom().toString();
            String nextTableName = nextSelect.getFrom().toString();
            baseTableList.add(prevTableName);
            baseTableList.add(nextTableName);
            return baseTableList;
        }
        return baseTableList;
    }

    public static String analysisSqlNode(SqlNode node) {
        if (! (node instanceof SqlBasicCall)) {
            return node.toString();
        }
        SqlBasicCall leftCall = (SqlBasicCall) node;
        System.out.println(leftCall.getOperands());
        String source = "";
        for (SqlNode sql : leftCall.getOperands()) {

            if (!sql.toString().contains("AS")) {
                source = sql.toString();
                break;
            }
        }

        return source;
    }


    /**
     * 切割并重组 statement
     *
     * @param statement
     * @return
     */
    public static List<String> spiltAndRegroupStatement(String statement) {
        List<String> list = SqlStatemenetParser.parseSqlStatement(statement);
        return list;
    }


    public static Optional<SqlOperation> specialStrategy(String stmt, boolean isFlinkParser, org.apache.calcite.sql.parser.SqlParseException e) {
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


    public static Optional<SqlOperation> processSqlOperationException(String statement, boolean isFlinkParser, org.apache.calcite.sql.parser.SqlParseException e) {
        Optional<SqlOperation> sqlOperation = specialStrategy(statement, isFlinkParser, e);
        if (sqlOperation.isPresent()) {
            return sqlOperation;
        } else {
            throw new UnsupportedOperationException(String.format("当前SQL语句解析失败!, 请检查是否符合sql规范, 当前sql语句: %s", statement));
        }
    }

    /**
     * 处理join查询是否取别名的情况
     * @param node
     * @param sourceTableList
     * @return
     */
    public static Set<String> processJoinQuery(SqlNode node, Set<String> sourceTableList) {
        if (node instanceof SqlIdentifier) {
            ImmutableList<String> leftNodes = ((SqlIdentifier) node).names;
            sourceTableList.add(leftNodes.get(0));

        } else if (node instanceof SqlBasicCall) {
            SqlBasicCall basicCall = (SqlBasicCall) node;
            Set<String> resultSet = StatementUtil.queryDeepSql(basicCall, new HashSet<>(sourceTableList));
            return resultSet;
        } else {
            return sourceTableList;
        }
        return sourceTableList;
    }

    /**
     * 判断目标表与源表是否重复
     * @param deepSourceSet
     * @param sourceStr
     * @param targetTable
     * @param insertNode
     * @return
     */
    public static TableLineager targetRepeatJudge(List<String> sourceList, String targetTable, TableLineager insertNode) {
        String sourceStr = sourceList.toString();
        if (sourceList.contains(targetTable)) {
            insertNode.setTableName(sourceStr.substring(1, sourceStr.length()-1));
        } else {
            insertNode.setTableName(sourceStr.substring(1, sourceStr.length()-1) + "," + targetTable);
        }
        return insertNode;
    }
}
