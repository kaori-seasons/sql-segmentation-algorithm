package com.complone;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateSpecialStrategy implements SqlSpecailParseStrategy {

    protected static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
    protected Pattern tablePattern;
    protected Pattern tableExistPattern;
    protected Pattern viewPattern;
    protected Pattern viewExistPattern;
    protected Pattern functionPattern;
    protected Pattern beforePattern;
    protected Pattern setPattern;
    private SpecialStrategyEnums strategyEnums;
    public static final String AS_STRING = "as";
    private String tableName;
    private String sql;
    private List<String> functionList;

    static final CreateSpecialStrategy strategy = new CreateSpecialStrategy();

    public CreateSpecialStrategy() {
        this.tableExistPattern = Pattern.compile(
                "(?i)create\\s+table\\s+if\\s+not\\s+exists\\s+(\\S+)\\s*", DEFAULT_PATTERN_FLAGS);
        this.viewExistPattern = Pattern.compile(
                "(?i)create\\s+view\\s+if\\s+not\\s+exists\\s+(\\S+)\\s*", DEFAULT_PATTERN_FLAGS);
        this.tablePattern = Pattern.compile(
                "(?i)create\\s+table\\s+(\\S+)\\s*", DEFAULT_PATTERN_FLAGS);
        this.viewPattern = Pattern.compile(
                "(?i)create\\s+view\\s+(\\S+)\\s*", DEFAULT_PATTERN_FLAGS);
        //create function正则匹配
        this.functionPattern = Pattern.compile(
                "(?i)create\\s+function\\s+if\\s+not\\s+exists\\s+as\\s+(\\S+)\\s*", DEFAULT_PATTERN_FLAGS);
        this.beforePattern = Pattern.compile(".*(?=create)", Pattern.CASE_INSENSITIVE);
    }


    @Override
    public Boolean validate(String statement, boolean isFlinkParser) {
        Matcher tableExistMatcher = tableExistPattern.matcher(statement.trim());
        Matcher viewExistMatcher = viewExistPattern.matcher(statement.trim());
        Matcher tableMatcher = tablePattern.matcher(statement.trim());
        Matcher viewMatcher = viewPattern.matcher(statement.trim());
        Matcher functionMatcher = functionPattern.matcher(statement.trim());
        Matcher beforeMatcher = beforePattern.matcher(statement.trim());

        if (beforeMatcher.find()) {
            if (StringUtils.isNotBlank(beforeMatcher.group(0))) {
                return false;
            }
        }

        if (tableExistMatcher.find()) {
            tableName = tableExistMatcher.group(1);
            strategyEnums = SpecialStrategyEnums.CREATE_TABLE;
            return true;
        } else if (tableMatcher.find()) {
            tableName = tableExistMatcher.group(1);
            strategyEnums = SpecialStrategyEnums.CREATE_TABLE;
            return true;
        } else if (viewExistMatcher.find()) {
            String viewName = viewExistMatcher.group(1);
            String selectSql = statement.split(viewName)[1];
            selectSql = StringUtils.removeStartIgnoreCase(selectSql.trim(), AS_STRING);
            SqlStatemenetParser.parseStatement(selectSql, isFlinkParser);
            return true;
        } else if (viewMatcher.find()) {
            strategyEnums = SpecialStrategyEnums.CREATE_VIEW;
            return true;
        } else if (functionMatcher.find()) {
            //策略模式只是针对单条SQL的顺序遍历 应该把添加的逻辑交给
            strategyEnums = SpecialStrategyEnums.CREATE_FUNCTION;
        }
        return false;
    }

    @Override
    public Optional<SqlOperation> apply(String statement, boolean isFlinkParser) {
        switch (strategyEnums) {
            case CREATE_TABLE:
                SqlOperation sqlOperation = new SqlOperation(tableName, SqlOperationEnum.TABLE, statement, statement, null);
                return Optional.of(sqlOperation);
            case CREATE_VIEW:
                return Optional.empty();
            case CREATE_FUNCTION:
                return Optional.empty();
        }
        return Optional.empty();
    }
}
