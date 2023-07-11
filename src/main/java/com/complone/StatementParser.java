package com.complone;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.complone.Constants.*;

public class StatementParser {
    /**
     * @description: SQL语句格式化这块需要兼容不同写法的SQL语句
     */
    public static String formatStatement(String statement) {
        if (Asserts.isBlank(statement)) {
            return null;
        }
        StringBuilder builder = new StringBuilder(statement.length());
        char[] chars = statement.toCharArray();
        boolean existSingleQuote = false;
        boolean existDoubleQuote = false;
        for (int i = 0; i < chars.length; i++) {
            StringBuilder stringBuilder = new StringBuilder(2);
            if (i >= 1) {
                stringBuilder.append(chars[i - 1]);
                stringBuilder.append(chars[i]);
            }
            if (Asserts.equalsIgnoreCase(Constants.ANNOTATE_SIGN, stringBuilder.toString())) {
                if (existDoubleQuote) {
                    builder.append(chars[i]);
                } else if (existSingleQuote) {
                    builder.append(chars[i]);
                } else {
                    builder.deleteCharAt(builder.length() - 1);
                    while (chars[i] != NEWLINE_SIGN) {
                        if (i == chars.length - 1) {
                            break;
                        }
                        i++;
                    }
                    builder.append(NEWLINE_SIGN);
                }
            } else if (chars[i] == DOUBLE_QUOTE_SIGN && BACK_SLASH_SIGN != chars[i] && !existSingleQuote) {
                existDoubleQuote = !existDoubleQuote;
                builder.append(chars[i]);
            } else if (chars[i] == SINGLE_QUOTE_SIGN && BACK_SLASH_SIGN != chars[i] && !existDoubleQuote) {
                existSingleQuote = !existSingleQuote;
                builder.append(chars[i]);
            } else {
                builder.append(chars[i]);
            }
        }
        return replaceEscapedCharacter(builder.toString());
    }

    public static String replaceEscapedCharacter(String statement) {
        return statement.replaceAll(ESCAPED_LINE_SIGN, " ")
                .replaceAll(NEWLINE_STRING_SIGN, " ")
                .replace(INDENT_STRING_SIGN, " ")
                .trim();
    }

    /**
     * sql语句切分为列表
     *
     * @param statement
     * @return
     */
    public static List<String> generateStatementList(String statement) {
        List<String> statementList = new ArrayList<>();
        boolean existSingleQuote = false;
        boolean existDoubleQuote = false;
        int leftBracketCounter = 0;
        StringBuilder builder = new StringBuilder();
        char[] chars = statement.toCharArray();
        int index = 0;
        for (char charValue : chars) {
            char flag = 0;
            if (index > 0) {
                flag = chars[index - 1];
            }
            if (charValue == SEMICOLON_SIGN) {
                if (existDoubleQuote) {
                    builder.append(charValue);
                } else if (existSingleQuote) {
                    builder.append(charValue);
                } else if (leftBracketCounter > 0) {
                    builder.append(charValue);
                } else if (Asserts.isNotBlank(builder.toString())) {
                    statementList.add(builder.toString());
                    builder = new StringBuilder();
                }
            } else if (charValue == DOUBLE_QUOTE_SIGN && BACK_SLASH_SIGN != flag && !existSingleQuote) {
                existDoubleQuote = !existDoubleQuote;
                builder.append(charValue);
            } else if (charValue == SINGLE_QUOTE_SIGN && BACK_SLASH_SIGN != flag && !existDoubleQuote) {
                existSingleQuote = !existSingleQuote;
                builder.append(charValue);
            } else if (charValue == LEFT_BRACKETS_SIGN && !existSingleQuote && !existDoubleQuote) {
                leftBracketCounter++;
                builder.append(charValue);
            } else if (charValue == RIGHT_BRACKETS_SIGN && !existSingleQuote && !existDoubleQuote) {
                leftBracketCounter--;
                builder.append(charValue);
            } else {
                builder.append(charValue);
            }
            index++;
        }
        if (Asserts.isNotBlank(builder.toString())) {
            statementList.add(builder.toString());
        }
        return statementList;
    }


    public static SqlNodeList parseSqlNodeList(String stmt, boolean isBlinkPlanner) {
        SqlParser.Config config = createSqlParserConfig(isBlinkPlanner);
        SqlParser sqlParser = SqlParser.create(stmt, config);
        SqlNodeList sqlNodes = null;
        try {
            sqlNodes = sqlParser.parseStmtList();
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            return sqlNodes;
        }
        if (sqlNodes.size() != 1) {
            throw new SqlParseException("Only single statement is supported now");
        }
        return sqlNodes;
    }


    public static SqlParser.Config createSqlParserConfig(boolean isFlinkParser) {
        if (isFlinkParser) {
            return SqlParser
                    .configBuilder()
                    .setParserFactory(FlinkSqlParserImpl.FACTORY)
                    .setConformance(FlinkSqlConformance.DEFAULT)
                    .setLex(Lex.JAVA)
                    .setIdentifierMaxLength(256)
                    .build();
        } else {
            return SqlParser
                    .configBuilder()
                    .setParserFactory(FlinkHiveSqlParserImpl.FACTORY)
                    .setConformance(FlinkSqlConformance.HIVE)
                    .setLex(Lex.JAVA)
                    .build();
        }
    }

    /**
     * isFlinkParser为true 则为default
     * @param statement
     * @param isFlinkParser
     * @return
     */
    public static boolean judgeFlinkParser(String statement, Boolean isFlinkParser) {
        if (StringUtils.contains(statement, FLINK_TABLE_SQL_DIALECT) && StringUtils.contains(statement, HIVE_STRING)) {
            isFlinkParser = false;
        } else if (StringUtils.contains(statement, FLINK_TABLE_SQL_DIALECT) && StringUtils.contains(statement, DEFAULT_STRING)) {
            isFlinkParser = true;
        }
        return isFlinkParser;
    }
    // --------------------------------------------------------------------------------------------

    private static final Function<String[], Optional<String[]>> NO_OPERANDS =
            (operands) -> Optional.of(new String[0]);

    private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

    /**
     * Supported SQL commands.
     */
    public enum SqlCommand {
        SELECT,

        INSERT_INTO,

        INSERT_OVERWRITE,

        CREATE_TABLE,

        CREATE_CATALOG,

        ALTER_TABLE,

        DROP_TABLE,

        CREATE_VIEW,

        DROP_VIEW,

        CREATE_DATABASE,

        ALTER_DATABASE,

        DROP_DATABASE,

        USE_CATALOG,

        USE,

        SHOW_CATALOGS,

        SHOW_DATABASES,

        SHOW_TABLES,

        SHOW_FUNCTIONS,

        CREATE_FUNCTIONS,

        EXPLAIN,

        LOAD_MODULE,

        USE_MODULES,

        DESCRIBE_TABLE,

        RESET,

        UPDATE,

        WITH,

        TRUNCATE_TABLE,

        // the following commands are not supported by SQL parser but are needed by users

        SET(
                "SET",
                // `SET` with operands can be parsed by SQL parser
                // we keep `SET` with no operands here to print all properties
                NO_OPERANDS),

        // the following commands will be supported by SQL parser in the future
        // remove them once they're supported

        // FLINK-17396
        SHOW_MODULES(
                "SHOW\\s+MODULES",
                NO_OPERANDS),

        // FLINK-17111
        SHOW_VIEWS(
                "SHOW\\s+VIEWS",
                NO_OPERANDS),

        // the following commands are not supported by SQL parser but are needed by JDBC driver
        // these should not be exposed to the user and should be used internally

        SHOW_CURRENT_CATALOG(
                "SHOW\\s+CURRENT\\s+CATALOG",
                NO_OPERANDS),

        SHOW_CURRENT_DATABASE(
                "SHOW\\s+CURRENT\\s+DATABASE",
                NO_OPERANDS);

        public final Pattern pattern;
        public final Function<String[], Optional<String[]>> operandConverter;

        SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
            this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
            this.operandConverter = operandConverter;
        }

        SqlCommand() {
            this.pattern = null;
            this.operandConverter = null;
        }

        @Override
        public String toString() {
            return super.toString().replace('_', ' ');
        }

        boolean hasPattern() {
            return pattern != null;
        }
    }

    /**
     * Call of SQL command with operands and command type.
     */
    public static class SqlCommandCall {
        public final SqlCommand command;
        public final String[] operands;

        public SqlCommandCall(SqlCommand command, String[] operands) {
            this.command = command;
            this.operands = operands;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SqlCommandCall that = (SqlCommandCall) o;
            return command == that.command && Arrays.equals(operands, that.operands);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(command);
            result = 31 * result + Arrays.hashCode(operands);
            return result;
        }

        @Override
        public String toString() {
            return command + "(" + Arrays.toString(operands) + ")";
        }
    }
}

