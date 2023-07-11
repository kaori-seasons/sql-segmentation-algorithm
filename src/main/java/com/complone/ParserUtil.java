package com.complone;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;

import java.util.Objects;

public class ParserUtil {

    public static SqlNode parseSqlNode(boolean isFlinkParser, String statement) {
        CalciteParser parser = parser(isFlinkParser);
        SqlNode sqlNode =  parser.parse(Objects.requireNonNull(statement));
        return sqlNode;
    }

    public static SqlNode parseNativeSqlNde(boolean isFlinkParser, String statement) throws SqlParseException {
        SqlParser parser = nativeParser(statement, isFlinkParser);
        return parser.parseQuery(Objects.requireNonNull(statement));
    }

    public static CalciteParser parser(boolean isFlinkParser){
        SqlDialect dialect = selectDialect(isFlinkParser);
        CalciteParser parser = new CalciteParser(getFlinkSqlParserConfig(dialect));
        return parser;
    }

    public static SqlParser nativeParser(String statement, boolean isFlinkParser) {
        SqlDialect dialect = selectDialect(isFlinkParser);
        SqlParser parser = SqlParser.create(statement, getFlinkSqlParserConfig(dialect));
        return parser;
    }

    public static SqlDialect selectDialect(boolean isFlinkParser) {
        if (isFlinkParser){
            return HiveSqlDialect.DEFAULT;
        }else {
            return CalciteSqlDialect.DEFAULT;
        }
    }

    public static FlinkSqlConformance getSqlConformance(SqlDialect dialect) {
        if (HiveSqlDialect.DEFAULT.equals(dialect)) {
            return FlinkSqlConformance.HIVE;
        } else if (CalciteSqlDialect.DEFAULT.equals(dialect)) {
            return FlinkSqlConformance.DEFAULT;
        }
        throw new TableException("unsupported SQL dialect: " + dialect);
    }

    private static SqlParser.Config getFlinkSqlParserConfig(SqlDialect dialect) {
        FlinkSqlConformance conformance =  getSqlConformance(dialect);

        SqlParserImplFactory sqlParserImplFactory = FlinkSqlParserFactories.create(conformance);
        return configureSqlParser(sqlParserImplFactory, conformance);
    }


    private static SqlParser.Config configureSqlParser(SqlParserImplFactory parserImplFactory, SqlConformance conformance){
        return SqlParser.config().withParserFactory(parserImplFactory)
                .withConformance(conformance)
                .withLex(Lex.JAVA)
                .withIdentifierMaxLength(256);
    }
}
