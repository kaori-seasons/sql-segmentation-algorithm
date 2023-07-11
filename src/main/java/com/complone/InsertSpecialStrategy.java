package com.complone;

import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertSpecialStrategy implements SqlSpecailParseStrategy {

    protected static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE;
    protected Pattern insertintoPattern;
    protected Pattern insertoverwritePattern;
    protected Pattern beforePattern;

    private SpecialStrategyEnums strategyEnums;

    public InsertSpecialStrategy() {
        this.insertintoPattern = Pattern.compile("insert\\s+into ", DEFAULT_PATTERN_FLAGS);
        this.insertoverwritePattern = Pattern.compile("insert\\s+overwrite ", DEFAULT_PATTERN_FLAGS);
        this.beforePattern = Pattern.compile(".*(?=INSERT)", Pattern.CASE_INSENSITIVE);
    }

    @Override
    public Boolean validate(String statement, boolean isFlinkParser) {
        Matcher insertintoMatcher = insertintoPattern.matcher(statement.trim());
        Matcher insertoverwriteMatcher = insertoverwritePattern.matcher(statement.trim());
        Matcher before = beforePattern.matcher(statement);
        if (before.find()) {
            if (StringUtils.isNotBlank(before.group(0))){
                return false;
            }
        }
        if (insertintoMatcher.find()) {
            strategyEnums = SpecialStrategyEnums.INSERT_INTO;
            return true;
        } else if (insertoverwriteMatcher.find()) {
            strategyEnums = SpecialStrategyEnums.INSERT_OVERWRITE;
            return true;
        }
        return false;
    }

    @Override
    public Optional<SqlOperation> apply(String statement, boolean isFlinkParser) {
        return Optional.empty();
        //如果insert语句解析失败 要么是用户语法写错， 那么是语法不规范
    }
}
