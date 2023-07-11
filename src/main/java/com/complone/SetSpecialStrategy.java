package com.complone;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.complone.Constants.MATCH_KEY_STRING;
import static com.complone.Constants.MATCH_QUOTED_VALUE;
import static com.complone.Constants.MATCH_VALUE_STRING;

public class SetSpecialStrategy implements SqlSpecailParseStrategy {

    protected static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
    protected Pattern pattern;
    protected Pattern beforePattern;

    List<String> operands = new ArrayList<>();

    //为保证在其他语句中在字符串胡最左边出现，使用正向零宽后发断言
    public SetSpecialStrategy() {
        this.pattern = Pattern.compile("SET(\\s+(?<key>\\S+)\\s*=\\s*('(?<quotedVal>[^']*)'|(?<val>\\S+)))?", DEFAULT_PATTERN_FLAGS);
        this.beforePattern = Pattern.compile(".*(?=SET)", Pattern.CASE_INSENSITIVE);
    }

    @Override
    public Boolean validate(String statement, boolean isFlinkParser) {
        statement = statement.trim();
        Matcher before = beforePattern.matcher(statement);
        if (before.find()) {
            if (StringUtils.isNotBlank(before.group(0))){
                return false;
            }
        }
        Matcher matcher = pattern.matcher(statement.trim());
        if (matcher.find()) {
            if (matcher.group(MATCH_KEY_STRING) != null) {
                operands.add(matcher.group(MATCH_KEY_STRING));
                operands.add(matcher.group(MATCH_QUOTED_VALUE) != null
                        ? matcher.group(MATCH_QUOTED_VALUE)
                        : matcher.group(MATCH_VALUE_STRING));
            }
            return true;
        }
        return false;
    }

    @Override
    public Optional<SqlOperation> apply(String statement, boolean isFlinkParser) {
        if (!operands.isEmpty() && operands.size() == 2) {
            SqlOperation sqlOperation = new SqlOperation(operands.get(0), SqlOperationEnum.SET, statement, statement, null);
            return Optional.of(sqlOperation);
        }
        return Optional.empty();
    }
}
