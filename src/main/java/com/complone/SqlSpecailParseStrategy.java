package com.complone;

import java.util.Optional;

public interface SqlSpecailParseStrategy {

    Boolean validate(String statement, boolean isFlinkParser);

    Optional<SqlOperation> apply(String statement, boolean isFlinkParser);
}
