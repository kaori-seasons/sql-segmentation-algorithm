package com.complone;

import com.complone.SqlOperationEnum;

import java.util.List;

// insert into xxx select * from yyy join zzz on yyy.id =zzz.id
public class SqlOperation {
    //当前操作的名称 functionName, tableName, catalogName
    private String name;
    private SqlOperationEnum type;
    private String statement;
    //在发生异常的时候 需要还原原来statement的上下文信息
    private String originStatement;
    private List<String> sourceTableList;

    public SqlOperation(String name, SqlOperationEnum type, String statement, String originStatement, List<String> sourceTableList) {
        this.name = name;
        this.type = type;
        this.statement = statement;
        this.originStatement = originStatement;
        this.sourceTableList = sourceTableList;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SqlOperationEnum getType() {
        return type;
    }

    public void setType(SqlOperationEnum type) {
        this.type = type;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public String getOriginStatement() {
        return originStatement;
    }

    public void setOriginStatement(String originStatement) {
        this.originStatement = originStatement;
    }

    public List<String> getSourceTableList() {
        return sourceTableList;
    }

    public void setSourceTableList(List<String> sourceTableList) {
        this.sourceTableList = sourceTableList;
    }

}
