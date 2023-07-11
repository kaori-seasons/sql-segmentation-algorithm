package com.complone;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 描述表之间胡血缘关系 如果两表之间成环 则说明属于join子图，单独划分
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TableLineager {

    //判断方言类型 如果是hive自动加上dialect
    //除了最父级的源表以外 insert语句 ,CTAS语法的connector字段均为empty

    /**
     * 各种SQL操作的函数名
     */
    private String connector;

    //源表的类型,该值在传递的时候必须有值 否则会影响后续重建SQL
    private String tableName;

    //该源表指向多个后继节点 可能需要判断是否成环
    private List<TableLineager> next;

    //当前ddl所归属的statement, 如果connector为hive
    // 就在当前statement前面插入方言
    private String statement;

    //该语句是否已经被使用过，用于catalog当中
    private Boolean isUse = false;

    private Boolean isCatalog = false;

    private Boolean isView = false;

    private Boolean isVisited = false;

    /**
     * 虚拟表 (create view as select ... from a left join b) 当中的a和b
     */
    private List<String> virualTableList;

    //是否需要移除表
    private boolean drop;

    private Boolean execute = Boolean.FALSE;

    /**
     * 拆分后需要还原udf的上下文 从子节点逐级向上查找时
     * 需要保存变量来附加到各个子任务
     */
    private Map<String, String> functionList;
}
