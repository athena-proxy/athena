package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.visitor.PrintableVisitor;

public interface SafeOutputVisitor extends PrintableVisitor {
    WhiteFieldsOutputProxy getWhiteFieldsOutputProxy();
}