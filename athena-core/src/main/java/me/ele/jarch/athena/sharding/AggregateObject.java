package me.ele.jarch.athena.sharding;

import java.math.BigDecimal;

public class AggregateObject {
    private String strVal;
    private Comparable comparableVal;

    public AggregateObject(String strVal, Comparable comparableVal) {
        this.strVal = strVal;
        this.comparableVal = comparableVal;
    }

    public String getStrVal() {
        return strVal;
    }

    public void setStrVal(String strVal) {
        this.strVal = strVal;
    }

    public Comparable getComparableVal() {
        return comparableVal;
    }

    public void setComparableVal(Comparable comparableVal) {
        this.comparableVal = comparableVal;
    }

    public BigDecimal getDecimalValue() {
        return new BigDecimal(strVal);
    }
}
