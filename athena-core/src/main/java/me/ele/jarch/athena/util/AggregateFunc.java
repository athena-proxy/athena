package me.ele.jarch.athena.util;

import me.ele.jarch.athena.sharding.AggregateObject;

import java.math.BigDecimal;

public enum AggregateFunc {
    NONE, COUNT, SUM, MAX, MIN, AVG, BIT_AND, BIT_OR, BIT_XOR, COUNT_DISTINCT, GROUP_CONCAT, STD, STDDEV_POP, STDDEV_SAMP, STDDEV, VAR_POP, VAR_SAMP, VARIANCE, DISTINCT;

    public static AggregateFunc valueOfFunc(final String func) {
        for (AggregateFunc afunc : values()) {
            if (afunc.name().equals(func)) {
                return afunc;
            }
        }
        return NONE;
    }

    public boolean validate() {
        switch (this) {
            case NONE:
            case COUNT:
            case SUM:
            case MAX:
            case MIN:
                return true;
            default:
                return false;
        }
    }

    public AggregateObject aggregate(AggregateObject value1, AggregateObject value2) {
        switch (this) {
            case SUM:
            case COUNT:
                BigDecimal newVal = value1.getDecimalValue().add(value2.getDecimalValue());
                value1.setComparableVal(newVal);
                value1.setStrVal(String.valueOf(newVal));
                return value1;
            case MAX:
                return value1.getComparableVal().compareTo(value2.getComparableVal()) > 0 ?
                    value1 :
                    value2;
            case MIN:
                return value1.getComparableVal().compareTo(value2.getComparableVal()) < 0 ?
                    value1 :
                    value2;
            case NONE:
            default:
                return value1;
        }
    }
}
