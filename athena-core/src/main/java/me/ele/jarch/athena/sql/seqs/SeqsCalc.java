package me.ele.jarch.athena.sql.seqs;

import java.util.Map;

@FunctionalInterface public interface SeqsCalc {
    String calc(long globalID, Map<String, String> params) throws SeqsException;
}
