package me.ele.jarch.athena.util;

import com.google.common.base.Splitter;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * parse jdbc config url style string
 * eg:
 * key1=value1&amp;key2=value2
 * key1=value1
 *
 * @author jinghao.wang
 */
public class KVChainParser {
    public static Map<String, String> parse(String s) {
        Map<String, String> map = new LinkedHashMap<>();
        for (String entry : Splitter.on('&').split(s)) {
            List<String> keyAndValue = Splitter.on('=').limit(2).splitToList(entry);
            if (keyAndValue.size() != 2) {
                continue;
            }
            String key = keyAndValue.get(0).trim();
            String value = keyAndValue.get(1).trim();
            map.put(key, value);
        }
        return map;
    }

    public static Map<String, String> parse(Collection<String> list) {
        Map<String, String> map = new LinkedHashMap<>();
        for (String entry : list) {
            List<String> keyAndValue = Splitter.on('=').limit(2).splitToList(entry);
            if (keyAndValue.size() != 2) {
                continue;
            }
            String key = keyAndValue.get(0).trim();
            String value = keyAndValue.get(1).trim();
            map.put(key, value);
        }
        return map;
    }
}
