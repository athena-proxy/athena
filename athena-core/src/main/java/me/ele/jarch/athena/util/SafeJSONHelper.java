package me.ele.jarch.athena.util;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * typical usage:
 * <pre>
 *     SafeJSONHelper.of(JacksonHelper.getMapper()).writeValueAsStringOrDefault(object, "{}")
 *     SafeJSONHelper.of(JacksonHelper.getPrettyMapper()).writeValueAsStringOrDefault(object, "{}")
 *     SafeJSONHelper.of(JacksonHelper.getMapper()).writeValueAsStringOrDefault(object, "[]")
 * </pre>
 * Created by jinghao.wang on 2019-02-18.
 */
public class SafeJSONHelper {
    private final ObjectMapper mapper;

    private SafeJSONHelper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public static SafeJSONHelper of(ObjectMapper mapper) {
        return new SafeJSONHelper(mapper);
    }

    public String writeValueAsStringOrDefault(Object object, String defaultValue) {
        try {
            return mapper.writeValueAsString(object);
        } catch (Throwable ignore) {
            return defaultValue;
        }
    }
}
