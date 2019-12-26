package me.ele.jarch.athena.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JacksonObjectMappers {
    // fix about parnew gc cost long time to scan string table
    private final static JsonFactory factory =
        new JsonFactory().disable(JsonFactory.Feature.INTERN_FIELD_NAMES);

    private final static ObjectMapper mapper;

    private final static ObjectMapper prettyMapper;

    static {
        mapper = new ObjectMapper(factory);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        prettyMapper = mapper.copy().configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static ObjectMapper getPrettyMapper() {
        return prettyMapper;
    }
}
