package me.ele.jarch.athena.util.deploy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.Org;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class OrgTest {
    @Test public void testOrgLoad() throws IOException {
        ObjectMapper objectMapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Org org = objectMapper
            .readValue(getClass().getClassLoader().getResourceAsStream("org.json"), Org.class);
        Assert.assertTrue(org.shouldLoad());
    }
}
