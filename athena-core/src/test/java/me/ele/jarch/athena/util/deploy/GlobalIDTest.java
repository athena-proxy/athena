package me.ele.jarch.athena.util.deploy;

import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.util.config.GlobalIdConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class GlobalIDTest {

    @Test public void testLoadGlobalId() throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();
        GlobalIdConfig.GlobalIdSchema globalId = objectMapper
            .readValue(getClass().getClassLoader().getResourceAsStream("globalId.json"),
                GlobalIdConfig.GlobalIdSchema.class);
        Assert.assertEquals("eleme_order_orderid_seq", globalId.getSeq_name());
        Assert.assertEquals("eleme_order_orderid_seq_xg",
            globalId.getActiveZone().getSeq_name_zone());
    }
}
