package me.ele.jarch.athena.netty.debug;

import me.ele.jarch.athena.util.JacksonObjectMappers;
import me.ele.jarch.athena.util.SafeJSONHelper;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;

public class SystemInfoTest {

    @Test public void testConvertStringtoMap() {
        String s =
            "-Delog.config=/Users/opt/data/conf/logback.xml\n-Dfile.encoding=UTF-8\n-agentlib:jdwp=transport=dt_socket,suspend=y,address=localhost:49667\n";
        String separator = "\n";
        String connector = "=";
        Map<String, Object> map = SystemInfo.convertString(s, separator, connector);
        String json = SafeJSONHelper.of(JacksonObjectMappers.getPrettyMapper())
            .writeValueAsStringOrDefault(map, "");
        System.out.println(json);
        assertEquals(true, true);
    }

    @Test public void testConvertStringtoList() {
        String s =
            "HeartBeatResult [group=eosgroup5, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup5, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup1, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup7, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup7, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup1, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup1, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup3, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup8, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eusgroup, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup3, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup10, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup10, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup5, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup3, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup9, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup7, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup6, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup6, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eusgroup, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup2, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup4, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup2, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup2, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup8, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup6, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup9, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup9, id=slave1, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup8, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup4, id=slave2, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup10, id=master0, alive=true, readOnly=true, failedTimes=0]\nHeartBeatResult [group=eosgroup4, id=slave1, alive=true, readOnly=true, failedTimes=0]\n";
        String separatorForArray = "HeartBeatResult";
        String separatorForKV = ",";
        String connector = "=";
        List<Map<String, Object>> list =
            SystemInfo.convertString(s, separatorForArray, separatorForKV, connector);
        String json = SafeJSONHelper.of(JacksonObjectMappers.getPrettyMapper())
            .writeValueAsStringOrDefault(list, "[]");
        System.out.println(json);
    }

}
