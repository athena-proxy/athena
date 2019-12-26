package me.ele.jarch.athena.util.deploy;

import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.ShardingTable;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class ShardingTableTest {

    @Test public void testLoadShardTable() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ShardingTable shardingTable = objectMapper.readValue(SHARD_TABLE_JSON, ShardingTable.class);
        Assert.assertEquals("eleme_order", shardingTable.getTable());
        Assert.assertEquals("order_id", shardingTable.getComposedKey().getSeq_name());
        Assert.assertEquals("dal_mapping_order_id_user_id_",
            shardingTable.getRules().get(0).getMapping_key().getMappingRuleBySepName("order_id")
                .getTablePrefix());
    }

    private static final String SHARD_TABLE_JSON =
        "\n" + "      {\n" + "        \"table\":\"eleme_order\",\n" + "        \"composedKey\":{\n"
            + "           \"column\":\"id\",\n" + "           \"seqName\":\"order_id\",\n"
            + "           \"composeRules\":[\"user_id\",\"restaurant_id\"]\n" + "        },   \n"
            + "        \"rules\":[\n" + "           {\n" + "             \"column\":\"user_id\",\n"
            + "             \"hashRoutes\":{\n"
            + "                 \"shd_eleme_order_uid0\":\"0-256\",              \n"
            + "                 \"shd_eleme_order_uid1\":\"257-512\",\n"
            + "                 \"shd_eleme_order_uid2\":\"513-768\",\n"
            + "                 \"shd_eleme_order_uid3\":\"769-1023\"\n" + "            },\n"
            + "            \"dbRoutes\":{\n"
            + "                 \"shd_eleme_order_uid[0-1]\": \"eosgroup_shard1\",\n"
            + "                 \"shd_eleme_order_uid[2-3]\": \"eosgroup_shard2\"\n"
            + "            },\n" + "            \"mappingRoutes\":{\n"
            + "                \"order_id\": \"dal_mapping_order_id_user_id_\"\n"
            + "            }\n" + "          },\n" + "          {\n"
            + "             \"column\":\"rest_id\",\n" + "             \"hashRoutes\":{\n"
            + "                 \"shd_eleme_order_rid0\":\"0-256\",              \n"
            + "                 \"shd_eleme_order_rid1\":\"257-512\",\n"
            + "                 \"shd_eleme_order_rid2\":\"513-768\",\n"
            + "                 \"shd_eleme_order_rid3\":\"769-1023\"\n" + "            },\n"
            + "            \"dbRoutes\":{\n"
            + "                 \"shd_eleme_order_rid[0-1]\": \"eosgroup_shard1\",\n"
            + "                 \"shd_eleme_order_rid[2-3]\": \"eosgroup_shard2\"\n"
            + "            },\n" + "            \"mappingRoutes\":{\n"
            + "                \"order_id\": \"dal_mapping_order_id_rest_id_\"\n"
            + "            }\n" + "          }\n" + "        ]\n" + "      },\n" + "      {\n"
            + "         \"table\":\"dal_mysql_task\",\n" + "         \"composedKey\":{\n"
            + "           \"column\":\"task_id\",\n" + "           \"seqName\":\"composed_seq\",\n"
            + "           \"composeRules\":[\"user_id\"]\n" + "         },\n"
            + "         \"rules\":[\n" + "            {\n"
            + "             \"column\":\"user_id\",\n" + "             \"hashRoutes\":{\n"
            + "                 \"eos_mysql_task_uid_0\":\"0-256\",              \n"
            + "                 \"eos_mysql_task_uid_1\":\"257-512\",\n"
            + "                 \"eos_mysql_task_uid_2\":\"513-768\",\n"
            + "                 \"eos_mysql_task_uid_3\":\"769-1023\"  \n" + "            },\n"
            + "            \"dbRoutes\":{\n"
            + "                 \"eos_mysql_task_uid_[0-1]\": \"eosgroup_shard1\",\n"
            + "                 \"eos_mysql_task_uid_[2-3]\": \"eosgroup_shard2\"\n"
            + "            },\n" + "            \"mappingRoutes\":{\n"
            + "                \"task_id\": \"dal_mapping_task_id_user_id_\"\n" + "            }\n"
            + "          }\n" + "         ]\n" + "      }\n";
}
