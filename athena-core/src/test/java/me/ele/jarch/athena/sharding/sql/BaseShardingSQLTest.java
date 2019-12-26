package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sharding.ShardingConfig;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.GrayType;
import me.ele.jarch.athena.util.deploy.OrgConfig;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Created by jinghao.wang on 2017/11/6.
 */
public abstract class BaseShardingSQLTest {
    protected static final String EOS = "eos";
    protected static final String NEW_EOS_MAIN_GROUP = "new_eos_main_group";
    protected static final String NAPOS_OPERATION_ZEUS_ERS_GROUP = "napos_operation-zeus_ers_group";
    protected static final String APOLLO_GROUP = "apollo_group";
    protected static Predicate<String> whiteFieldsFilter = null;

    protected List<String> expectResults;
    protected List<String> executeResults;
    protected ShardingRouter newOrderShardingRouter;
    protected ShardingRouter shardingRouterSingle =
        ShardingRouter.getShardingRouter("eos_single_dimension_group");
    protected ShardingRouter shardingRouterMulKey =
        ShardingRouter.getShardingRouter("eos_mulkeyhash__group");
    protected ShardingRouter shardingRouterSingleMapping =
        ShardingRouter.getShardingRouter("eos_single_dimension_mapping_group");
    protected ShardingRouter mixShardingRouter = ShardingRouter.getShardingRouter("apollo_group");
    protected ShardingRouter shardingRouterNewApollo =
        ShardingRouter.getShardingRouter("apollo_new_group");


    @BeforeClass public void setUp() throws Exception {
        AthenaConfig.getInstance().setGrayType(GrayType.SHARDING);
        ShardingRouter.clear();
        DBChannelDispatcher.getHolders().clear();
        DBChannelDispatcher.TEMP_DAL_GROUP_CONFIGS.clear();
        ShardingConfig.load(getClass().getClassLoader().getResource("sharding.yml").getFile());
        Set<String> whiteFields = new TreeSet<>();
        whiteFields.add("id");
        whiteFields.add("user_id");
        whiteFields.add("restaurant_id");
        whiteFields.add("task_hash");
        whiteFields.add("seq_name");
        whiteFields.add("biz");
        whiteFields.add("field1");
        whiteFields.add("field2");
        whiteFields.add("field3");
        whiteFieldsFilter = whiteFields::contains;
        // for test
        OrgConfig.getInstance().loadConfig(
            getClass().getClassLoader().getResource("conf/goproxy-front-port.cfg.template")
                .getFile());
        DBChannelDispatcher.updateDispatcher(buildDalGroupCfg(EOS, EOS));
        DBChannelDispatcher
            .updateDispatcher(buildDalGroupCfg(NAPOS_OPERATION_ZEUS_ERS_GROUP, "napos_group"));
        DBChannelDispatcher
            .updateDispatcher(buildDalGroupCfg(NEW_EOS_MAIN_GROUP, NEW_EOS_MAIN_GROUP));
        DBChannelDispatcher.updateDispatcher(buildDalGroupCfg(APOLLO_GROUP, APOLLO_GROUP));
        DBChannelDispatcher.updateDispatcher(
            buildDalGroupCfg("eos_single_dimension_group", "eos_single_dimension_group"));
        DBChannelDispatcher
            .updateDispatcher(buildDalGroupCfg("eos_mulkeyhash__group", "eos_mulkeyhash__group"));
        DBChannelDispatcher.updateDispatcher(buildDalGroupCfg("eos_single_dimension_mapping_group",
            "eos_single_dimension_mapping_group"));
        DBChannelDispatcher
            .updateDispatcher(buildDalGroupCfg("apollo_new_group", "apollo_new_group"));
    }

    @AfterClass public static void tearDown() {
        // for test
        OrgConfig.getInstance().loadConfig("conf/goproxy-front-port.cfg");
    }

    @BeforeMethod public void beforeMethod() {
        newOrderShardingRouter = ShardingRouter.getShardingRouter(NEW_EOS_MAIN_GROUP);
        shardingRouterSingle = ShardingRouter.getShardingRouter("eos_single_dimension_group");
        shardingRouterMulKey = ShardingRouter.getShardingRouter("eos_mulkeyhash__group");
        shardingRouterSingleMapping =
            ShardingRouter.getShardingRouter("eos_single_dimension_mapping_group");
        shardingRouterNewApollo = ShardingRouter.getShardingRouter("apollo_new_group");
        mixShardingRouter = ShardingRouter.getShardingRouter(APOLLO_GROUP);
        expectResults = new ArrayList<>();
        executeResults = new ArrayList<>();
    }

    private static DalGroupConfig buildDalGroupCfg(String name, String dbGroup) {
        DalGroupConfig dalGroupConfig = new DalGroupConfig(name);
        dalGroupConfig.setHomeDbGroupName(dbGroup);
        dalGroupConfig.updateShardingCfg();
        return dalGroupConfig;
    }
}
