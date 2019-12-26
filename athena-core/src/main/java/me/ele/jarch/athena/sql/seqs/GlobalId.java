package me.ele.jarch.athena.sql.seqs;

import me.ele.jarch.athena.util.config.GlobalIdConfig.GlobalIdSchema;
import me.ele.jarch.athena.util.config.GlobalIdConfig.Zone;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jinghao.wang on 17/2/20.
 */
public class GlobalId {
    private static volatile Map<String, GlobalId> globalIds = new ConcurrentHashMap<>();
    private static volatile Map<String, List<GlobalId>> groupGlobalIds = new ConcurrentHashMap<>();

    public final String bizName;
    public final String seqName;
    public final String generator;
    public final String zoneName;
    public final String zoneSeqName;
    public final boolean recycled;
    public final List<String> params;
    public final long minSeed;
    public final long minGlobalId;
    public final long maxSeed;
    public final long maxGlobalId;
    public final int recycled_cache_lifecycle; //可回收seq cache的生命周期,单位秒,默认值30分钟,超出重新get seed cache
    public final Set<String> attachedDALGroups;

    public GlobalId(String bizName, String seqName, String generator, boolean recycled,
        int recycled_cache_lifecycle, String zoneName, String zoneSeqName, List<String> params,
        long minSeed, long minGlobalId, long maxSeed, long maxGlobalId,
        Set<String> attachedDALGroups) {
        this.bizName = bizName;
        this.seqName = seqName;
        this.generator = generator;
        this.zoneName = zoneName;
        this.zoneSeqName = zoneSeqName;
        this.params = params;
        this.minSeed = minSeed;
        this.minGlobalId = minGlobalId;
        this.maxSeed = maxSeed;
        this.maxGlobalId = maxGlobalId;
        this.attachedDALGroups = attachedDALGroups;
        this.recycled = recycled;
        this.recycled_cache_lifecycle =
            recycled_cache_lifecycle > 0 ? recycled_cache_lifecycle : 30 * 60;
    }

    public static void updateGlobalId(DalGroupConfig dalGroupConfig) {
        List<GlobalId> newGroupGlobalIds = new ArrayList<>();
        dalGroupConfig.getGlobalIds().forEach(schema -> {
            Zone activeZone = schema.getActiveZone();
            if (activeZone.isActive()) {
                GlobalId globalId =
                    new GlobalId(schema.getBiz_name(), schema.getSeq_name(), schema.getGenerator(),
                        schema.getRecycled(), schema.getRecycled_cache_lifecycle(),
                        activeZone.getZone(), activeZone.getSeq_name_zone(),
                        Collections.unmodifiableList(schema.getParams()), activeZone.getMin_seed(),
                        activeZone.getMin_globalid(), activeZone.getMax_seed(),
                        activeZone.getMax_globalid(),
                        Collections.synchronizedSet(new HashSet<>(activeZone.getGroups())));
                GlobalId oldGlobalId = globalIds.put(schema.getSeq_name(), globalId);
                if (Objects.nonNull(oldGlobalId)) {
                    globalId.attachedDALGroups.addAll(oldGlobalId.attachedDALGroups);
                }
                newGroupGlobalIds.add(globalId);
            }
            groupGlobalIds.put(dalGroupConfig.getName(), newGroupGlobalIds);
        });
        //将此dalgroup从被删除的global ID的groups中删除
        globalIds.entrySet().stream().filter(
            entry -> dalGroupConfig.getGlobalIds().stream().map(GlobalIdSchema::getSeq_name)
                .noneMatch(seqName -> entry.getKey().equals(seqName)))
            .filter(entry -> entry.getValue().attachedDALGroups.contains(dalGroupConfig.getName()))
            .forEach(entry -> entry.getValue().attachedDALGroups.remove(dalGroupConfig.getName()));
        SeqsCache.updateGlobalIdCfg();
    }

    public static GlobalId getGlobalIdBySeqName(String seqName) {
        return globalIds.get(seqName);
    }

    public static List<GlobalId> getGlobalIdsByDALGroup(String DALGroupName) {
        List<GlobalId> globalIds =
            groupGlobalIds.getOrDefault(DALGroupName, Collections.emptyList());
        return Collections.unmodifiableList(globalIds);
    }

    public static void removeDalGroupGlobalId(DalGroupConfig dalGroupCfg) {
        groupGlobalIds.remove(dalGroupCfg.getName());
        dalGroupCfg.getGlobalIds().forEach(globalId -> {
            GlobalId oldGlobalId = globalIds.get(globalId.getSeq_name());
            if (Objects.nonNull(oldGlobalId)) {
                //只将此dalgroup从global Id中去掉
                oldGlobalId.attachedDALGroups.remove(dalGroupCfg.getName());
            }
        });
    }

    public static Map<String, GlobalId> allActiveGlobalIds() {
        return globalIds;
    }

    /**
     * 当前zone的种子范围
     * 仅仅用最大种子 - 最小种子, 没有对0或负值防御
     *
     * @return > 0 配置正确， <= 0 配置错误
     */
    public long seedCount() {
        return maxSeed - minSeed;
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("GlobalId{");
        sb.append("bizName='").append(bizName).append('\'');
        sb.append(", seqName='").append(seqName).append('\'');
        sb.append(", generator='").append(generator).append('\'');
        sb.append(", zoneName='").append(zoneName).append('\'');
        sb.append(", zoneSeqName='").append(zoneSeqName).append('\'');
        sb.append(", params=").append(params);
        sb.append(", minSeed=").append(minSeed);
        sb.append(", minGlobalId=").append(minGlobalId);
        sb.append(", maxSeed=").append(maxSeed);
        sb.append(", maxGlobalId=").append(maxGlobalId);
        sb.append(", attachedDALGroups=").append(attachedDALGroups);
        sb.append('}');
        return sb.toString();
    }
}
