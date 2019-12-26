package me.ele.jarch.athena.sql.seqs.generator;

import me.ele.jarch.athena.sql.seqs.*;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommonGenerator extends Generator {

    @Override public long decode(String column, String globalID) {
        return Generator.getHashCode(globalID);
    }

    protected void checkParams(Map<String, String> params) throws SeqsException {
        Map<String, String> kv = new HashMap<>(params);
        kv.remove(GeneratorUtil.BIZ);
        kv.remove(GeneratorUtil.SEQ_NAME);
        kv.remove(GeneratorUtil.COUNT);
        if (!kv.isEmpty()) {
            throw new SeqsException("too many useless params: " + kv + ERR_MSG);
        }
    }

    public void getGlobalID(SeqsHandler handler) throws Exception {
        String biz_seq_name =
            handler.getParams().getOrDefault(GeneratorUtil.BIZ, GeneratorUtil.COMMON_SEQ);
        checkParams(handler.getParams());
        SeqsCache seqsCache = getSeqsCache(biz_seq_name, handler.getGroup());
        seqsCache.getSeqValue(handler);
    }

    @Override public String calc(long globalID, Map<String, String> params) throws SeqsException {
        return String.valueOf(globalID);
    }

    public void checkGlobalIdConf(SeqsHandler handler) throws SeqsException {
        String seqName = handler.getParams().get(GeneratorUtil.BIZ);
        String dalGroup = handler.getGroup();
        if (StringUtils.isEmpty(seqName)) {
            throw new SeqsException("cannot find global id config" + ERR_MSG);
        }
        List<GlobalId> globalIds = GlobalId.getGlobalIdsByDALGroup(dalGroup);
        if (globalIds.stream().noneMatch(globalId -> seqName.equals(globalId.seqName))) {
            throw new SeqsException("cannot find global id config" + ERR_MSG);
        }
    }
}
