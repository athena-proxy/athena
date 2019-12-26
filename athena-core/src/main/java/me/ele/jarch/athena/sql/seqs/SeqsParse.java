package me.ele.jarch.athena.sql.seqs;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.util.AthenaConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SeqsParse {
    public String seqName;
    public boolean isRange = false;
    public boolean isSerial = false;
    public Map<String, String> params = new HashMap<>();

    public boolean parseGlobalIDSQL(CmdQuery cmdQuery) {
        if (!GeneratorUtil.DAL_DUAL.equalsIgnoreCase(cmdQuery.table)) {
            return false;
        }
        if (Objects.isNull(cmdQuery.shardingSql)) {
            return false;
        }
        if (cmdQuery.shardingSql.params.isEmpty()) {
            return false;
        }
        // 如果不是select next_value或sharding_count,表示不是拿种子和拿sharding片的数量
        params = cmdQuery.shardingSql.params;
        switch (cmdQuery.shardingSql.selected_value) {
            case GeneratorUtil.SHARDING_COUNT:
                seqName = GeneratorUtil.SHARDING_COUNT;
                return true;
            case GeneratorUtil.NEXT_VALUE:
                seqName = params.getOrDefault("seq_name", "").toLowerCase();
                if (params.get("count") != null) {
                    isRange = true;
                }
                return true;
            case "":
                if (GeneratorUtil.NEXT_BEGIN.equalsIgnoreCase(cmdQuery.shardingSql.next_begin)
                    && GeneratorUtil.NEXT_END.equalsIgnoreCase(cmdQuery.shardingSql.next_end)) {
                    seqName = params.getOrDefault("seq_name", "").toLowerCase();
                    if (!"common_seq".equalsIgnoreCase(seqName)) {
                        return false;
                    }
                    isRange = true;
                    isSerial = true;
                    return true;
                } else {
                    return false;
                }

            default:
                return false;
        }
    }

    /**
     * 检查 range globalid count 值必须在设置范围内
     */
    public void checkSeqParamCount() {
        if (this.params.containsKey(GeneratorUtil.COUNT)) {
            long maxRange = AthenaConfig.getInstance().getDalSeqsCachePool();
            long rangeCount = 0;
            try {
                rangeCount = Long.valueOf(this.params.get(GeneratorUtil.COUNT));
            } catch (NumberFormatException e) {
                throw new QueryException.Builder(ErrorCode.ER_WRONG_TYPE_COLUMN_VALUE_ERROR)
                    .setSequenceId(1).setErrorMessage("global id sql count must be type integer ")
                    .bulid();
            }
            if (rangeCount <= 0 || rangeCount > maxRange) {
                throw new QueryException.Builder(ErrorCode.ER_WARN_DATA_OUT_OF_RANGE)
                    .setSequenceId(1)
                    .setErrorMessage("global id sql count must be in [1, " + maxRange + "]")
                    .bulid();
            }
        }
    }
}
