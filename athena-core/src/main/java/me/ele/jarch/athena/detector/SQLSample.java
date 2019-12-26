package me.ele.jarch.athena.detector;

import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.util.etrace.EtracePatternUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by Dal-Dev-Team on 17/3/3.
 */
public class SQLSample extends AbstractSample {
    private String pattern = "";
    public String sqlid = "";
    public QUERY_TYPE type = QUERY_TYPE.OTHER;

    // sqlid的单行的request/response/affected rows
    // 请求SQL字节数
    protected long reqBytesSumInCurWindow = 0;
    // SQL返回字节数
    protected long respBytesSumInCurWindow = 0;
    // 影响行数
    protected long affectedRowsSumInCurWindow = 0;

    public String getPattern() {
        return pattern;
    }

    // caculate when get result
    public final SQLSampleResult result = new SQLSampleResult();

    @Override protected void calc() {
        if (count == 0 || StringUtils.isEmpty(pattern)) {
            return;
        }
        EtracePatternUtil.SQLRecord record = EtracePatternUtil.addAndGet(pattern);
        if (record.getCount() != 0) {
            result.setAvgDurInAllWindow(
                DetectUtil.div(record.getSumResponseTimeInMill(), record.getCount()));
        }
        sqlid = record.hash;
        result.setAvgDurInCurWindow(DetectUtil.div(durSumInCurWindow, count));
        //        result.setUpper90Dur(calcUpper90());
        result.setAvgReqBytesInCurWindow(reqBytesSumInCurWindow / count);
        result.setAvgRespBytesInCurWindow(respBytesSumInCurWindow / count);
        result.setAvgAffectedRowsInCurWindow(affectedRowsSumInCurWindow / count);
        result.setQps(DetectUtil.div(1000 * count, curWindowDur));
    }

    @Override public void addSample(long dur, SqlSessionContext ctx) {
        CmdQuery query = ctx.curCmdQuery;
        String pattern = query.shardingSql.getOriginMostSafeSQL();
        String dalGroup = ctx.getHolder().getDalGroup().getName();
        this.dalGroup = dalGroup;
        this.pattern = pattern;
        this.type = query.queryType;
        this.durSumInCurWindow += dur;
        //        this.putDur(dur);
        this.count++;
        this.respBytesSumInCurWindow += ctx.writeToClientCounts;
        this.reqBytesSumInCurWindow += query.getSendBuf() != null ? query.getSendBuf().length : 0;
        this.affectedRowsSumInCurWindow += ctx.sqlSessionContextUtil.getAffectedRows();
    }
}
