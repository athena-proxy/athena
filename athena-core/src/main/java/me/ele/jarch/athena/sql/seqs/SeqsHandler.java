package me.ele.jarch.athena.sql.seqs;

import me.ele.jarch.athena.sql.seqs.generator.Generator;
import me.ele.jarch.athena.util.etrace.DalMultiMessageProducer;

import java.util.Map;

public abstract class SeqsHandler {
    /**
     * @param globalIDData 从seqCache里得到的globalID值,并且拼装成返回的二维数组
     * @param success      此次尝试获取globalid是否成功
     */
    public abstract void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg);



    private long count;

    private final Generator generator;

    private final String group;

    private final DalMultiMessageProducer producer;

    private final Map<String, String> params;

    private final boolean isRange;

    private final boolean isSerial;

    public SeqsHandler(int count, String seqName, String group, DalMultiMessageProducer producer,
        Map<String, String> params, boolean isRange, boolean isSerial) throws Exception {
        this.count = count;
        if (params != null && params.get(GeneratorUtil.COUNT) != null) {
            this.count = Long.valueOf(params.get(GeneratorUtil.COUNT));
        }
        this.generator = SeqsGenerator.getGenerator(seqName);
        this.group = group;
        this.producer = producer;
        this.params = params;
        this.isRange = isRange;
        this.isSerial = isSerial;
    }

    public long getCount() {
        return count;
    }

    public SeqsCalc getCalc() {
        return generator;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public String getGroup() {
        return group;
    }

    public DalMultiMessageProducer getProducer() {
        return producer;
    }

    public void getGlobalID() throws Exception {
        generator.getGlobalID(this);
    }

    public boolean isRange() {
        return isRange;
    }

    public boolean isSerial() {
        return isSerial;
    }

}
