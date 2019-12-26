package me.ele.jarch.athena.sql.seqs;

import me.ele.jarch.athena.sql.seqs.generator.CommonGenerator;
import me.ele.jarch.athena.sql.seqs.generator.ComposedGenerator;
import me.ele.jarch.athena.sql.seqs.generator.Generator;
import me.ele.jarch.athena.sql.seqs.generator.TwoDimTaskGenerator;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xczhang on 15/8/6 上午10:46.
 */
// 1) 新版本,没有composed_key的通用生成globalID规则seq_name = 'common_seq' AND biz =
// SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao';
//
// 2) 有composed_key的通用globalID规则 seq_name = 'composed_seq' AND biz = AND ...
// SELECT next_value FROM dal_dual WHERE seq_name = 'composed_seq' AND biz = 'moses' AND org_id = '123' AND team_id = '456';
public class SeqsGenerator {
    private static Map<String, Generator> generatorMap = new HashMap<>();

    static {

        // SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao'
        generatorMap.put(GeneratorUtil.COMMON_SEQ, new CommonGenerator());

        // SELECT next_value FROM dal_dual WHERE seq_name = 'composed_seq' AND biz = 'moses' AND org_id = '123' AND team_id = '456';
        generatorMap.put(GeneratorUtil.COMPOSED_SEQ, new ComposedGenerator());

        // SELECT next_value FROM dal_dual WHERE seq_name = 'fix_hash_seq' AND prefix = '26383326abc' AND first_dim = '123' AND second_dim = '456'
        generatorMap.put(GeneratorUtil.FIX_HASH_SEQ,
            new TwoDimTaskGenerator(GeneratorUtil.PREFIX, GeneratorUtil.FIRST_DIM,
                GeneratorUtil.SECOND_DIM));
    }

    /**
     * @param seq_name
     * @return Generator to seq_name or null
     */
    public static Generator getGenerator(String seq_name) throws Exception {
        if (!generatorMap.containsKey(seq_name))
            throw new SeqsException(seq_name);
        return generatorMap.get(seq_name);
    }
}
