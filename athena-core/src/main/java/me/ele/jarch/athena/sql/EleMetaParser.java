package me.ele.jarch.athena.sql;

import me.ele.jarch.athena.util.KVChainParser;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Created by jinghao.wang on 16/11/28.
 */
public class EleMetaParser {
    private Map<String, String> eleMeta = Collections.emptyMap();
    private String queryComment = "";
    private String queryWithoutComment = "";

    public Map<String, String> getEleMeta() {
        return eleMeta;
    }

    public String getQueryComment() {
        return queryComment;
    }

    public String getQueryWithoutComment() {
        return queryWithoutComment;
    }

    public static EleMetaParser parse(String query) {
        EleMetaParser eleMetaParser = new EleMetaParser();
        eleMetaParser.queryWithoutComment = query;
        EleCommentIndexer indexer = searchEleMetaCommentIndexer(query);
        if (!Objects.equals(EleCommentIndexer.EMPTY, indexer)) {
            eleMetaParser.queryComment =
                query.substring(indexer.leftCommentIndex, indexer.rightCommentIndex + 1);
            eleMetaParser.eleMeta = KVChainParser
                .parse(query.substring(indexer.leftContentIndex + 1, indexer.rightContentIndex));
            eleMetaParser.queryWithoutComment = query.substring(indexer.rightCommentIndex + 1);
        }
        return eleMetaParser;
    }

    public static EleCommentIndexer searchEleMetaCommentIndexer(final String rawSQL) {
        if (Objects.isNull(rawSQL) || rawSQL.isEmpty()) {
            return EleCommentIndexer.EMPTY;
        }
        //最短的饿了么自定注释字符串为/*E::E*/, 8个字符
        if (rawSQL.length() < 8) {
            return EleCommentIndexer.EMPTY;
        }
        if (!(Objects.equals('/', rawSQL.charAt(0)) && Objects.equals('*', rawSQL.charAt(1)))) {
            //条件反转,如果原始SQL不以/*开头提前返回
            return EleCommentIndexer.EMPTY;
        }
        for (int j = 2; j < rawSQL.length() - 5; j++) {
            if (Objects.equals('E', rawSQL.charAt(j)) && Objects
                .equals(':', rawSQL.charAt(j + 1))) {
                for (int k = j + 2; k < rawSQL.length() - 3; k++) {
                    if (Objects.equals(':', rawSQL.charAt(k)) && Objects
                        .equals('E', rawSQL.charAt(k + 1))) {
                        for (int l = k + 2; l < rawSQL.length() - 1; l++) {
                            if (Objects.equals('*', rawSQL.charAt(l)) && Objects
                                .equals('/', rawSQL.charAt(l + 1))) {
                                return new EleCommentIndexer(0, j + 1, k, l + 1);
                            } else if (Character.isWhitespace(rawSQL.charAt(l))) {
                                continue;
                            } else {
                                break;
                            }
                        }
                    }
                }
            } else if (Character.isWhitespace(rawSQL.charAt(j))) {
                continue;
            } else {
                break;
            }
        }
        return EleCommentIndexer.EMPTY;
    }

    public static class EleCommentIndexer {
        public static final EleCommentIndexer EMPTY = new EleCommentIndexer(-1, -1, -1, -1);
        /**
         * /* E:
         * ^
         * |
         * 注释左斜杠位置
         */
        public final int leftCommentIndex;
        /**
         * /* E:
         * ^
         * |
         * 冒号所在位置
         */
        public int leftContentIndex;
        /**
         * 对称冒号位置
         */
        public int rightContentIndex;
        /**
         * 对称右斜杠位置
         */
        public int rightCommentIndex;

        public EleCommentIndexer(int leftCommentIndex, int leftContentIndex, int rightContentIndex,
            int rightCommentIndex) {
            this.leftCommentIndex = leftCommentIndex;
            this.leftContentIndex = leftContentIndex;
            this.rightContentIndex = rightContentIndex;
            this.rightCommentIndex = rightCommentIndex;
        }
    }
}
