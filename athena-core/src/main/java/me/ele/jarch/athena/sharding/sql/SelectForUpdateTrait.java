package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;

import java.util.Objects;

/**
 * 用于减少重复提供公有方法判定SQL是否是SELECT_FOR_UPDATE类型的特质
 * Created by jinghao.wang on 2018/1/2.
 */
public interface SelectForUpdateTrait {

    /**
     * 判定是否是SELECT_FOR_UPDATE请调用此方法
     *
     * @param sqlSelectQueryBlock
     * @return
     */
    default boolean isSelectForUpdate(SQLSelectQueryBlock sqlSelectQueryBlock) {
        if (Objects.isNull(sqlSelectQueryBlock)) {
            return false;
        }
        if (sqlSelectQueryBlock instanceof MySqlSelectQueryBlock) {
            return isMySqlSelectForUpdate((MySqlSelectQueryBlock) sqlSelectQueryBlock);
        } else if (sqlSelectQueryBlock instanceof PGSelectQueryBlock) {
            return isPGSelectForUpdate((PGSelectQueryBlock) sqlSelectQueryBlock);
        } else {
            return false;
        }
    }

    default boolean isMySqlSelectForUpdate(MySqlSelectQueryBlock mySqlSelectQueryBlock) {
        return mySqlSelectQueryBlock.isForUpdate() || mySqlSelectQueryBlock.isLockInShareMode();
    }

    default boolean isPGSelectForUpdate(PGSelectQueryBlock pgSelectQueryBlock) {
        if (Objects.isNull(pgSelectQueryBlock.getForClause())) {
            return false;
        }
        return pgSelectQueryBlock.getForClause().getOption()
            == PGSelectQueryBlock.ForClause.Option.SHARE
            || pgSelectQueryBlock.getForClause().getOption()
            == PGSelectQueryBlock.ForClause.Option.UPDATE;
    }
}
