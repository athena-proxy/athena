package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.statement.SQLCommitStatement;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.*;
import me.ele.jarch.athena.sharding.ShardingRouter;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by jie on 16/12/2.
 */
public class PGSQLStatementParserTest {

    private void testParseSQL(String sql, String expected, Class<?> type) {
        ShardingSQL shardingSQL = ShardingSQL
            .handlePGBasicly(sql, "", ShardingRouter.defaultShardingRouter, new Send2BatchCond(),
                ShardingSQL.BLACK_FIELDS_FILTER);
        assertTrue(type.isAssignableFrom(shardingSQL.sqlStmt.getClass()));
        assertEquals(ShardingUtil.debugSql(shardingSQL.sqlStmt), expected);
    }

    @Test public void testUnsignedBigInt() throws Exception {
        String sql = "SELECT a from b where c <> 1 LIMIT 18446744073709551615 OFFSET 1";
        String expected = "SELECT a FROM b WHERE c <> 1 LIMIT 18446744073709551615 OFFSET 1";
        testParseSQL(sql, expected, PGSelectQueryBlock.class);
    }

    @Test public void testBegin() throws Exception {
        String sql = "begIn;";
        String expected = "BEGIN";
        Class<?> type = SQLStartTransactionStatement.class;
        testParseSQL(sql, expected, type);

        sql = "start transaction;";
        expected = "START TRANSACTION";
        type = PGStartTransactionStatement.class;
        testParseSQL(sql, expected, type);
    }

    @Test public void testShow() throws Exception {
        String sql = "show all;";
        String expected = "SHOW ALL";
        Class<?> type = PGShowStatement.class;
        testParseSQL(sql, expected, type);

        sql = "show transaction_read_only;";
        expected = "SHOW transaction_read_only";
        type = PGShowStatement.class;
        testParseSQL(sql, expected, type);
    }

    @Test public void testCommit() throws Exception {
        String sql = "commit;";
        String expected = "COMMIT";
        Class<?> type = SQLCommitStatement.class;
        testParseSQL(sql, expected, type);
    }

    @Test public void testSet() throws Exception {
        String sql = "SET TIME ZONE 'Europe/Rome';";
        String expected = "SET TIME ZONE 'Europe/Rome'";
        Class<?> type = SQLSetStatement.class;
        testParseSQL(sql, expected, type);

        sql = "SET configuration_parameter TO DEFAULT;";
        expected = "SET configuration_parameter TO DEFAULT";
        type = SQLSetStatement.class;
        testParseSQL(sql, expected, type);

        sql = "SET search_path TO my_schema, public;";
        expected = "SET search_path TO my_schema, public";
        type = SQLSetStatement.class;
        testParseSQL(sql, expected, type);

        sql = "SET search_path =  my_schema, public;";
        expected = "SET search_path TO my_schema, public";
        type = SQLSetStatement.class;
        testParseSQL(sql, expected, type);

        sql = "SET a=1";
        expected = "SET a TO 1";
        type = SQLSetStatement.class;
        testParseSQL(sql, expected, type);

        sql = "SET a=1,2";
        expected = "SET a TO 1, 2";
        type = SQLSetStatement.class;
        testParseSQL(sql, expected, type);
    }

    @Test public void testSelectAsIntersect() throws Exception {
        String sql =
            "select ST_ASTEXT(ST_Intersection(service_range, ST_GeomFromText('POLYGON((1))', 4326))) AS intersect, ST_AREA(ST_Intersection(service_range, ST_GeomFromText('POLYGON((1))', 4326))::geography) AS area FROM tb_carrier where platform_id='1' and ST_CONTAINS(service_range, ST_GeomFromText('POINT(1)',4326));";
        String expected =
            "SELECT ST_ASTEXT(ST_Intersection(service_range, ST_GeomFromText('POLYGON((1))', 4326))) AS intersect , ST_AREA(ST_Intersection(service_range, ST_GeomFromText('POLYGON((1))', 4326))::geography) AS area FROM tb_carrier WHERE platform_id = '1' AND ST_CONTAINS(service_range, ST_GeomFromText('POINT(1)', 4326))";
        Class<?> type = PGSelectQueryBlock.class;
        testParseSQL(sql, expected, type);
    }

    @Test public void testClairSQL() throws Exception {
        String[] selects = {"SELECT ST_IsValid(ST_GeomFromText('POLYGON((1))', 4236)) AS isvalid",
            "SELECT id FROM tb_carrier where platform_id = '1' and carrier_id = '1'",
            "SELECT ST_ASTEXT(ST_Intersection(service_range, ST_GeomFromText('POLYGON((1))', 4326))) AS intersect, ST_AREA(ST_Intersection(service_range, ST_GeomFromText('POLYGON((1))', 4326))::geography) AS area FROM tb_carrier where platform_id='1' and ST_CONTAINS(service_range, ST_GeomFromText('POINT(1)',4326))",
            "SELECT ST_ASTEXT(ST_Intersection(service_range, ST_GeomFromText('POLYGON((1))', 4326))) AS intersect FROM tb_carrier where platform_id='1' and ST_CONTAINS(service_range, ST_GeomFromText('POINT(1)',4326))",
            "SELECT COUNT(id) as count FROM tb_carrier where ST_CONTAINS(service_range, ST_GeomFromText('POINT(1)',4326))",
            "SELECT id FROM tb_static_target WHERE target_type = 1 and target_id = '1'",
            "SELECT target_id from tb_static_target where target_type = 1 AND ST_Contains(range, ST_GeomFromText('POINT(1)', 4326)) and ST_DWithin(ST_GeomFromText('POINT(1)', 4326)::geography, location::geography, 1) LIMIT 100",
            "SELECT target_id from tb_static_target where target_type = 1 AND ST_Intersects(range, ST_GeomFromText('POLYGON((1))', 4326)) LIMIT 100",
            "SELECT target_id from tb_static_target where target_type = 1 AND ST_Contains(range, ST_GeomFromText('POINT(1)', 4326)) LIMIT 100",
            "SELECT id from tb_target where app_id = $1 and target_desc = $2 order by id desc limit 1",
            "SELECT detail FROM tb_trace WHERE target_type = 1 AND target_id = '1' AND (detail->>'utc')::int >= 1 AND (detail->>'utc')::int <= 1",
            "SELECT detail FROM tb_trace WHERE target_type = 1 AND target_id = '1' AND created_at >= 1 AND created_at<= 1",
            "SELECT detail FROM tb_trace_archive WHERE target_type = 1 AND target_id = '1' AND created_at >= 1 AND created_at<= 1",
            "SELECT detail FROM tb_trace WHERE target_type = 1 AND target_id = '1' AND created_at >= 1 AND created_at<= 1 AND (detail->>'utc')::int >= 1 AND (detail->>'utc')::int <= 1",
            "SELECT id FROM tb_active_target where target_type = 1 AND target_id = '1' ",
            "SELECT target_id from tb_active_target where target_type = 1 AND ST_DWithin(location::geography, ST_GeomFromText('POINT(1 1)', 4326)::geography, 1)",
            "SELECT ST_AsText(location) as location, to_char(updated_at, 'Dy, DD Mon YYYY HH24:MI:SS TZ') as updated_at FROM tb_active_target where target_type = 1 AND target_id = '1' ",
            "SELECT target_id, ST_AsText(location) as location, to_char(updated_at, 'Dy, DD Mon YYYY HH24:MI:SS TZ') as updated_at from tb_active_target where target_type = 1 AND ST_DWithin(location::geography, ST_GeomFromText('1', 4326)::geography, 1) order by ST_DISTANCE(location::geography, ST_GeomFromText('1', 4326)::geography) limit 1",
            "SELECT target_id, ST_AsText(location) as location, to_char(updated_at, 'Dy, DD Mon YYYY HH24:MI:SS TZ') as updated_at from tb_active_target where target_type = 1 AND ST_DWithin(location::geography, ST_GeogFromText('1'), 1, false) order by ST_DISTANCE(location::geography, ST_GeogFromText('1')) limit 1",};

        String[] inserts = {"INSERT INTO tb_target (app_id, target_desc) VALUES ($1, $2)",
            "INSERT INTO tb_trace (zone_id, created_at, city_id, target_type, target_id, detail) VALUES (1, 1, 1, 1, '1', '1'::jsonb)",
            "INSERT INTO tb_active_target (zone_id, city_id, target_type, target_id, location) VALUES (1, 1, 1, '1', ST_GeomFromText('POINT(1 1)', 4326))",
            "INSERT INTO tb_carrier (platform_id, carrier_id, service_range) VALUES ('1', '1', ST_GeomFromText('POLYGON((1))', 4326))",
            "INSERT INTO tb_static_target (target_type, target_id, location, range) VALUES (1, '1', ST_GeomFromText('POINT(1)', 4326), ST_GeomFromText('POLYGON((1))', 4326))",};
        String[] updates =
            {"UPDATE tb_active_target SET location = ST_GeomFromText('POINT(1 1)', 4326), updated_at = 1 where id = 1",
                "UPDATE tb_active_target SET location = ST_GeomFromText('POINT(1 1)', 4326), updated_at = 1, zone_id = 1, city_id = 1 where target_type = 1 AND target_id = '1'",
                "UPDATE tb_static_target SET location = ST_GeomFromText('POINT(1)', 4326), range = ST_GeomFromText('POLYGON((1))', 4326), updated_at = 1 where id = 1",
                "UPDATE tb_carrier set service_range = ST_GeomFromText('POLYGON((1))', 4326) where platform_id = '1' and carrier_id = '1'",};
        String[] deletes =
            {"DELETE FROM tb_static_target WHERE target_type = 1 and target_id = '1'",
                "DELETE from tb_active_target where target_type = 1 AND target_id = '1' ",
                "DELETE from tb_active_target where updated_at <= 1",
                "DELETE FROM tb_carrier where platform_id = '1' and carrier_id = '1'",};
        for (String sql : selects) {
            ShardingSQL shardingSQL = ShardingSQL
                .handlePGBasicly(sql, "", ShardingRouter.defaultShardingRouter,
                    new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
            assertTrue(shardingSQL.sqlStmt instanceof PGSelectQueryBlock);
        }
        for (String sql : inserts) {
            ShardingSQL shardingSQL = ShardingSQL
                .handlePGBasicly(sql, "", ShardingRouter.defaultShardingRouter,
                    new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
            assertTrue(shardingSQL.sqlStmt instanceof PGInsertStatement);
        }
        for (String sql : updates) {
            ShardingSQL shardingSQL = ShardingSQL
                .handlePGBasicly(sql, "", ShardingRouter.defaultShardingRouter,
                    new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
            assertTrue(shardingSQL.sqlStmt instanceof PGUpdateStatement);
        }
        for (String sql : deletes) {
            ShardingSQL shardingSQL = ShardingSQL
                .handlePGBasicly(sql, "", ShardingRouter.defaultShardingRouter,
                    new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
            assertTrue(shardingSQL.sqlStmt instanceof PGDeleteStatement);
        }
    }
}
