package me.ele.jarch.athena.util.health.check;

/**
 * Created by Tsai on 17/10/30.
 */
public enum DalGroupHealthCheckStatus {
    //@formatter:off
    /* 自检成功 */
    HEART_BEAT_SUCCESS,
    /* 通过DAL奇数端口自检home库失败 */
    SLAVE_HEART_BEAT_FAILED,
    /* 通过DAL偶数端口自检home库失败 */
    MASTER_HEART_BEAT_FAILED,
    /* 通过DAL奇数端口自检sharding库失败 */
    SHARDING_SLAVE_HEART_BEAT_FAILED,
    /* 通过DAL偶数端口自检sharding库失败 */
    SHARDING_MASTER_HEART_BEAT_FAILED,
    /* DAL服务端口未开启 */
    NO_SERVICE_PORT
    //@formatter:on
}
