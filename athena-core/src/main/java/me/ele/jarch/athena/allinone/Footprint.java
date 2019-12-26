package me.ele.jarch.athena.allinone;

/**
 * 枚举类，枚举了SQL在DAL内部的路由选择方式
 * <p>
 * Created by peng.tan on 17/9/23.
 */
public enum Footprint {
    UNKNOWN_FOOTPRINT, AUTO_PORT,      //单端口传入，正常的读写分离逻辑
    BIND_MASTER,    //单端口传入，SQL注入了 bind_master
    MASTER_PORT,    //双端口
    REDIRECT2MASTER, //单端口传入，无有效SLAVE，被DAL重定向到MASTER
    MANUAL_REDIRECT2MASTER  //单数端口传入,通过ZK配置指定SQL hash被DAL重定向到MASTER
}
