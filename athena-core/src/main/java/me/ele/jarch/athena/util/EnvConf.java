package me.ele.jarch.athena.util;

public class EnvConf {
    private final String idc;

    private volatile static EnvConf instance;

    private EnvConf() {
        idc = System.getProperty("IDC", "dev");
    }

    public String idc() {
        return idc;
    }

    public static EnvConf get() {
        if (instance == null) {
            synchronized (EnvConf.class) {
                if (instance == null)
                    instance = new EnvConf();
            }
        }
        return instance;
    }
}
