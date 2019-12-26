package me.ele.jarch.athena.util;

import ch.qos.logback.core.rolling.RollingFileAppender;

public class AthenaFileAppender<E> extends RollingFileAppender {

    @Override public void setFile(String file) {
        if (file == null) {
            this.fileName = file;
        } else {
            this.fileName = file.trim();
        }
    }
}
