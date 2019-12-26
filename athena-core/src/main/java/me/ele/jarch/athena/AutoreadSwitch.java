package me.ele.jarch.athena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by avenger on 2016/12/15.
 */
public class AutoreadSwitch {
    private static final Logger logger = LoggerFactory.getLogger(AutoreadSwitch.class);
    public static final String AUTOREAD_ON = "autoread_on";
    public static final String AUTOREAD_OFF = "autoread_off";
    public static final AtomicBoolean autoreadOn = new AtomicBoolean(true);

    public static void toggleAutoreadSwitch(boolean isOn) {
        AutoreadSwitch.autoreadOn.set(isOn);
        String type = isOn ? AUTOREAD_ON : AUTOREAD_OFF;
        logger.warn(type + " finished");
    }
}
