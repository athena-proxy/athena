package me.ele.jarch.athena.detector;

/**
 * Created by Dal-Dev-Team on 17/3/10.
 */
public class DetectUtil {
    public static double div(double a, long b) {
        return a / (b + 0.001d);
    }

    public static double div(double a, double b) {
        return a / (b + 0.001d);
    }
}
