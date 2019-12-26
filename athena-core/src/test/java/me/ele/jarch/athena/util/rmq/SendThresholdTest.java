package me.ele.jarch.athena.util.rmq;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by rui.wang07 on 2017/9/30.
 */
public class SendThresholdTest {
    @Test public void testParseThresholds() throws Exception {
        assertEquals(SendThreshold.THRES_0.threshold, 1000);
        assertEquals(SendThreshold.THRES_10.threshold, 100);
        assertEquals(SendThreshold.THRES_100.threshold, 10);
        assertEquals(SendThreshold.THRES_1000.threshold, 1);

        String thresholds = "THRES_0:2000;Thres_10:200;thres_100:20;thREs_1000:2;";
        SendThreshold.parseThresholds(thresholds);
        assertEquals(SendThreshold.THRES_0.threshold, 2000);
        assertEquals(SendThreshold.THRES_10.threshold, 200);
        assertEquals(SendThreshold.THRES_100.threshold, 20);
        assertEquals(SendThreshold.THRES_1000.threshold, 2);

        thresholds = "thres_0:3000;thRES_10:300;thres_100:30;thres_100:3";
        SendThreshold.parseThresholds(thresholds);
        assertEquals(SendThreshold.THRES_0.threshold, 2000);
        assertEquals(SendThreshold.THRES_10.threshold, 200);
        assertEquals(SendThreshold.THRES_100.threshold, 20);
        assertEquals(SendThreshold.THRES_1000.threshold, 2);

        thresholds = "thres_0:3000;thres_20:300;thres_100:30;thres_800:3";
        SendThreshold.parseThresholds(thresholds);
        assertEquals(SendThreshold.THRES_0.threshold, 2000);
        assertEquals(SendThreshold.THRES_10.threshold, 200);
        assertEquals(SendThreshold.THRES_100.threshold, 20);
        assertEquals(SendThreshold.THRES_1000.threshold, 2);
    }

}
