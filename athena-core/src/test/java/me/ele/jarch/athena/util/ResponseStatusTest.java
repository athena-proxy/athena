package me.ele.jarch.athena.util;

import com.github.mpjct.jmpjct.util.ErrorCode;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by jinghao.wang on 16/1/11.
 */
public class ResponseStatusTest {

    @Test public void testInstance() throws Exception {
        ResponseStatus responseStatus = new ResponseStatus(ResponseStatus.ResponseType.OK);
        ResponseStatus responseStatus1 = new ResponseStatus(ResponseStatus.ResponseType.OK);
        Assert.assertNotSame(responseStatus, responseStatus1);
        Assert.assertTrue(responseStatus.equals(responseStatus1));
    }

    @Test public void testGetResponseType() throws Exception {
        ResponseStatus responseStatus = new ResponseStatus(ResponseStatus.ResponseType.OK);
        Assert.assertTrue(ResponseStatus.ResponseType.OK == responseStatus.getResponseType());
        Assert.assertFalse(ResponseStatus.ResponseType.DBERR == responseStatus.getResponseType());
    }

    @Test public void testToString() throws Exception {
        ResponseStatus responseStatus = new ResponseStatus(ResponseStatus.ResponseType.ABORT)
            .setCode(ErrorCode.ABORT_SERVER_UNSYNC).setMessage("cdba");
        ResponseStatus responseStatus1 = new ResponseStatus(ResponseStatus.ResponseType.ABORT)
            .setCode(ErrorCode.ABORT_SERVER_UNSYNC).setMessage("abcd");
        System.out.println(responseStatus);
        System.out.println(responseStatus1);
        Assert.assertFalse(responseStatus.equals(responseStatus1));

        ResponseStatus responseStatus2 = new ResponseStatus(ResponseStatus.ResponseType.OK)
            .setCode(ErrorCode.ABORT_SERVER_UNSYNC).setMessage("cdba");
        ResponseStatus responseStatus3 = new ResponseStatus(ResponseStatus.ResponseType.OK)
            .setCode(ErrorCode.ABORT_SERVER_UNSYNC).setMessage("cdba");
        System.out.println(responseStatus2);
        System.out.println(responseStatus3);
        Assert.assertEquals(responseStatus2, responseStatus3);

        ResponseStatus responseStatus4 = new ResponseStatus(ResponseStatus.ResponseType.OK)
            .setCode(ErrorCode.ABORT_SERVER_UNSYNC).setMessage("cdba");
        ResponseStatus responseStatus5 = new ResponseStatus(ResponseStatus.ResponseType.DALERR)
            .setCode(ErrorCode.ABORT_SERVER_UNSYNC).setMessage("cdba");
        System.out.println(responseStatus4);
        System.out.println(responseStatus5);
        Assert.assertFalse(responseStatus4.equals(responseStatus5));
    }
}
