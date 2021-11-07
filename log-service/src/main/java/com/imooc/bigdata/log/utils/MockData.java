package com.imooc.bigdata.log.utils;

import com.imooc.bigdata.log.domain.Access;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MockData {

    public static final String url = "http://localhost:9527/pk-web/upload";

    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

    @Test
    public void testUpload() throws Exception {

        for (int i=1; i<=10; i++) {
            Thread.sleep(100);
            Access access = new Access();
            access.setId(i);
            access.setName("name" + i);
            access.setTime(format.format(new Date()));

            UploadUtils.upload(url, access.toString());
        }
    }
}
