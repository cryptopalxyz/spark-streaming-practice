package com.imooc.bigdata.log.utils;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class UploadUtils {

    public static void upload(String path, String log) {

        try {
            URL url = new URL(path);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type","application/json");

            OutputStream out = conn.getOutputStream();
            out.write(log.getBytes());
            out.flush();
            out.close();

            int code = conn.getResponseCode();
            System.out.println("response code is " + code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
