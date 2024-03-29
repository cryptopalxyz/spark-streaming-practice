package com.imooc.spark.sss.project;

import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbMakerConfigException;
import org.lionsoul.ip2region.DbSearcher;
import java.io.FileNotFoundException;
import java.io.IOException;

public class IPUtils {

    public static String parseIP(String ip) {
        String result = "";
        String dbFile = IPUtils.class.getClassLoader().getResource("ip2region.db").getPath();
        DbSearcher search = null;
        try {
            search = new DbSearcher(new DbConfig(), dbFile);
            DataBlock dataBlock = search.btreeSearch(ip);
            String region = dataBlock.getRegion();
            String replace = region.replace("|", ",");
            String[] splits = replace.split(",");
            if (splits.length == 5) {
                result = splits[2];
            }
            return result;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(search!=null) search.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public static void main(String[] args) {
        String detail = IPUtils.parseIP("210.51.167.169");
        System.out.println(detail);
    }
}
