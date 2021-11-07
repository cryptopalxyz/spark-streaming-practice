package com.imooc.bigdata.controller;



import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;



/*
日志服务

upload=>磁盘
      => 1. console
      => 2. land to disk

 */
@RestController
@Slf4j
public class LogController {

    private static final Logger logger = Logger.getLogger(LogController.class);
    @PostMapping("/upload")
    @ResponseBody
    public void upload(@RequestBody String info) {

        logger.info( info);
        //logger.error("error");

    }

}
