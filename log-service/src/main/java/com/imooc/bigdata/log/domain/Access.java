package com.imooc.bigdata.log.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
public class Access {

    private int id;
    private String name;
    private String time;

    @Override
    public String toString() {
        return
                "" + id +
                " " + name +
                " " + time ;
    }
}
