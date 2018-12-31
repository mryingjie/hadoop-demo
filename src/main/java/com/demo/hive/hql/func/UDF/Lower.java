package com.demo.hive.hql.func.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/31
 * @Description
 */
public class Lower extends UDF {



    public String evaluate (final String s) {

        if (s == null) {
            return null;
        }
        return s.toLowerCase();
    }

}
