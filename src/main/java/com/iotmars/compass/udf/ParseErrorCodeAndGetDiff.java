package com.iotmars.compass.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * @author CJ
 * @date: 2022/11/3 20:31
 */
@FunctionHint(output = @DataTypeHint("ROW<newFaultCode STRING>"))
public class ParseErrorCodeAndGetDiff extends TableFunction<Row> {
    public void eval(String event_real_value, String event_real_ori_value) {
        try {
            Long newErrorCode = Long.parseLong(event_real_value);
            ArrayList<String> newFaultCodeList = parseErrorCode(newErrorCode);

            Long oldErrorCode = Long.parseLong(event_real_ori_value);
            ArrayList<String> oldFaultCodeList = parseErrorCode(oldErrorCode);

            // 返回新增的异常值
            for (String newFaultCode : newFaultCodeList) {
                if (!oldFaultCodeList.contains(newFaultCode)) {
                    collect(Row.of(newFaultCode));
                }
            }

        } catch (Exception e) {
            Logger.getLogger("ParseErrorCodeAndGetDiff").warn("解析错误代码异常: " + event_real_value + "  " + event_real_ori_value);
        }
    }

    private ArrayList<String> parseErrorCode(Long code) {
        ArrayList<String> list = new ArrayList<>();

        long exp = 0L;
        long value = 0L;
        while (code > value) {
            // 通过比较
            value = Double.valueOf(Math.pow(2D, exp)).longValue();
            long calc = code & value;
            // 1为E1,2为E2,4为E3,8192为E12
            if (calc > 0L) {
                list.add(String.valueOf(exp + 1));
            }
            ++exp;
        }

        // 转化为指数上标的集合
        return list;
    }

}
