package com.huyida.hotproblemanalysis;

import com.huyida.hotproblemanalysis.domain.ItemViewCount;
import com.huyida.hotproblemanalysis.util.DbUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

/**
 * @program: HotProblemAnalysis
 * @description:
 * @author: huyida
 * @create: 2022-06-20 20:17
 **/

public class MySqlSink extends RichSinkFunction<ItemViewCount> {

    private PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取数据库连接，准备写入数据库
        connection = DbUtils.getConnection();
        String sql = "insert into itembuycount(itemId, buyCount, createDate) values (?, ?, ?); ";
        ps = connection.prepareStatement(sql);
        System.out.println("-------------open------------");
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭并释放资源
        if (connection != null) {
            connection.close();
        }

        if (ps != null) {
            ps.close();
        }
        System.out.println("-------------close------------");
    }

    public void invoke(List<ItemViewCount> topNItems, Context context) throws Exception {
        for (ItemViewCount itemBuyCount : topNItems) {
            ps.setString(1, itemBuyCount.getItemId());
            ps.setLong(2, itemBuyCount.getCount());
            ps.setTimestamp(3, new Timestamp(itemBuyCount.getWindowEnd()));
            ps.addBatch();
        }

        //一次性写入
        int[] count = ps.executeBatch();
        System.out.println("-------------invoke------------");
        System.out.println("成功写入Mysql数量：" + count.length);

    }
}
