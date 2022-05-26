package com.gzl0ng.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 郭正龙
 * @date 2021-10-22
 */
public class MySink extends AbstractSink implements Configurable {
    //声明数据的前缀后缀
    private String prefix;
    private String subfix;

    //创建Looger对象
    private Logger logger = LoggerFactory.getLogger(MySink.class);
    @Override
    public Status process() throws EventDeliveryException {
        //1.获取channel并开启事务
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        //2.从channel中抓取数据打印到控制台
        try {

            //2.抓取数据(可能为空)
            Event event;
            while (true){
                event = channel.take();
                if (event != null){
                    break;
                }
            }

            //2.2处理数据(debug,info,warn,error,trace)
            logger.info(prefix + new String(event.getBody())+ subfix);

            //2.3提交事务
            transaction.commit();
            return Status.READY;
        }catch (Exception e){
            //回滚
            transaction.rollback();
            return Status.BACKOFF;
        }finally {
            transaction.close();
        }
    }

    @Override
    public void configure(Context context) {
        //pre sub delay都是在conf文件中读取
        prefix = context.getString("pre","pre-");
        subfix = context.getString("sub");
    }
}
