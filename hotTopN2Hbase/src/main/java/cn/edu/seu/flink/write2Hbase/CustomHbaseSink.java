package cn.edu.seu.flink.write2Hbase;

import cn.edu.seu.flink.write2Hbase.util.HbaseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 该类自定义sink。
 * <p>继承<code>RichSinkFunction</code>重写父类方法。
 *
 * @author L_Wang
 */

public class CustomHbaseSink extends RichSinkFunction<List<ResultEvent>> {

    private static final Logger logger=Logger.getLogger(CustomHbaseSink.class);

    private Connection connection=null;
    private Table table=null;
    private ArrayList<Put> putArrayList;
    private BufferedMutatorParams params;
    private BufferedMutator mutator;

    private String cf="info";
    private String tableName="flink2Hbase";
    private String bootList;

    public CustomHbaseSink(){}
    public CustomHbaseSink(String bootList){
        this.bootList=bootList;
    }

//  只调用一次
    @Override
    public void open(Configuration configuration) throws Exception {
        logger.debug("********call open()*********");
        super.open(configuration);
        HbaseUtil.setConfig(bootList);
        connection=HbaseUtil.connection;
        params=new BufferedMutatorParams(TableName.valueOf(tableName));
        params.writeBufferSize(1024*1024);
        mutator=connection.getBufferedMutator(params);
        putArrayList=new ArrayList<>();
    }

//  每条消息都调用<i>invoke</i>
    @Override
    public void invoke(List<ResultEvent> events,Context ctx) throws IOException {

        for(ResultEvent event:events){
            Put put=new Put(Bytes.toBytes(String.valueOf(event.windowEndTime)+String.valueOf(event.ranking)));
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("itemId"),Bytes.toBytes(event.itemId));
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("countPv"),Bytes.toBytes(event.countPv));
            putArrayList.add(put);
        }

        mutator.mutate(putArrayList);
        if(putArrayList.size()>200){
            mutator.flush();
            putArrayList.clear();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(table!=null){
            table.close();
        }
        if(connection!=null){
            connection.close();
        }
    }

}
