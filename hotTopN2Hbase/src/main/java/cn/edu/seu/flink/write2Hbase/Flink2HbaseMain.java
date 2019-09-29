package cn.edu.seu.flink.write2Hbase;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 * 这个简单的项目是想实现：将kafka中的数据通过Flink处理后，写入到HBase中。
 * 其中Flink处理数据的逻辑是参考了阿里吴翀博文《Flink 零基础实战教程：如何计算实时热门商品》一文。
 *
 * <p>Example usage:
 * --topic test-topic ----bootstrap.servers localhost:9092 --hbase.zookeeper.quorum localhost:2181
 *
 * @author L_Wang
 */
public class Flink2HbaseMain {

    private static Logger logger=Logger.getLogger(Flink2HbaseMain.class);


    public static void main(String[] args) throws Exception {

        ParameterTool params=ParameterTool.fromArgs(args);
        if(params.getNumberOfParameters()<3){
            System.out.println("-------------------------------------------");
            System.out.println("------------------Usage: Please specify --topic $topic," +
                    "--bootstrap.servers $bootstrap.servers," +
                    "--hbase.zookeeper.quorum $hbase.zookeeper.quorum" +
                    "----------------");
            return;
        }

        String topic=params.getRequired("topic");
        Properties properties=new Properties();
        properties.put("bootstrap.servers",params.getRequired("bootstrap.servers"));
        String bootList=params.getRequired("hbase.zookeeper.quorum");

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);
        env.setStateBackend(new FsStateBackend("hdfs://nameservice/flink/checkpoints"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(30000);

        logger.debug("set config sucessfully!");

//      flink的处理是不影响kafka中的源数据的，此外，需注意的是dataStream的数据也是不变的。
        DataStream<UserBehaviorSchema> dataStream=env.addSource(new FlinkKafkaConsumer010<UserBehaviorSchema>(
                topic,
                new UserBehaviorSerial(),
                properties
        ));

        DataStream<List<ResultEvent>> resultSource=dataStream.filter(new FilterFunction<UserBehaviorSchema>() {
            @Override
            public boolean filter(UserBehaviorSchema userBehaviorSchema) throws Exception {
                return userBehaviorSchema.behavior.equals("pv");
            }
        })
                .assignTimestampsAndWatermarks(new customWaterExtractor())
                .keyBy("itemId")
                .timeWindow(Time.minutes(60),Time.minutes(5))
                .aggregate(new customAggFunction(),new customWindowFunciton())
                .keyBy("windowEndTime")
                .process(new TopNItem(3));

        resultSource.addSink(new CustomHbaseSink(bootList));

        env.execute("write data from kafka to hbase with flink!");
    }

    /*----------------------------------------------*/
    public static class customWaterExtractor implements AssignerWithPeriodicWatermarks<UserBehaviorSchema>{

        private static final long serialVersionUID = 298015256202705122L;

        private final long maxOutOrderness=3500;
        private long currentTimeStamp=Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimeStamp-maxOutOrderness);
        }

        @Override
        public long extractTimestamp(UserBehaviorSchema element, long previousElementTimestamp) {
//          此处需要注意的点：timestamp得到的值是否乘以1000转换为毫秒，需要根据消息中被指定为timestamp字段的单位。
            long timeStamp=element.timestamp*1000;
            currentTimeStamp=Math.max(timeStamp,currentTimeStamp);
            return timeStamp;
        }
    }

    public static class customAggFunction implements AggregateFunction<UserBehaviorSchema,Long,Long>{

        private static final long serialVersionUID = -2717758404394680445L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehaviorSchema userBehaviorSchema, Long acc) {
            return acc+1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc, Long acc1) {
            return acc+acc1;
        }
    }

    public static class customWindowFunciton implements WindowFunction<Long,
            ResultEvent, Tuple, TimeWindow>{

        private static final long serialVersionUID = -7043268250252228452L;

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ResultEvent> out) throws Exception {
            Long itemId= ((Tuple1<Long>)tuple).f0;
            Long count=input.iterator().next();
            out.collect(ResultEvent.of(itemId,count,window.getEnd()));
        }
    }


/*-----------------------------------------------------*/

    public static class TopNItem extends KeyedProcessFunction<Tuple, ResultEvent, List<ResultEvent>> {

        private static final long serialVersionUID = 94453630467293687L;
        private int topSize;

        public TopNItem(int topSize){
            this.topSize=topSize;
        }

        private ListState<ResultEvent> itemState;

        @Override
        public void open(Configuration config) throws Exception {
            super.open(config);
            ListStateDescriptor<ResultEvent> listState=new ListStateDescriptor<ResultEvent>(
                    "itemState_list",
                    ResultEvent.class
            );
            itemState=getRuntimeContext().getListState(listState);

        }

        @Override
        public void processElement(ResultEvent value, Context ctx, Collector<List<ResultEvent>> out) throws Exception {

            itemState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEndTime+1);
        }

        /**
         * 这里重载了<code>KeyedProcessFunction</code>中的<i>onTimer</i>,将一个窗口之间的前三名
         * 商品信息写入到一个<code>List</code>中，以便发出。
         *
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp,OnTimerContext ctx,Collector<List<ResultEvent>> out) throws Exception {

            List<ResultEvent> allItems=new ArrayList<>();
            for(ResultEvent item:itemState.get()){
                allItems.add(item);
            }

            itemState.clear();

            allItems.sort(new Comparator<ResultEvent>() {
                @Override
                public int compare(ResultEvent o1, ResultEvent o2) {
                    return (int)(o2.countPv-o1.countPv);
                }
            });

            List<ResultEvent> list=new ArrayList<>(topSize);
            for(int i=0;i<topSize&i<allItems.size();i++){
                allItems.get(i).ranking=i;
                list.add(allItems.get(i));
            }
            out.collect(list);
        }
    }
}
