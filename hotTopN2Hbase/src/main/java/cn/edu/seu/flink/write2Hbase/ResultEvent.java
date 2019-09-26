package cn.edu.seu.flink.write2Hbase;

public class ResultEvent {

    public long itemId;
    public long countPv;
    public long windowEndTime;
    public long ranking=0;


    public static ResultEvent of(long itemId,long countPv,long windowEndTime){
        ResultEvent resultEvent=new ResultEvent();
        resultEvent.itemId=itemId;
        resultEvent.countPv=countPv;
        resultEvent.windowEndTime=windowEndTime;
        return resultEvent;
    }
}
