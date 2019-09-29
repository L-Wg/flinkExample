package cn.edu.seu.flink.write2Hbase;

import org.apache.log4j.Logger;

public class UserBehaviorSchema {

    public long userId;
    public long itemId;
    public int categoryId;
    public String behavior;
    public long timestamp;

    private static Logger logger=Logger.getLogger(UserBehaviorSchema.class);

    public UserBehaviorSchema(){}

    public UserBehaviorSchema(long userId,long itemId,int categoryId,
                              String behavior,long timestamp){
        this.userId=userId;
        this.itemId=itemId;
        this.categoryId=categoryId;
        this.behavior=behavior;
        this.timestamp=timestamp;
    }

    public static UserBehaviorSchema fromString(String message){

        String[] mgs=message.split(",");
        return new UserBehaviorSchema(Long.valueOf(mgs[0]),Long.valueOf(mgs[1]),
                Integer.valueOf(mgs[2]),mgs[3],Long.valueOf(mgs[4]));
    }

    public String toString(){
        return userId+","+itemId+","+categoryId+","+behavior+","+timestamp;
    }
}
