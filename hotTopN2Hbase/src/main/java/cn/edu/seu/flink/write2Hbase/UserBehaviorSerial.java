package cn.edu.seu.flink.write2Hbase;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class UserBehaviorSerial implements SerializationSchema<UserBehaviorSchema>,
        DeserializationSchema<UserBehaviorSchema> {


    @Override
    public UserBehaviorSchema deserialize(byte[] bytes) throws IOException {
        return UserBehaviorSchema.fromString(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(UserBehaviorSchema userBehaviorSchema) {
        return false;
    }

    @Override
    public byte[] serialize(UserBehaviorSchema userBehaviorSchema) {
        return userBehaviorSchema.toString().getBytes();
    }

    @Override
    public TypeInformation<UserBehaviorSchema> getProducedType() {
        return TypeInformation.of(UserBehaviorSchema.class);
    }
}
