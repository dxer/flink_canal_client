package org.dxer.flink.canal.client;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The deserialization schema describes how to turn the Kafka ConsumerRecords
 * into data types (Java/Scala objects) that are processed by Flink.
 * key, value, topic, partition, offset
 */
public class CustomKafkaDeserializationSchema implements KafkaDeserializationSchema<Tuple5<String, String, String, Integer, Long>> {

    @Override
    public boolean isEndOfStream(Tuple5<String, String, String, Integer, Long> nextElement) {
        return false;
    }

    /**
     * key, value, topic, partition, offset
     *
     * @param record
     * @return
     * @throws Exception
     */
    @Override
    public Tuple5<String, String, String, Integer, Long> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        SimpleStringSchema stringSchema = new SimpleStringSchema();

        String key = null;
        String value = null;

        if (record.key() != null) {
            key = stringSchema.deserialize(record.key());
        }

        if (record.value() != null) {
            value = stringSchema.deserialize(record.value());
        }

        return new Tuple5<>(key, value, record.topic(), record.partition(), record.offset());
    }

    @Override
    public TypeInformation<Tuple5<String, String, String, Integer, Long>> getProducedType() {
        return new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }
}
