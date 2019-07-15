package com.alibaba.dts.subscribe;

import com.alibaba.dts.formats.avro.Character;
import com.alibaba.dts.formats.avro.Integer;
import com.alibaba.dts.formats.avro.*;
import com.alibaba.dts.subscribe.positioner.MemoryPositioner;
import com.alibaba.dts.subscribe.positioner.Positioner;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Client {

    private static final Logger log = LoggerFactory.getLogger(Client.class);

    private Positioner positioner;

    private RdsSubscribeProperties rdsSubscribeProperties;

    private List<Listener> listeners;

    private KafkaConsumer<String, byte[]> consumer;

    private DatumReader<Record> reader = new SpecificDatumReader<>(Record.class);

    private long lastCommitTime = System.currentTimeMillis();

    private AtomicLong reloadTime = new AtomicLong(0);

    private boolean isPolled = false;

    private AtomicBoolean isClosed = new AtomicBoolean(true);

    private ZoneOffset zoneOffset = OffsetDateTime.now().getOffset();

    private ObjectMapper objectMapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);


    public Client(RdsSubscribeProperties rdsSubscribeProperties, List<Listener> listeners) {
        this(new MemoryPositioner(rdsSubscribeProperties), listeners);
    }

    public Client(Positioner positioner, List<Listener> listeners) {
        this.positioner = positioner;
        this.listeners = new ArrayList<>(listeners);
        init();
    }

    private void init() {
        this.rdsSubscribeProperties = positioner.loadRdsSubscribeProperties();
    }


    private OffsetAndTimestamp fetchOffsetByTime(KafkaConsumer<String, byte[]> consumer,
                                                 TopicPartition partition,
                                                 Long startTime) {
        Map<TopicPartition, Long> query = new HashMap<>();
        query.put(partition, startTime);

        final Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(query);
        if (offsetResult == null || offsetResult.isEmpty()) {
            log.error(" No Offset to Fetch");
            return null;
        }

        //test
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> m : offsetResult.entrySet()) {
            log.info("key:" + m.getKey() + " value:" + m.getValue());
        }


        final OffsetAndTimestamp offsetTimestamp = offsetResult.get(partition);
        if (null == offsetTimestamp) {
            log.error("No Offset Found for partition :" + partition.partition());
        }

        return offsetTimestamp;
    }

    private boolean assignOffsetToConsumer(KafkaConsumer<String, byte[]> consumer, String topic, Long startTime) {
        Set<TopicPartition> assignment = consumer.assignment();
        final List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        if (partitionInfoList.isEmpty()) {
            log.warn("topic:" + topic + " no partition");
            return false;
        }
        log.info("Number of Partitions : " + partitionInfoList.size());

        final List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo pInfo : partitionInfoList) {
            TopicPartition partition = new TopicPartition(topic, pInfo.partition());
            topicPartitions.add(partition);
        }
        consumer.assign(topicPartitions);
        for (TopicPartition partition : topicPartitions) {
            OffsetAndTimestamp offSetTs = fetchOffsetByTime(consumer, partition, startTime);
            if (offSetTs == null) {
                log.warn("No Offset Found for partition : " + partition.partition());
                return false;
            } else {
                log.info(" Offset Found for partition : " + offSetTs.offset() + " " + partition.partition());
                log.info("FETCH offset success" + " Offset " + offSetTs.offset() + " offSetTs " + offSetTs);
                consumer.seek(partition, offSetTs.offset());
            }
        }

        return true;
    }

    /**
     * 开始消费topic数据
     */
    public void start() {
        if (!this.isClosed.get()) {
            throw new RuntimeException("This consumer has already been started.");
        }
        this.rdsSubscribeProperties = positioner.loadRdsSubscribeProperties();
        String topic = rdsSubscribeProperties.getTopic();
        long startTime = rdsSubscribeProperties.getStartTimeMs() / 1000;
        log.info("begin consume:topic:" + topic + ",startTime:" + startTime);
        this.consumer = new KafkaConsumer<>(buildProperties(rdsSubscribeProperties));
        if (!assignOffsetToConsumer(consumer, topic, startTime)) {
            log.error("ose assignOffsetToConsumer error,need check");
            return;
        }

        this.isClosed.set(false);
        try {
            while (!this.isClosed.get() && !Thread.interrupted()) {
                if (reloadTime.get() != 0) {
                    startTime = reloadTime.get() / 1000;
                    if (!assignOffsetToConsumer(consumer, rdsSubscribeProperties.getTopic(), startTime)) {
                        log.error("ose assignOffsetToConsumer error,need check");
                        break;
                    }
                    rdsSubscribeProperties.setStartTimeMs(reloadTime.get());
                    reloadTime.set(0);
                }
                ConsumerRecords<String, byte[]> records = consumer.poll(rdsSubscribeProperties.getPollTimeout());
                Map<Listener, List<Row>> rowMap = new LinkedHashMap<>();
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        Row row = toRow(record);
                        if (row == null) continue;
                        //将解析的数据进行消费处理
                        for (Listener listener : listeners) {
                            List<Row> rows = rowMap.computeIfAbsent(listener, k -> new ArrayList<>());
                            if (listener.match(row)) {
                                try {
                                    rows.add(row);
                                } catch (Exception e) {
                                    listener.onError(e);
                                }
                            }
                        }
                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                    }
                }

                for (Map.Entry<Listener, List<Row>> entry : rowMap.entrySet()) {
                    Listener key = entry.getKey();
                    List<Row> value = entry.getValue();
                    if (value.isEmpty()) continue;
                    try {
                        key.onNext(value);
                    } catch (Exception e) {
                        key.onError(e);
                    }
                }

                if (!this.isPolled) {
                    this.isPolled = !records.isEmpty();
                }

                long currentTime = System.currentTimeMillis();
                int autoCommitIntervalMs = this.rdsSubscribeProperties.getAutoCommitIntervalMs();
                if (this.isPolled && ((currentTime - this.lastCommitTime) > autoCommitIntervalMs)) {
                    commit();
                    this.lastCommitTime = currentTime;
                    this.isPolled = false;
                }
            }
        } catch (WakeupException e) {
            if (!isClosed.get()) throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            this.consumer.close();

            for (Listener listener : this.listeners) {
                try {
                    listener.close();
                } catch (Exception e) {
                    log.warn(e.getMessage(), e);
                }
            }

            try {
                commit();
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }

        }
    }

    private Row toRow(ConsumerRecord<String, byte[]> record) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
        GenericRecord payload = reader.read(null, decoder);
        Operation operation = ((Record) payload).getOperation();
        long offset = record.offset();
        int partition = record.partition(); //只有一个分区 0
        if (operation == Operation.INSERT || operation == Operation.DELETE || operation == Operation.UPDATE) {
            List<Field> fields = ((List<Field>) ((Record) payload).getFields());
            List<Row.Column> data = new ArrayList<>(fields.size());
            List<Row.Column> old = new ArrayList<>(fields.size());
            List<Row.Column> dataPrimaryKeys = new ArrayList<>();
            List<Row.Column> oldPrimaryKeys = new ArrayList<>();
            String[] objectName = ((Record) payload).getObjectName().toString().split("[.]");

            Row row = new Row()
                    .setDatabase(objectName[0])
                    .setTable(objectName[1])
                    .setType(Row.Type.valueOf(operation.name().toLowerCase()));

            for (Field field : fields) {
                Row.Column afterColumn = new Row.Column()
                        .setName(String.valueOf(field.getName()))
                        .setType(RdsSqlTypes.getType(field.getDataTypeNumber()).toLowerCase());
                data.add(afterColumn);
                Row.Column beforeColumn = new Row.Column()
                        .setName(String.valueOf(field.getName()))
                        .setType(RdsSqlTypes.getType(field.getDataTypeNumber()).toLowerCase());
                old.add(beforeColumn);
            }

            Map<CharSequence, CharSequence> tags = ((Record) payload).getTags();

            for (Map.Entry<CharSequence, CharSequence> entry : tags.entrySet()) {
                HashMap map = objectMapper.readValue(entry.getValue().toString(), HashMap.class);
                List<String> primaryKeys = (List<String>) map.get("PRIMARY");
                if (primaryKeys == null) break;
                for (String primaryKey : primaryKeys) {
                    for (Row.Column column : data) {
                        if (column.getName().equals(primaryKey)) {
                            column.setPrimaryKey(true);
                            dataPrimaryKeys.add(column);
                        } else {
                            column.setPrimaryKey(false);
                        }
                    }
                    for (Row.Column column : old) {
                        if (column.getName().equals(primaryKey)) {
                            column.setPrimaryKey(true);
                            oldPrimaryKeys.add(column);
                        } else {
                            column.setPrimaryKey(false);
                        }
                    }
                }
            }


            if (operation == Operation.INSERT || operation == Operation.UPDATE) {
                List<Object> afterImages = (List<Object>) ((Record) payload).getAfterImages();
                for (int i = 0; i < afterImages.size(); i++) {
                    Row.Column column = data.get(i);
                    Object value = afterImages.get(i);
                    setValue(column, value);
                }
                row.setData(data);
            }

            if (operation == Operation.DELETE || operation == Operation.UPDATE) {
                List<Object> beforeImages = (List<Object>) ((Record) payload).getBeforeImages();
                for (int i = 0; i < beforeImages.size(); i++) {
                    Row.Column column = old.get(i);
                    Object value = beforeImages.get(i);
                    setValue(column, value);
                }
                row.setOld(old);
            }

            if (operation != Operation.INSERT) {
                row.setPrimaryKeys(oldPrimaryKeys);
            } else {
                row.setPrimaryKeys(dataPrimaryKeys);
            }
            this.rdsSubscribeProperties.setStartTimeMs(record.timestamp()).setOffset(offset);
            return row;
        }
        return null;
    }

    public void asyncStart() {
        new Thread(this::start).start();
    }

    public void close() {
        if (this.isClosed.get())
            throw new RuntimeException("This consumer has already been closed.");
        try {
            if (consumer != null) {
                consumer.wakeup();
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }

        this.isClosed.set(true);
    }

    private void commit() {
//        this.consumer.commitSync(); //数据订阅无需 commit
        positioner.save(this.rdsSubscribeProperties); //保存自己消费的最后一次时间戳
    }

    /**
     * 从指定时间开始消费
     *
     * @param startTime 指定时间
     */
    public void reload(Date startTime) {
        reloadTime.set(startTime.getTime());
    }

    private Properties buildProperties(RdsSubscribeProperties rdsSubscribeProperties) {
        Properties props = new Properties();
        String brokers = rdsSubscribeProperties.getBrokers();
        String username = rdsSubscribeProperties.getUsername();
        String password = rdsSubscribeProperties.getPassword();
        String sid = rdsSubscribeProperties.getGroupId();
        int autoCommitIntervalMs = rdsSubscribeProperties.getAutoCommitIntervalMs();
        int sessionTimeout = rdsSubscribeProperties.getSessionTimeoutMs();
        String serializer = "org.apache.kafka.common.serialization.StringDeserializer";
        String deserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
        String groupId = rdsSubscribeProperties.getGroupId();
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s-%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, sid, password);
        props.put("bootstrap.servers", brokers);
//        props.put("auto.commit.interval.ms", String.valueOf(autoCommitIntervalMs));
        // props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", String.valueOf(sessionTimeout));
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
//        props.put("key.serializer", serializer);
//        props.put("value.serializer", serializer);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasCfg);
        props.put("enable.auto.commit", "false");
        props.put("group.id", groupId);
//        props.put("consumer.id", groupId);
        return props;
    }


    private void setValue(Row.Column column, Object o) {
        Object value = null;
        if (o == null) {
            column.setValue(null);
        }

        if (o instanceof Character) {
            Character character = (Character) o;
            value = new String(character.getValue().array(), StandardCharsets.UTF_8);
        }

        if (o instanceof Integer) {
            Integer integer = (Integer) o;
            int precision = integer.getPrecision();
            if (precision <= 4) {
                value = new java.lang.Integer(integer.getValue().toString());
            } else {
                value = new java.lang.Long(integer.getValue().toString());
            }
        }

        if (o instanceof com.alibaba.dts.formats.avro.Float) {
            com.alibaba.dts.formats.avro.Float aFloat = (com.alibaba.dts.formats.avro.Float) o;
            value = aFloat.getValue();
        }

        if (o instanceof com.alibaba.dts.formats.avro.Decimal) {
            com.alibaba.dts.formats.avro.Decimal decimal = (com.alibaba.dts.formats.avro.Decimal) o;
            value = decimal.getValue();
        }

        if (o instanceof com.alibaba.dts.formats.avro.TextObject) {
            com.alibaba.dts.formats.avro.TextObject textObject = (com.alibaba.dts.formats.avro.TextObject) o;
            value = textObject.getValue().toString();
        }

        if (o instanceof DateTime) {
            DateTime dateTime = (DateTime) o;
            java.lang.Integer year = dateTime.getYear() != null ? dateTime.getYear() : 0;
            java.lang.Integer month = dateTime.getMonth() != null ? dateTime.getMonth() : 0;
            java.lang.Integer day = dateTime.getDay() != null ? dateTime.getDay() : 0;
            java.lang.Integer hour = dateTime.getHour() != null ? dateTime.getHour() : 0;
            java.lang.Integer minute = dateTime.getMinute() != null ? dateTime.getMinute() : 0;
            java.lang.Integer second = dateTime.getSecond() != null ? dateTime.getSecond() : 0;
            java.lang.Integer millis = dateTime.getMillis() != null ? dateTime.getMillis() : 0;
            LocalDateTime localDateTime = LocalDateTime.of(year, month, day, hour, minute, second, millis);
            value = localDateTime.toInstant(zoneOffset).toEpochMilli();
        }

        if (o instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) o;
            value = timestamp.getTimestamp();
        }

        if (o instanceof TimestampWithTimeZone) {
            TimestampWithTimeZone timestamp = (TimestampWithTimeZone) o;
            DateTime dateTime = timestamp.getValue();
            java.lang.Integer year = dateTime.getYear() != null ? dateTime.getYear() : 0;
            java.lang.Integer month = dateTime.getMonth() != null ? dateTime.getMonth() : 0;
            java.lang.Integer day = dateTime.getDay() != null ? dateTime.getDay() : 0;
            java.lang.Integer hour = dateTime.getHour() != null ? dateTime.getHour() : 0;
            java.lang.Integer minute = dateTime.getMinute() != null ? dateTime.getMinute() : 0;
            java.lang.Integer second = dateTime.getSecond() != null ? dateTime.getSecond() : 0;
            java.lang.Integer millis = dateTime.getMillis() != null ? dateTime.getMillis() : 0;
            LocalDateTime localDateTime = LocalDateTime.of(year, month, day, hour, minute, second, millis);
            value = localDateTime.toInstant(zoneOffset).toEpochMilli();
        }

        if (o instanceof TextObject) {
            TextObject textObject = (TextObject) o;
            column.setType("text");
            value = textObject.getValue();
        }

        if (o instanceof TextGeometry) {
            TextGeometry textGeometry = (TextGeometry) o;
            column.setType("text");
            value = textGeometry.getValue();
        }

        if (o instanceof BinaryObject) {
            BinaryObject binaryObject = (BinaryObject) o;
            column.setType("binary");
            value = binaryObject.getValue().array();
        }


        if (o instanceof BinaryGeometry) {
            BinaryGeometry binaryGeometry = (BinaryGeometry) o;
            column.setType("binary");
            value = binaryGeometry.getValue().array();
        }

        column.setValue(value);
    }

    public boolean isClosed() {
        return this.isClosed.get();
    }

    String getId() {
        return this.rdsSubscribeProperties.getGroupId();
    }

}
