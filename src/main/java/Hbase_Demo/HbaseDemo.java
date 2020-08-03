package Hbase_Demo;

import com.alibaba.flink.connectors.cloudhbase.sink.CloudHBaseRecordResolver;
import com.alibaba.flink.connectors.cloudhbase.sink.CloudHBaseSinkFunction;
import com.alibaba.flink.connectors.datahub.datastream.source.DatahubSourceFunction;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class HbaseDemo implements Serializable {
    private static final Logger logger =  LoggerFactory.getLogger(HbaseDemo.class);

    //DataHub相关参数
//private static String endPoint ="public endpoint";//公网访问（填写内网Endpoint，就不用填写公网Endpoint）。
    private static String endPoint = "inner endpoint";//内网访问。
    private static String projectName = "yourProject";
    private static String topicSourceName = "yourTopic";
    private static String accessId = "yourAK";
    private static String accessKey = "yourAS";
    private static Long datahubStartInMs = 0L;//设置消费的启动位点对应的时间。
    //Hbase相关参数
    private static String zkQuorum = "yourZK";
    private static String tableName = "yourTable";
    private static String columnFamily = "yourcolumnFamily";


    public static void main(String[] args) throws Exception {

        HbaseDemo hbaseDemoDatahub = new HbaseDemo();
        hbaseDemoDatahub.runExample();

    }
    public void runExample() throws Exception {
        int numColumns = 3;
        Row columnNames = new Row(numColumns);
        columnNames.setField(0, "a");
        columnNames.setField(1, "b");
        columnNames.setField(2, "c");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DatahubSourceFunction datahubSource =
                new DatahubSourceFunction(endPoint, projectName, topicSourceName, accessId, accessKey, datahubStartInMs,
                        Long.MAX_VALUE, 1, 1, 1);

        env.addSource(datahubSource).flatMap(new FlatMapFunction<List<RecordEntry>, Tuple3<Boolean,String,String>>() {
            public void flatMap(List<RecordEntry> ls, Collector<Tuple3<Boolean,String,String>> collector) throws Exception {
                for (RecordEntry recordEntry : ls){
                    collector.collect(getTuple3(recordEntry));
                }
            }
        })
                .returns(new TypeHint<Tuple3<Boolean, String,String>>() {})
//                .print();
                .addSink(new CloudHBaseSinkFunction(
                        zkQuorum, tableName, new HbaseDemo.TupleRecordResolver(columnFamily, columnNames)))
                .setParallelism(1);
        env.execute("testdatahub");
    }
    private Tuple3<Boolean,String,String> getTuple3(RecordEntry recordEntry) {//mvn assembly:assembly
        Tuple3<Boolean,String,String> tuple3 = new Tuple3<Boolean, String,String>();
        TupleRecordData recordData = (TupleRecordData) (recordEntry.getRecordData());
        tuple3.f0 = (Boolean) recordData.getField(0);
        tuple3.f1 = (String) recordData.getField(1);
        tuple3.f2 = (String) recordData.getField(2);
        return tuple3;
    }

    static class TupleRecordResolver implements CloudHBaseRecordResolver<Tuple3<Boolean, String, String>> {

        private final String columnFamily;

        private final Row columnNames;

        public TupleRecordResolver(String columnFamily, Row columnNames) {
            this.columnFamily = columnFamily;
            this.columnNames = columnNames;
        }

        @Override
        public String getRowKey(Tuple3<Boolean, String, String> record) {
            return record.f1.toString();
        }

        @Override
        public Mutation getMutation(Tuple3<Boolean, String, String> record) {
            if (record.f0) {
                // Put mutation
                Put put = new Put(record.f1.toString().getBytes());
                String row = record.f2;
                for (int i = 0; i < columnNames.getArity(); i++) {
                    Object object = row.getBytes();
                    if (object != null) {
                        put.addColumn(columnFamily.getBytes(),
                                columnNames.getField(i).toString().getBytes(),
                                object.toString().getBytes());
                    }
                }
                return put;
            } else {
                // Delete mutation
                // Put mutation
                Delete delete = new Delete(record.f1.toString().getBytes());
                String row = record.f2;
                for (int i = 0; i < columnNames.getArity(); i++) {
                    Object object = row.getBytes();
                    if (object != null) {
                        delete.addColumn(columnFamily.getBytes(),
                                columnNames.getField(i).toString().getBytes());
                    }
                }
                return delete;
            }
        }
    }
}
