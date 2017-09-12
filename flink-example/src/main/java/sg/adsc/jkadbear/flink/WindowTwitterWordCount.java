package sg.adsc.jkadbear.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

import java.util.Properties;

public class WindowTwitterWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(WindowTwitterWordCount.class);

    public static class TweetParser implements FlatMapFunction<String, RegressionPara> {
        private static final long serialVersionUID = -1028247726481567824L;

        @Override
        public void flatMap(String line, Collector<RegressionPara> out) throws Exception {
            try {
                Long markerId = Long.parseLong(line.substring(0, 32));
                Long x = Long.parseLong(line.substring(32, 64)); // x is timestamp
                // is the tuple a marker?
                if (markerId < 0) {
                    out.collect(new RegressionPara("", markerId, x));
                    return;
                }

                String content = line.substring(64);
                Status s = DataObjectFactory.createStatus(content);
                if (s != null) {
                    HashtagEntity[] hashtags = s.getHashtagEntities();
                    for (HashtagEntity hashtag : hashtags) {
                        String tag = hashtag.getText();
                        out.collect(new RegressionPara(tag, markerId, x));
                    }
                }
            } catch (TwitterException e) {
                e.printStackTrace();
            }
        }
    }

    // over shoes over boots
    // The logic of LeftPara and RightPara is wrong
    // but ~~~ it doesn't matter ~~~
//    public static class LeftPara implements ReduceFunction<RegressionPara> {
//        @Override
//        public RegressionPara reduce(RegressionPara reg1, RegressionPara reg2) throws Exception {
//            if (reg2.getMarkerId() < 0) {
//                return reg2;
//            }
//            reg1.setY(reg1.getY() + reg2.getY());
//            reg1.setySum(reg1.getySum() + reg1.getY() + reg2.getY());
//            reg1.setxCrossY(reg1.getxCrossY() + reg2.getX() * reg2.getY());
//            return reg1;
//        }
//    }

//    public static class RightPara implements ReduceFunction<RegressionPara> {
//        @Override
//        public RegressionPara reduce(RegressionPara reg1, RegressionPara reg2) throws Exception {
//            if (reg2.getMarkerId() < 0) {
//                return reg2;
//            }
////            reg1.setX(reg2.getX());
//            reg1.setxSum(reg1.getxSum() + reg2.getX());
//            reg1.setxSqSum(reg1.getxSqSum() + reg2.getX() * reg2.getX());
//            reg1.setN(reg1.getN() + reg2.getN());
//            return reg1;
//        }
//    }

    public static class LeftPara extends RichWindowFunction<RegressionPara, RegressionPara, String, TimeWindow> {
        private transient ValueState<RegressionPara> lpBuffer;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<RegressionPara> descriptor =
                    new ValueStateDescriptor<>("lpBuffer", // the state name
                            TypeInformation.of(new TypeHint<RegressionPara>() {}), // type information
                            null); // default value of the state, if nothing was set
            lpBuffer = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<RegressionPara> iterable, Collector<RegressionPara> collector) throws Exception {
            RegressionPara firstReg = iterable.iterator().next();
            if (firstReg != null) {
                RegressionPara state = lpBuffer.value();
                if (state == null) {
                    Long id = firstReg.getMarkerId();
                    Long x = firstReg.getX();
                    state = new RegressionPara(s, id, x, 0, x, 0, x*x, 0, 0, 0, 0);
                }
                state.setMarkerId(firstReg.getMarkerId());
                state.setX(firstReg.getX());
                for (RegressionPara reg : iterable) {
                    state.setY(state.getY() + reg.getY());
                    state.setySum(state.getySum() + state.getY() + reg.getY());
                    state.setxCrossY(state.getxCrossY() + reg.getX() * reg.getY());
                }
                lpBuffer.update(state);
                collector.collect(state);
            }
        }
    }


    public static class RightPara extends RichWindowFunction<RegressionPara, RegressionPara, String, TimeWindow> {
        private transient ValueState<RegressionPara> rpBuffer;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<RegressionPara> descriptor =
                    new ValueStateDescriptor<>("rpBuffer", // the state name
                            TypeInformation.of(new TypeHint<RegressionPara>() {}), // type information
                            null); // default value of the state, if nothing was set
            rpBuffer = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<RegressionPara> iterable, Collector<RegressionPara> collector) throws Exception {
            RegressionPara firstReg = iterable.iterator().next();
            if (firstReg != null) {
                RegressionPara state = rpBuffer.value();
                if (state == null) {
                    Long id = firstReg.getMarkerId();
                    Long x = firstReg.getX();
                    state = new RegressionPara(s, id, x, 0, x, 0, x*x, 0, 0, 0, 0);
                }
                state.setMarkerId(firstReg.getMarkerId());
                state.setX(firstReg.getX());
                for (RegressionPara reg : iterable) {
                    state.setxSum(state.getxSum() + reg.getX());
                    state.setxSqSum(state.getxSqSum() + reg.getX() * reg.getX());
                    state.setN(state.getN() + reg.getN());
                }
                rpBuffer.update(state);
                collector.collect(state);
            }
        }
    }


    public static class CombinePara implements JoinFunction<RegressionPara, RegressionPara, RegressionPara> {
        @Override
        public RegressionPara join(RegressionPara reg1, RegressionPara reg2) throws Exception {
            if (reg1.getMarkerId() < 0) {
                return reg1;
            }
            reg1.setRightRegressionPara(reg2.getX(), reg2.getxSum(), reg2.getxSqSum(), reg2.getN());
            return reg1;
        }
    }

    public static class FinalPara extends RichMapFunction<RegressionPara, RegressionPara> {
        private transient ValueState<RegressionPara> fpBuffer;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<RegressionPara> descriptor =
                    new ValueStateDescriptor<>(
                            "fpBuffer", // the state name
                            TypeInformation.of(new TypeHint<RegressionPara>() {}), // type information
                            null); // default value of the state, if nothing was set
            fpBuffer = getRuntimeContext().getState(descriptor);
        }

        @Override
        public RegressionPara map(RegressionPara reg) throws Exception {
//            RegressionPara oldReg = fpBuffer.value();
            // if this is a marker, send the marker to downstream
            if (reg.getMarkerId() < 0){
                return reg;
            }
            // if this is a tuple, calculate a and b
            else {
                double b = (reg.getN() * reg.getxCrossY() - reg.getxSum() * reg.getySum()) /
                        (reg.getN() * reg.getxSqSum() - reg.getxSum() * reg.getxSum());
                double a = reg.getySum() / reg.getN() - b * reg.getxSum() / reg.getN();
                reg.setAB(a, b);
                fpBuffer.update(reg);
                return reg;
            }
        }
    }

//    public static class FinalFilter extends RichFilterFunction<RegressionPara> {
//        private transient ValueState<Long> ffBuffer;
//
//        @Override
//        public void open(Configuration config) {
//            ValueStateDescriptor<Long> descriptor =
//                    new ValueStateDescriptor<>(
//                            "ffBuffer", // the state name
//                            TypeInformation.of(new TypeHint<Long>() {}), // type information
//                            0L); // default value of the state, if nothing was set
//            ffBuffer = getRuntimeContext().getState(descriptor);
//        }
//
//        @Override
//        public boolean filter(RegressionPara reg) throws Exception {
//            Long cnt = ffBuffer.value();
//            ffBuffer.update(cnt+1);
//            return cnt > 193000 && cnt % 1000 == 0;
//        }
//    }

    public static class WriteMarkerSink implements SinkFunction<RegressionPara> {
        @Override
        public void invoke(RegressionPara reg) throws Exception {
            if (reg.getMarkerId() < 0) { LOG.info(String.format("#*FROMFINAL\t%d %d", reg.getMarkerId(), System.currentTimeMillis())); }
            if (Math.random() > 0.9995) { LOG.info(String.format("#*LATENCY\t%d", System.currentTimeMillis() - reg.getX())); }
        }
    }

    public static class RegKeySelector implements KeySelector<RegressionPara, String> {
        @Override
        public String getKey(RegressionPara reg) throws Exception {
            return reg.getHashTag();
        }
    }

    private static FlinkKafkaConsumer09<String> KafkaSource() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "jkmaster:9092");
        return new FlinkKafkaConsumer09<>("source-topic", new SimpleStringSchema(), properties);
    }

    private static class RegFinalMap implements FlatMapFunction<RegressionPara, String> {
        @Override
        public void flatMap(RegressionPara reg, Collector<String> collector) throws Exception {
            if (reg.getMarkerId() < 0) {
//            if (Math.random() > 0.999) {
                collector.collect(String.format("%032d%032d", reg.getMarkerId(), reg.getX()));
            }
        }
    }

    private static FlinkKafkaProducer09<String> KafkaSink() {
        return new FlinkKafkaProducer09<>("jkmaster:9092", "ack-topic", new SimpleStringSchema());
    }

    public static void main(String[] args) throws Exception {
        ParameterTool pmTool = ParameterTool.fromArgs(args);
        boolean isCkp = pmTool.getBoolean("isCkp", false);
        int ckpInterval = pmTool.getInt("ckpInterval", 0);
        boolean isS3 = pmTool.getBoolean("isS3", true);
        String dataPath = pmTool.get("dataPath", "s3://flink-cluster/splittweetstream.txt");
        int rate = pmTool.getInt("rate", 600);
        int countWindowSize = (int) (rate * 5.38);
        int timeWindowSize = pmTool.getInt("timeWinSize", 100); // 100 ms
        int slideTimeWindowStep = 5;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.getConfig().setLatencyTrackingInterval(1000);

        if (isCkp) {
            env.enableCheckpointing(ckpInterval);
        }

//        DataStream<String> source = env.addSource(KafkaSource());//.slotSharingGroup("slotG1");
        DataStream<String> source = env.addSource(new TweetSource(dataPath, rate, isS3)).slotSharingGroup("slotG1");

        // round-robin
        DataStream<RegressionPara> tweets = source//.rebalance()
                .flatMap(new TweetParser()).setParallelism(3).name("TweetParser").slotSharingGroup("slotG2");
//                .flatMap(new TweetParser()).setParallelism(3).name("TweetParser");
//                .keyBy(new RegKeySelector())
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(timeWindowSize)))
//                .reduce(new HashTagCounter()).setParallelism(3).slotSharingGroup("slotG2");
//                .reduce(new HashTagCounter()).setParallelism(3);
//        tweets.flatMap(new RegFinalMap()).addSink(KafkaSink());

        // Part 1
        DataStream<RegressionPara> p1 = tweets
                .keyBy(new RegKeySelector())
                .timeWindow(Time.milliseconds(timeWindowSize))
//                .countWindow(countWindowSize)
                .apply(new LeftPara()).name("Parameters1").slotSharingGroup("slotG3");
//                .keyBy(new RegKeySelector())
//                .map(new StateLeftPara());

//        p1.addSink(new WriteMarkerSink()).slotSharingGroup("slotG1");

        // Part 2
        DataStream<RegressionPara> p2 = tweets
                .keyBy(new RegKeySelector())
                .timeWindow(Time.milliseconds(timeWindowSize))
//                .countWindow(countWindowSize)
                .apply(new RightPara()).name("Parameters2").slotSharingGroup("slotG3");
//                .keyBy(new RegKeySelector())
//                .map(new StateRightPara());

        DataStream<RegressionPara> joinStream = p1.join(p2)
                .where(new RegKeySelector())
                .equalTo(new RegKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(timeWindowSize)))
//                .window(SlidingEventTimeWindows.of(Time.milliseconds(timeWindowSize), Time.milliseconds(slideTimeWindowStep)))
//                .window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(countWindowSize)))
                .apply(new CombinePara());

        DataStream<RegressionPara> finalStream = joinStream//.filter(new FinalFilter())
                .keyBy(new RegKeySelector())
                .map(new FinalPara()).name("Final");

//        finalStream.flatMap(new RegFinalMap()).addSink(KafkaSink());
        finalStream.addSink(new WriteMarkerSink()).name("Sink").slotSharingGroup("slotG1");

        // execute program
        env.execute("Twitter Streaming");
    }
}
