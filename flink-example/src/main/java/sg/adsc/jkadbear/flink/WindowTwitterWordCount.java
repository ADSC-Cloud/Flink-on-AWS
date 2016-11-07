package sg.adsc.jkadbear.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
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

import java.util.*;

public class WindowTwitterWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(WindowTwitterWordCount.class);

    private static class TweetParser implements FlatMapFunction<String, Tweet> {
        private static final long serialVersionUID = -1028247726481567824L;

        @Override
        public void flatMap(String line, Collector<Tweet> out) throws Exception {
            try {
                Long markerId = Long.parseLong(line.substring(0, 32));
                Long ts = Long.parseLong(line.substring(32, 64));
                // is the tuple a marker?
                if (markerId < 0) {
                    out.collect(new Tweet("", new RegressionPara(ts, markerId, Long.valueOf(line.substring(64)))));
//                    LOG.info("markerId: " + markerId + " tweetparser ts: " + (System.currentTimeMillis() - ts));
                    return;
                }

                String content = line.substring(64);
                Status s = DataObjectFactory.createStatus(content);
                if (s != null) {
                    HashtagEntity[] hashtags = s.getHashtagEntities();
                    for (HashtagEntity hashtag : hashtags) {
                        String tag = hashtag.getText();
                        out.collect(new Tweet(tag, new RegressionPara(ts, markerId, 1L)));
                    }
                }
            } catch (TwitterException e) {
                e.printStackTrace();
            }
        }
    }

    // simulate micro-batch by batch-marker
    public static class TupleBuffer {
        public Map<Long, List<Tweet>> tupleMap;
        public Map<Long, Integer> readyCntMap;
        public Map<Long, Tweet> markerMap;
        public Map<String, Tweet> states;

        TupleBuffer() {
            tupleMap = new HashMap<>();
            readyCntMap = new LinkedHashMap<>();
            markerMap = new HashMap<>();
            states= new HashMap<>();
        }

        void addTuple(Tweet tweet) {
            long markerId = tweet.getMarkerId();
            // if this is a marker
            if (markerId < 0) {
                if (!readyCntMap.containsKey(markerId)) {
                    readyCntMap.put(markerId, 1);
                    markerMap.put(markerId, tweet);
                }
                else {
                    int cnt = readyCntMap.get(markerId);
                    readyCntMap.put(markerId, cnt+1);
                }
                return;
            }

            // tuple's markerId is the opposite number of batchmarker's markerId
            markerId = -markerId;
            if (!tupleMap.containsKey(markerId)) {
                tupleMap.put(markerId, new LinkedList<>());
            }
            tupleMap.get(markerId).add(tweet);
        }

        boolean isReady() {
            for (long ll : readyCntMap.keySet()) {
                if (readyCntMap.get(ll) == 3) {
                    return true;
                }
            }
            return false;
        }

        List<List<Tweet>> getReadyBatches() {
            List<List<Tweet>> list = new LinkedList<>();
            for (long ll : readyCntMap.keySet()) {
                if (readyCntMap.get(ll) == 3) {
                    list.add(tupleMap.get(ll));
                }
            }
            return list;
        }

        Tweet getMarker(long id) {
            return markerMap.get(id);
        }

        void clearBatch(long id) {
            tupleMap.remove(id);
            readyCntMap.remove(id);
            markerMap.remove(id);
        }

        Map<String, Tweet> getStates() {
            return states;
        }

        List<Tweet> getSentBatch(Map<String, String> currHashTagMap) {
            List<Tweet> resList = new ArrayList<>();
            for (String tag : currHashTagMap.keySet()) {
                resList.add(states.get(tag));
            }
            return resList;
        }

    }

    private static class LeftPara extends RichFlatMapFunction<Tweet, Tweet> {
        private transient ValueState<TupleBuffer> lpBuffer;

        @Override
        public void flatMap(Tweet tweet, Collector<Tweet> collector) throws Exception {
            TupleBuffer tb = lpBuffer.value();
            // buffer it
            tb.addTuple(tweet);
            // if a batch is ready, do processing and send a marker to downstream
            if (tb.isReady()) {
                Map<String, Tweet> states = tb.getStates();
                for (List<Tweet> batch : tb.getReadyBatches()) {
                    // batchmarker id < 0
                    long markerId = -batch.get(0).getMarkerId();
                    Map<String, String> currHashTagMap = new HashMap<>();
                    for (Tweet tuple : batch) {
                        RegressionPara tupleRP = tuple.regPara;
                        Long ts = tupleRP.ts;
                        if (!states.containsKey(tuple.hashTag)) {
                            states.put(tuple.hashTag, new Tweet(tuple.hashTag, new RegressionPara(ts, tuple.getMarkerId(), 0L)));
                        }
                        RegressionPara statesRP = states.get(tuple.hashTag).regPara;
                        double p1 = statesRP.p1 + tupleRP.p1;
                        double p2 = statesRP.p2 + tupleRP.ts;
                        double p3 = statesRP.p3 + p1;
                        statesRP.setLeftRegressionPara(ts, p1, p2, p3);
                        currHashTagMap.put(tuple.hashTag, "");
                    }
                    // emit tuples
                    tb.getSentBatch(currHashTagMap).forEach(collector::collect);
                    // emit marker
                    collector.collect(tb.getMarker(markerId));
                    // clear current batch
                    tb.clearBatch(markerId);
                }
            }
            lpBuffer.update(tb);
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TupleBuffer> descriptor =
                    new ValueStateDescriptor<>(
                            "lpBuffer", // the state name
                            TypeInformation.of(new TypeHint<TupleBuffer>() {}), // type information
                            new TupleBuffer()); // default value of the state, if nothing was set
            lpBuffer = getRuntimeContext().getState(descriptor);
        }
    }

    private static class RightPara extends RichFlatMapFunction<Tweet, Tweet> {
        private transient ValueState<TupleBuffer> rpBuffer;
        @Override
        public void flatMap(Tweet tweet, Collector<Tweet> collector) throws Exception {
            TupleBuffer tb = rpBuffer.value();
            // buffer it
            tb.addTuple(tweet);
            // if a batch is ready, do processing and send a marker to downstream
            if (tb.isReady()) {
                Map<String, Tweet> states = tb.getStates();
                for (List<Tweet> batch : tb.getReadyBatches()) {
                    // batchmarker id < 0
                    long markerId = -batch.get(0).getMarkerId();
                    Map<String, String> currHashTagMap = new HashMap<>();
                    for (Tweet tuple : batch) {
                        RegressionPara tupleRP = tuple.regPara;
                        Long ts = tupleRP.ts;
                        if (!states.containsKey(tuple.hashTag)) {
                            states.put(tuple.hashTag, new Tweet(tuple.hashTag, new RegressionPara(ts, tuple.getMarkerId(), 0L)));
                        }
                        RegressionPara statesRP = states.get(tuple.hashTag).regPara;
                        double p1 = statesRP.p1 + tupleRP.p1;
                        double p4 = statesRP.p4 + tupleRP.ts * tupleRP.ts;
                        double p6 = statesRP.p6 + tupleRP.ts * p1;
                        statesRP.setLeftRegressionPara(ts, p1, p4, p6);
                        currHashTagMap.put(tuple.hashTag, "");
                    }
                    // emit tuples
                    tb.getSentBatch(currHashTagMap).forEach(collector::collect);
                    // emit marker
                    collector.collect(tb.getMarker(markerId));
                    // clear current batch
                    tb.clearBatch(markerId);
                }
            }
            rpBuffer.update(tb);
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TupleBuffer> descriptor =
                    new ValueStateDescriptor<>(
                            "rpBuffer", // the state name
                            TypeInformation.of(new TypeHint<TupleBuffer>() {}), // type information
                            new TupleBuffer()); // default value of the state, if nothing was set
            rpBuffer = getRuntimeContext().getState(descriptor);
        }
    }


    public static class CoFlatMapTupleBuffer {
        public Map<Long, List<Tweet>> tupleMap1;
        public Map<Long, List<Tweet>> tupleMap2;
        public Long markerId1;
        public Long markerId2;
        public Map<Long, Tweet> readyBatch1;
        public Map<Long, Tweet> readyBatch2;

        CoFlatMapTupleBuffer() {}

        CoFlatMapTupleBuffer(long _markerId) {
            tupleMap1 = new HashMap<>();
            tupleMap2 = new HashMap<>();
            markerId1 = _markerId;
            markerId2 = _markerId;
            readyBatch1 = new HashMap<>();
            readyBatch2 = new HashMap<>();
        }

        void addTuple1(Tweet tweet) {
            if (tweet.getMarkerId() < 0) {
                readyBatch1.put(markerId1--, tweet);
                return;
            }
            if (!tupleMap1.containsKey(markerId1)) {
                tupleMap1.put(markerId1, new LinkedList<>());
            }
            tupleMap1.get(markerId1).add(tweet);
        }

        void addTuple2(Tweet tweet) {
            if (tweet.getMarkerId() < 0) {
                readyBatch2.put(markerId2--, tweet);
                return;
            }
            if (!tupleMap2.containsKey(markerId2)) {
                tupleMap2.put(markerId2, new LinkedList<>());
            }
            tupleMap2.get(markerId2).add(tweet);
        }

        List<Long> getReadyMarkerId1() {
            List<Long> list = new LinkedList<>();
            list.addAll(readyBatch1.keySet());
            return list;
        }

        Long getTs1(long id) {
            return readyBatch1.get(id).getCreateTime();
        }

        Long getBatchStartTs(long id) {
            return (long) readyBatch1.get(id).regPara.p1;
        }

        RegressionPara getMarker(long id) {
            return new RegressionPara(getTs1(id), id, getBatchStartTs(id));
        }

        // be called when stream1 and stream2 are all ready,
        List<Tweet> getBatch1(Long id) {
            return tupleMap1.get(id);
        }

        List<Tweet> getBatch2(Long id) {
            return tupleMap2.get(id);
        }

        void removeBatch1(Long id) {
            tupleMap1.remove(id);
            readyBatch1.remove(id);
        }

        void removeBatch2(Long id) {
            tupleMap2.remove(id);
            readyBatch2.remove(id);
        }
    }

    private static class CombinePara extends RichCoFlatMapFunction<Tweet, Tweet, Tweet> {
        private static final long serialVersionUID = 575640980903073393L;
        private transient ValueState<CoFlatMapTupleBuffer> cfBuffer;

        public void flatMap1(Tweet tweet, Collector<Tweet> collector) throws Exception {
            doJoin(tweet, collector, 1);
        }

        @Override
        public void flatMap2(Tweet tweet, Collector<Tweet> collector) throws Exception {
            doJoin(tweet, collector, 2);
        }

        void doJoin(Tweet tweet, Collector<Tweet> collector, int num) throws Exception {
            CoFlatMapTupleBuffer cftb = cfBuffer.value();
            if (num == 1) {
                cftb.addTuple1(tweet);
            }
            else {
                cftb.addTuple2(tweet);
            }
            for (Long readyMarkerId : cftb.getReadyMarkerId1()) {
                List<Tweet> l1 = cftb.getBatch1(readyMarkerId);
                List<Tweet> l2 = cftb.getBatch2(readyMarkerId);
                if (l1 != null && l2 != null) {
                    Map<String, Tweet> currTuples1 = new HashMap<>();
                    Map<String, Tweet> currTuples2 = new HashMap<>();
                    for (Tweet tt : l1) {
                        currTuples1.put(tt.hashTag, tt);
                    }
                    for (Tweet tt : l2) {
                        currTuples2.put(tt.hashTag, tt);
                    }
                    for (String tag : currTuples1.keySet()) {
                        if (currTuples2.containsKey(tag)) {
                            RegressionPara val1 = currTuples1.get(tag).regPara;
                            RegressionPara val2 = currTuples2.get(tag).regPara;
                            val1.SetRightRegressionPara(val1.ts, val2.p1, val2.p4, val2.p6);
                            collector.collect(new Tweet(tag, val1));
                        }
                    }
                    // emit marker
                    collector.collect(new Tweet("", cftb.getMarker(readyMarkerId)));
//                    LOG.info("markerId: " + readyMarkerId + " combine ts: " + (System.currentTimeMillis() - cftb.getMarker(readyMarkerId).p1));
                    // remove batch that has been sent
                    cftb.removeBatch1(readyMarkerId);
                    cftb.removeBatch2(readyMarkerId);
                }
            }
            cfBuffer.update(cftb);
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<CoFlatMapTupleBuffer> descriptor =
                    new ValueStateDescriptor<>(
                            "cfBuffer", // the state name
                            TypeInformation.of(new TypeHint<CoFlatMapTupleBuffer>() {}), // type information
                            new CoFlatMapTupleBuffer(-1L)); // default value of the state, if nothing was set
            cfBuffer = getRuntimeContext().getState(descriptor);
        }
    }

    // simulate micro-batch by batch-marker
    public static class FinalBuffer {
        public Map<Long, List<Tweet>> tupleMap;
        public Long markerId;

        FinalBuffer() {
            markerId = -1L;
            tupleMap = new HashMap<>();
        }

        List<Tweet> getCurrTuples() {
//            if (!tupleMap.containsKey(markerId)) {
//                tupleMap.put(markerId, new LinkedList<>());
//            }
            return tupleMap.get(markerId);
        }

        void addTuple(Tweet tweet) {
            if (!tupleMap.containsKey(markerId)) {
                tupleMap.put(markerId, new LinkedList<>());
            }
            tupleMap.get(markerId).add(tweet);
        }

        void clearCurrBatch() {
            tupleMap.remove(markerId);
            // set next marker id
            markerId--;
        }
    }

    private static class FinalPara extends RichFlatMapFunction<Tweet, Tweet> {
        private transient ValueState<FinalBuffer> fpBuffer;

        @Override
        public void flatMap(Tweet tweet, Collector<Tweet> collector) throws Exception {
            FinalBuffer tb = fpBuffer.value();
            // if this is a tuple, buffer it
            if (tweet.getMarkerId() >= 0) {
                tb.addTuple(tweet);
            }
            // if this is a marker, do processing and send a marker to downstream
            else {
                // emit tuples
                for (Tweet tuple : tb.getCurrTuples()) {
                    RegressionPara val2 = tuple.regPara;
                    double p5 = val2.p1 * val2.p1;
                    double p7 = val2.p2 * val2.p3;
                    RegressionPara ret = new RegressionPara();
                    ret.setFinalRegressionPara(val2.ts, val2.p1, val2.p2, val2.p3, val2.p4, p5, val2.p6, p7);
                    double b = (ret.p1 * ret.p6 - ret.p2 * ret.p3) / (ret.p1 * ret.p4 - ret.p5);
                    double a = ret.p3 / ret.p1 - b * ret.p2 / ret.p1;
                    ret.setAB(a, b);
                    collector.collect(new Tweet(tuple.hashTag, ret));
                }
                // emit marker
//                collector.collect(tb.getMarker(tweet.getCreateTime()));
                collector.collect(tweet);
//                LOG.info("markerId: " + tweet.getMarkerId() + " finalpara ts: " + (System.currentTimeMillis() - tweet.regPara.p1));
                // clear current batch
                tb.clearCurrBatch();
            }
            fpBuffer.update(tb);
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<FinalBuffer> descriptor =
                    new ValueStateDescriptor<>(
                            "fpBuffer", // the state name
                            TypeInformation.of(new TypeHint<FinalBuffer>() {}), // type information
                            new FinalBuffer()); // default value of the state, if nothing was set
            fpBuffer = getRuntimeContext().getState(descriptor);
        }
    }

    private static class SampleFlatMap implements FlatMapFunction<Tweet, String> {
        @Override
        public void flatMap(Tweet tweet, Collector<String> collector) throws Exception {
            if (tweet.getMarkerId() < 0) {
//                LOG.info("new sample, marker: " + Long.toString(tweet.getMarkerId()) + ", startTs: " + Long.toString(tweet.getCreateTime()));
//                LOG.info("markerId: " + tweet.getMarkerId() + " samplemap ts: " + (System.currentTimeMillis() - tweet.getCreateTime()));
//                collector.collect(Long.toString(tweet.getMarkerId()));
                collector.collect(String.format("%032d%032d", tweet.getMarkerId(), (long) tweet.regPara.p1));
            }
        }
    }
    private static class SampleQueueFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            // is the tuple a marker?
            if (s.charAt(0) == '-') {
                collector.collect(s.substring(0, 64));
            }
        }
    }

//    private static class TagKeySelector implements KeySelector<Tweet, Integer> {
//        @Override
//        public Integer getKey(Tweet v) throws Exception {
//            return v.uselessKey;
//        }
//    }

    /**
     * Create Kafka Source
     */
    private static FlinkKafkaConsumer09<String> kafkaSource(BenchmarkConfig config) {
        return new FlinkKafkaConsumer09<>(
                config.kafkaSourceTopic,
                new SimpleStringSchema(),
                config.getParameters().getProperties());
    }

    /**
     * Create Kafka Sink
     */
    private static FlinkKafkaProducer09<String> kafkaSink(BenchmarkConfig config) {
        return new FlinkKafkaProducer09<>(
                config.kafkaBootstrapServers,
                config.kafkaAckTopic,
                new SimpleStringSchema());
    }

    /**
     * Create Kafka Sink
     */
    private static FlinkKafkaProducer09<String> kafkaQueueSink(BenchmarkConfig config) {
        return new FlinkKafkaProducer09<>(
                config.kafkaBootstrapServers,
                config.kafkaQueueTopic,
                new SimpleStringSchema());
    }

    /**
     * This generator generates watermarks that are lagging behind processing time by a certain amount.
     * It assumes that elements arrive in Flink after at most a certain time.
     */
    private static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tweet> {

        private final long maxTimeLag = 5000; // 5 seconds

        @Override
        public long extractTimestamp(Tweet tweet, long previousElementTimestamp) {
            return tweet.getCreateTime();
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }

    public static void main(String[] args) throws Exception {

//        Properties properties = new Properties();
//        properties = new Properties();
//        properties.put("metadata.broker.list", "localhost:9092");
//        properties.put("serializer.class", "kafka.serializer.StringEncoder");
//        properties.put("request.required.acks", "1");
//        properties.put("bootstrap.servers", "jkmaster:9092");
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
//        producer.send(new ProducerRecord<String, String>("ackTopic", String.format("%032d", ret.ts)));

        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(config.getParameters());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

//        env.getConfig().setLatencyTrackingInterval(1000);
//        if (config.checkpointsEnabled) {
//            env.enableCheckpointing(config.checkpointInterval);
//        }


        // different source: from kafka or directly from disk
//        DataStream<String> source = env.addSource(kafkaSource(config));
        DataStream<String> source = env.addSource(new TweetSource("/home/jkadbear/Downloads/splittweetstream.txt", 600));


        // measure queue latency
        source.flatMap(new SampleQueueFlatMap())
                .addSink(kafkaQueueSink(config)).name("QueueLatencySink");

        // round-robin
        DataStream<Tweet> tweets = source.rebalance()
                .flatMap(new TweetParser()).setParallelism(3).name("TweetParser")
                .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator());

        KeyedStream<Tweet, Tuple> keyTweets = tweets.keyBy("uselessKey");

        // Part 1
        DataStream<Tweet> p1 = keyTweets.flatMap(new LeftPara()).name("Parameters1");

        // Part 2
        DataStream<Tweet> p2 = keyTweets.flatMap(new RightPara()).name("Parameters2");

        DataStream<Tweet> finalStream = p1.connect(p2)
                .keyBy("uselessKey", "uselessKey")
                .flatMap(new CombinePara()).name("Combine")
                .keyBy("uselessKey")
                .flatMap(new FinalPara()).name("Final");

        finalStream.flatMap(new SampleFlatMap())
                .addSink(kafkaSink(config)).name("Sink");

        // execute program
        env.execute("Twitter Streaming");
    }
}

