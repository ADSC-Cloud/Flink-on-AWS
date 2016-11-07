package sg.adsc.jkadbear.utiltool;

/**
 * Created by jkadbear on 27/9/16.
 */

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

public class MessageController {
    public static boolean isS3 = false;
    public static long startMarkerId = 150L;
    private transient BufferedReader reader;
    private Properties properties;

    private String dataFilePath;
    private String sourceTopic;
    private String ackTopic;
    private String queueAckTopic;
    private long markerId;
    private long rate;

    private static final Logger LOG = Logger.getLogger(MessageController.class);

    private Sender sender;

    private Acker acker;
    private QueueAcker queueAcker;
    private Map<Long, Long> batchStartTs;
    private boolean isRunning;

    // test part
//    private long testMarkerId;
//    private Map<Long, Long> testStartTs;
//    private TestSender testSender;
//    private TestAcker testAcker;

    private MessageController(String _dataFilePath, String _sourceTopic, String _ackTopic, String _queueAckTopic, long _rate) throws IOException {
        dataFilePath = _dataFilePath;
        sourceTopic = _sourceTopic;
        ackTopic = _ackTopic;
        queueAckTopic = _queueAckTopic;
        rate = _rate;
        try (InputStream ins = MessageController.class.getResourceAsStream("/producer.properties")) {
            properties = new Properties();
            properties.load(ins);
        } catch (IOException e) {
            e.printStackTrace();
        }
        isRunning = true;
        sender = new Sender();
        acker = new Acker();

        queueAcker = new QueueAcker();
//        controller = new Controller();
        batchStartTs = new HashMap<>();

        // test part
//        testStartTs = new HashMap<>();
//        testSender = new TestSender();
//        testAcker = new TestAcker();
    }

    private class Sender extends Thread {
        private long totalSentEle;
        private long batchTimeInteval;
        private double totalSize;
        private double ex;
        private KafkaProducer<String, String> producer;
        private List<String> msgList;

        Sender() {
            totalSentEle = 0;
            totalSize = 0;
            batchTimeInteval = 1000; // in 1s
            ex = (double) 1000 / batchTimeInteval;
            markerId = -1L;
            msgList = new LinkedList<>();
            producer = new KafkaProducer<>(properties);
        }

        @Override
        public void run() {
            double sumSendingLatency = 0;
            long latencyCnt = 0;
            long wakeupTime = System.currentTimeMillis();
            long emptyCnt = -1;
            while (isRunning) {
                try {
                    if (isS3) {
                        AmazonS3 s3 = new AmazonS3Client(new ProfileCredentialsProvider());
                        s3.setRegion(Region.getRegion(Regions.EU_WEST_1));
                        S3Object s3object = s3.getObject(new GetObjectRequest("flink-cluster", "splittweetstream.txt"));
                        reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
                    }
                    else {
                        reader = new BufferedReader(new FileReader(new File(dataFilePath)));
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }

                try {
                    String line;
                    while ((line = reader.readLine()) != null) {

                        if (emptyCnt == rate) {
                            long ts1 = System.currentTimeMillis();
                            batchStartTs.put(markerId, ts1);
                            for (String s: msgList) {
                                // insert timestamp into the head of a message
                                producer.send(new ProducerRecord<>(sourceTopic, String.format("%032d%032d%s",
                                        -markerId, System.currentTimeMillis(), s)));
                            }
                            msgList.clear();

                            // emit BatchMarker
                            long ts2 = System.currentTimeMillis();
                            if (markerId < -startMarkerId) {
                                long newSendingLatency = ts2 - ts1;
                                latencyCnt++;
                                sumSendingLatency += newSendingLatency;
                                LOG.info(String.format("MarkerId: %d, SendingLatency: %dms, AveSLatency: %.2fms",
                                        markerId, newSendingLatency, sumSendingLatency / latencyCnt));
                            }
                            String markerStr = String.format("%032d%032d%032d", markerId--, ts2, ts1);
                            // send 3 markers
                            for (int i = 0; i < 3; i++) {
                                producer.send(new ProducerRecord<>(sourceTopic, markerStr));
                            }

//                            LOG.info(String.format("During the last %d ms, we sent %d elements. Sending Rate: %.2f tuples/sec, %.3f MB/sec.",
//                                    now - wakeupTime, totalSentEle, (double) totalSentEle * ex, totalSize * ex / 1024 / 1024));
                            long sleepTime = batchTimeInteval - (System.currentTimeMillis() - wakeupTime);
                            if (sleepTime > 0) {
//                                    LOG.info("SleepTime: " + sleepTime + "ms");
                                Thread.sleep(sleepTime);
                            } else {
                                LOG.warn(String.format("negative sleeptime: %dms, no enough time to emit a batch", sleepTime));
//                                    Thread.sleep(1000);
                            }
                            emptyCnt = 0;
                            totalSize = 0;
                            totalSentEle = 0;
                            wakeupTime = System.currentTimeMillis();
                        }
                        if (line.equals(" ")) {
                            emptyCnt++;
                            continue;
                        }
                        totalSentEle++;
                        totalSize += (double) line.getBytes().length;
                        msgList.add(line);
                    }
                    reader.close();
                    reader = null;
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
            producer.close();
        }
    }

    private class Acker extends Thread {
        private KafkaConsumer<String, String> consumer;

        Acker() {
            consumer = new KafkaConsumer<>(properties);

            consumer.subscribe(Collections.singletonList(ackTopic));
        }

        @Override
        public void run() {
            double sumLatency = 0;
            long latencyCnt = 0;
            double sumQueueLatency = 0;
            long queueLatencyCnt = 0;
            while (isRunning) {
                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record : records) {
//                    long oldMarkerId = Long.valueOf(record.value());
                    long oldMarkerId = Long.valueOf(record.value().substring(0, 32));
                    long oldTs = Long.valueOf(record.value().substring(32, 64));
                    if (oldMarkerId > startMarkerId) {
                        queueLatencyCnt++;
//                        long queueLatency = System.currentTimeMillis() - batchStartTs.get(-oldMarkerId);
                        long queueLatency = System.currentTimeMillis() - oldTs;
                        sumQueueLatency += queueLatency;
                        LOG.info(String.format("MarkerId: %d, QueueLatency: %dms, AveQLatency: %.2fms",
                                oldMarkerId, queueLatency, sumQueueLatency / queueLatencyCnt));
                    }
                    if (oldMarkerId < -startMarkerId) {
//                        long newLatency = System.currentTimeMillis() - batchStartTs.get(oldMarkerId);
                        long newLatency = System.currentTimeMillis() - oldTs;
                        latencyCnt++;
                        sumLatency += newLatency;
                        LOG.info(String.format("MarkerId: %d, BatchLatency: %dms, AveBLatency: %.2fms",
                                oldMarkerId, newLatency, sumLatency / latencyCnt));
                        batchStartTs.remove(oldMarkerId);
                    }
                }
            }
        }
    }

    private class QueueAcker extends Thread {
        private KafkaConsumer<String, String> consumer;

        QueueAcker() {
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(queueAckTopic));
        }

        @Override
        public void run() {
            double sumQueueLatency = 0;
            long queueLatencyCnt = 0;
            while (isRunning) {
                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record : records) {
//                    long oldMarkerId = Long.valueOf(record.value());
                    long oldMarkerId = Long.valueOf(record.value().substring(0, 32));
                    long oldTs = Long.valueOf(record.value().substring(32, 64));
                    if (oldMarkerId < -startMarkerId) {
                        queueLatencyCnt++;
//                        long queueLatency = System.currentTimeMillis() - batchStartTs.get(-oldMarkerId);
                        long queueLatency = System.currentTimeMillis() - oldTs;
                        sumQueueLatency += queueLatency;
                        LOG.info(String.format("MarkerId: %d, QueueLatency: %dms, AveQLatency: %.2fms",
                                oldMarkerId, queueLatency, sumQueueLatency / queueLatencyCnt));
                    }
//                    ackCnt++;
//                    String msg = record.value();
//                    long oldTs = Long.parseLong(msg.substring(0, 32));
//                    sumLatency += System.currentTimeMillis() - oldTs;
//                    if (ackCnt == ackFreq) {
////                        String msg = record.value();
////                        long oldTs = Long.parseLong(msg.substring(0, 32));
////                        LOG.info(String.format("Latency: %dms", System.currentTimeMillis() - oldTs));
//                        double newLatency = sumLatency / (double)ackFreq;
//                        latencyList.add(newLatency);
//                        double ave = 0;
//                        for (double latency : latencyList) {
//                            ave += latency;
//                        }
//                        LOG.info(String.format("Latency: %.2fms, Average Latency: %.2fms", newLatency, ave / latencyList.size()));
//                        ackCnt = 0;
//                    }
                }
            }
        }
    }

//    private class TestSender extends Thread {
//        private KafkaProducer<String, String> producer;
//
//        TestSender() {
//            producer = new KafkaProducer<>(properties);
//        }
//
//        @Override
//        public void run() {
//            while (isRunning) {
//                for (int i = 0; i < 10000; i++) {
//                    long now = System.currentTimeMillis();
//                    testStartTs.put(testMarkerId, now);
//                    producer.send(new ProducerRecord<>("test-topic", String.format("%064d", testMarkerId) + new String(new char[2000]).replace('\0', ' ')));
//                    testMarkerId--;
//                }
//                try {
//                    Thread.sleep(1000);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            producer.close();
//        }
//    }
//
//    private class TestAcker extends Thread {
//        private KafkaConsumer<String, String> consumer;
//
//        TestAcker() {
//            consumer = new KafkaConsumer<>(properties);
//            consumer.subscribe(Collections.singletonList("test-topic"));
//        }
//
//        @Override
//        public void run() {
//            double sumLatency = 0;
//            long latencyCnt = 0;
//            while (isRunning) {
//                ConsumerRecords<String, String> records = consumer.poll(10);
//                for (ConsumerRecord<String, String> record : records) {
//                    long oldMarkerId = Long.valueOf(record.value().substring(0, 64));
//                    long newLatency = System.currentTimeMillis() - testStartTs.get(oldMarkerId);
//                    testStartTs.remove(oldMarkerId);
//                    latencyCnt++;
//                    sumLatency += newLatency;
//                    if (-oldMarkerId % 9999 == 0) {
//                        LOG.info(String.format("MarkerId: %d, TestLatency: %dms, AveTLatency: %.2fms",
//                                oldMarkerId, newLatency, sumLatency / latencyCnt));
//                    }
//                }
//            }
//        }
//    }

    public static void main(String[] args) throws Exception {
        String dataFilePath = args[0];
        isS3 = Boolean.valueOf(args[1]);
        startMarkerId = Long.valueOf(args[2]);
        long rate = Integer.valueOf(args[3]);

        MessageController msgctller = new MessageController(dataFilePath, "source-topic", "ack-topic", "queue-topic", rate);
        msgctller.acker.start();
        msgctller.queueAcker.start();
//        msgctller.sender.start();
//        msgctller.testSender.start();
//        msgctller.testAcker.start();
    }
}
