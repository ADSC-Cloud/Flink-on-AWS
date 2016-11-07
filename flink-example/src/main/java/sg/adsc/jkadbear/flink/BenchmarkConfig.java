package sg.adsc.jkadbear.flink;

/**
 * Created by jkadbear on 27/9/16.
 */

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Encapsulating configuration in once place
 */
public class BenchmarkConfig implements Serializable{

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkConfig.class);

    // Kafka
    public final String kafkaSourceTopic;
    public final String kafkaAckTopic;
    public final String kafkaQueueTopic;
    public final String kafkaBootstrapServers;

    // Load Generator
    public final int loadTargetHz;
    public final int timeSliceLengthMs;
    public final boolean useLocalEventGenerator;
    public final int numCampaigns;

    // Akka
    public final String akkaZookeeperQuorum;
    public final String akkaZookeeperPath;

    // Application
    public final long windowSize;

    // Flink
    public final long checkpointInterval;
    public final boolean checkpointsEnabled;
    public final String checkpointUri;
    public boolean checkpointToUri;

    // The raw parameters
    public final ParameterTool parameters;

    /**
     * Create a config starting with an instance of ParameterTool
     */
    public BenchmarkConfig(ParameterTool parameterTool){
        this.parameters = parameterTool;

        // load generator
        this.loadTargetHz = parameterTool.getInt("load.target.hz", 400_000);
        this.timeSliceLengthMs = parameterTool.getInt("load.time.slice.length.ms", 100);
        this.useLocalEventGenerator = parameters.has("use.local.event.generator");
        this.numCampaigns = parameterTool.getInt("num.campaigns", 1_000_000);

        // Kafka
        this.kafkaSourceTopic = parameterTool.getRequired("kafka.sourcetopic");
        this.kafkaAckTopic = parameterTool.getRequired("kafka.acktopic");
        this.kafkaQueueTopic = parameterTool.getRequired("kafka.queuetopic");
        this.kafkaBootstrapServers = parameterTool.getRequired("bootstrap.servers");

        // Akka
        this.akkaZookeeperQuorum = parameterTool.get("akka.zookeeper.quorum", "localhost");
        this.akkaZookeeperPath = parameterTool.get("akka.zookeeper.path", "/akkaQuery");

        // Application
        this.windowSize = parameterTool.getLong("window.size", 10_000);

        // Flink
        this.checkpointInterval = parameterTool.getLong("flink.checkpoint.interval", 0);
        this.checkpointsEnabled = checkpointInterval > 0;
        this.checkpointUri = parameterTool.get("flink.checkpoint.uri", "");
        this.checkpointToUri = checkpointUri.length() > 0;
    }

    /**
     * Creates a config given a Yaml file
     */
    public BenchmarkConfig(String yamlFile, boolean inJar) throws FileNotFoundException {
        this(yamlToParameters(yamlFile, inJar));
    }

    /**
     * Create a config directly from the command line arguments
     */
    public static BenchmarkConfig fromArgs(String[] args) throws FileNotFoundException {
        if(args.length < 1){
            return new BenchmarkConfig("/benchmarkConf.yaml", true);
        }
        else{
            return new BenchmarkConfig(args[0], false);
        }
    }

    /**
     * Get the parameters
     */
    public ParameterTool getParameters(){
        return this.parameters;
    }

    private static ParameterTool yamlToParameters(String yamlFile, boolean inJar) throws FileNotFoundException {
        // load yaml file
        Yaml yml = new Yaml(new SafeConstructor());
        Map<String, String> ymlMap;
        if (inJar) {
            InputStream input = BenchmarkConfig.class.getResourceAsStream(yamlFile);
            ymlMap = (Map) yml.load(new BufferedReader(new InputStreamReader(input)));
        }
        else {
            ymlMap = (Map) yml.load(new FileInputStream(yamlFile));
        }

        String kafkaZookeeperConnect = getZookeeperServers(ymlMap, String.valueOf(ymlMap.get("kafka.zookeeper.path")));
        String akkaZookeeperQuorum = getZookeeperServers(ymlMap, "");

        // We need to add these values as "parameters"
        // -- This is a bit of a hack but the Kafka consumers and producers
        //    expect these values to be there
        ymlMap.put("zookeeper.connect", kafkaZookeeperConnect); // set ZK connect for Kafka
        ymlMap.put("bootstrap.servers", getKafkaBrokers(ymlMap));
        ymlMap.put("akka.zookeeper.quorum", akkaZookeeperQuorum);
        ymlMap.put("auto.offset.reset", "latest");
        ymlMap.put("group.id", UUID.randomUUID().toString());

        // Convert everything to strings
        for (Map.Entry e : ymlMap.entrySet()) {
            {
                e.setValue(e.getValue().toString());
            }
        }
        return ParameterTool.fromMap(ymlMap);
    }

    private static String getZookeeperServers(Map conf, String zkPath) {
        if(!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")), zkPath);
    }

    private static String getKafkaBrokers(Map conf) {
        if(!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if(!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")), "");
    }


    private static String listOfStringToString(List<String> list, String port, String path) {
        String val = "";
        for(int i=0; i<list.size(); i++) {
            val += list.get(i) + ":" + port + path;
            if(i < list.size()-1) {
                val += ",";
            }
        }
        return val;
    }

}

