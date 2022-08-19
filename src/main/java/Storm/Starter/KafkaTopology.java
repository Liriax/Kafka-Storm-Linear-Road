package Storm.Starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.kafka.common.serialization.ListDeserializer;
public class KafkaTopology {

	public static void main (String[] args) throws Exception {
		KafkaSpoutConfig<String, String> spoutConfig = new KafkaSpoutConfig.Builder<String, String>("localhost:9092","connect2")
				.setProp("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				.setProp("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				.setProp("group.id","consumer-testgroup-1")
				.build();	
		KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(spoutConfig);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", kafkaSpout ,4);
		builder.setBolt("SplitBolt", new SplitBolt(),1)
			.shuffleGrouping("KafkaSpout"); 
		//builder.setBolt("AggregateBolt", new AggregateBolt())
			//.shuffleGrouping("SplitBolt"); // get data in a random fashion
	
		LocalCluster cluster = new LocalCluster();
		Config config = new Config();
		//config.registerEventLogger(org.apache.storm.metric.FileBasedEventLogger.class);
		config.setMaxSpoutPending(5000);
//		config.setDebug(true);
        try {
        	cluster.submitTopology("kafkaTopologyy", config, builder.createTopology());
        	Thread.sleep(10000);
        }
        catch (Exception e) {
        	e.printStackTrace();
        } 
        finally {
        	cluster.shutdown();
        	cluster.close();
        }
	}
}
