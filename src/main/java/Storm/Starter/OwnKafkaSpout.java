package Storm.Starter;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

public class OwnKafkaSpout {

	//Creating SpoutConfig Object
	KafkaSpoutConfig<String, String> spoutConfig = new KafkaSpoutConfig<String, String>(new KafkaSpoutConfig.Builder<String, String>("localhost:9092","connect-test"));
	
	//Assign SpoutConfig to KafkaSpout.
	KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(spoutConfig);
	
}
