package Storm.Starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology{
	
	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("IntegerSpout", new IntegerSpout());
		builder.setBolt("MultiplierBolt", new MultiplierBolt())
			.shuffleGrouping("IntegerSpout"); // get data in a random fashion
		
		LocalCluster cluster = new LocalCluster();
		Config config = new Config();
		config.setDebug(true);
        try {
        	cluster.submitTopology("helloTopologyy", config, builder.createTopology());
        	Thread.sleep(10000);
        }
        catch (Exception e) {
        	e.printStackTrace();
        } finally {
        	cluster.shutdown();
        }
	}
}
