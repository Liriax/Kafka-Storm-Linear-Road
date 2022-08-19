package Storm.Starter;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

// spout creates random integer, bolt multiplies them by 2
public class IntegerSpout extends BaseRichSpout {
	SpoutOutputCollector spoutOutputCollector;
	private Integer index = 0;
	
	public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
		this.spoutOutputCollector = collector;
	}

	public void nextTuple() {
		if (index < 100) {
			this.spoutOutputCollector.emit(new Values(index));
			index++;
			
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("field"));
		
	}

}
