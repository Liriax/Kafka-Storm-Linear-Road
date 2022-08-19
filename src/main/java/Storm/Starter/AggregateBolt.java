package Storm.Starter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AggregateBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5207693499680815007L;
	//"type","time","vid","spd","xway","lane","dir","seg","pos"
    private OutputCollector collector;
    private Map<Long, Map<Integer[],Set<Long>>> timeToVehicle = new HashMap<Long, Map<Integer[],Set<Long>>>();
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	public void execute(Tuple input) {
		Long currTime = input.getLongByField("time");
		Long currMin = currTime/60 + 1;
		Long firstMin = currMin - 6;
		Long vid = input.getLongByField("vid");
		int currSeg = input.getIntegerByField("seg");
		int currXway = input.getIntegerByField("xway");
		int currDir = input.getIntegerByField("dir");
		Integer[] currPos = new Integer[3];
		currPos[0] = currXway;
		currPos[1] = currSeg;
		currPos[2] = currDir;
		
		timeToVehicle.remove(firstMin-1);
		Map<Integer[],Set<Long>> carsMap = timeToVehicle.get(currMin);
		if (carsMap == null) {
			carsMap = new HashMap<Integer[],Set<Long>>();
			timeToVehicle.put(currMin, carsMap);
		}
		Set<Long> cars = carsMap.get(currPos);
		if (cars == null) {
			cars = new HashSet<Long>();
			timeToVehicle.get(currMin).put(currPos, cars);
		}
		cars.add(vid);
		int ncars = cars.size();
		System.out.println(ncars);
		collector.emit(new Values(currMin, currXway, currSeg, currDir, ncars));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "xway", "seg", "dir", "ncars"));
		
	}

}
