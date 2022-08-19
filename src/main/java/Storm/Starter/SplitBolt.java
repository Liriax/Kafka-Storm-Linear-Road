package Storm.Starter;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
public class SplitBolt extends BaseRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4386250152999223010L;
    private OutputCollector collector;   
    private static final Logger logger = Logger.getLogger("starter");
    
	public void execute(Tuple input) {
		
		// System.out.println(input.toString());
		String s = input.getString(4);
//		String[] arr = s.split(",");
		// int size = arr.length;
		
//		int type = Integer.parseInt(arr[0]);
//		if (arr.length > 8) {
//			long time = Long.parseLong(arr[1]);
//			Long vid = Long.parseLong(arr[2]);
//			int spd = Integer.parseInt(arr[3]);
//			int xway = Integer.parseInt(arr[4]);
//			int lane = Integer.parseInt(arr[5]);
//			int dir = Integer.parseInt(arr[6]);
//			int seg = Integer.parseInt(arr[7]);
//			Long pos = Long.parseLong(arr[8]);	 
			logger.info(s);
//	        collector.emit( new Values(type,time,vid,spd,xway,lane,dir,seg,pos));
	        collector.emit( new Values(s));
//		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("type","time","vid","spd","xway","lane","dir","seg","pos"));
		declarer.declare(new Fields("array"));
	}

	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		FileHandler handler;
		Locale.setDefault(new Locale("en", "EN"));
		try {
			handler = new FileHandler("splitbolt10000.log", false);
			handler.setFormatter(new SimpleFormatter());
			logger.addHandler(handler);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    
		
	}

}
