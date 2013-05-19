package edu.bilkent.cs545.bolts;

import java.util.ArrayList;

import edu.bilkent.cs545.utils.Pair;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CreatePairsBolt extends BaseBasicBolt{

	
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String tweet = (String) input.getValue(0);
		String [] words = tweet.split(" ");
		ArrayList<Pair> pairs = new ArrayList<Pair>();
		if(words.length > 1){
			for(int i = 0; i<words.length; i++){
				for(int k = i+1; k<words.length; k++){
					Pair p = new Pair(words[i], words[k]);
					pairs.add(p);
				}
			}
			collector.emit(new Values(pairs));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("wordPairs"));
		
	}

}
