package edu.bilkent.cs545.bolts;

import java.util.ArrayList;
import java.util.HashMap;

import edu.bilkent.cs545.utils.Pair;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class LossyCountingBolt extends BaseBasicBolt{
	HashMap <String, Integer> map;
	
	public LossyCountingBolt(){
		map = new HashMap <String, Integer>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		ArrayList <Pair> pairs = (ArrayList<Pair>) input.getValue(0);
		for(Pair p : pairs){
			System.out.println("First: " + p.getFirst() + " Second: " + p.getSecond() + " pair: " + p.getForwards() );
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
}
