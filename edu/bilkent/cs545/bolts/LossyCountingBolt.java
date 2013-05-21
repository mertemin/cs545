package edu.bilkent.cs545.bolts;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.bilkent.cs545.utils.Pair;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class LossyCountingBolt extends BaseBasicBolt{
	HashMap <String, Integer> map;
	static PrintWriter writer;

	public LossyCountingBolt() throws FileNotFoundException, UnsupportedEncodingException{
		map = new HashMap <String, Integer>();

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		ArrayList <Pair> pairs = (ArrayList<Pair>) input.getValue(0);

		if(pairs.size() == 1){
			if(pairs.get(0).getFirst().equals("kalender")){
				int size = map.size();
				System.out.println("number of pairs: " + size);
				System.exit(0);
			}
		}

		for(Pair p : pairs){
			if(p.check(map)){
				int freq = map.get(p.getForwards());
				freq = freq + 1;
				map.put(p.getForwards(), freq);
			}else{
				map.put(p.getForwards(), 1);
			}

		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
	public static void printMap(HashMap mp, String fileName) throws FileNotFoundException, UnsupportedEncodingException {
		int count = 0;
		writer = new PrintWriter(fileName, "UTF-8");
		Iterator it = mp.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry)it.next();
			writer.println(pairs.getKey() + " = " + pairs.getValue());
			count++;
			it.remove(); // avoids a ConcurrentModificationException
		}
		
		System.out.println("lines written: " + count);
		writer.close();
	}
}
