package cs545.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TweetSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	File[] fileList;
	BufferedReader br;
	int fileID = 0;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		File folder = new File("/Users/mert/Public/School/CS545/project/data/step2");
		fileList = folder.listFiles();
		reinitializeBufferedReader(fileID);
	}

	@Override
	public void nextTuple() {
		// Wait for a second before emitting
		Utils.sleep(1000);
		try {
			if (br.ready()) {
				// If BufferedReader is ready, then read a line and emit it
				_collector.emit(new Values(br.readLine()));
			} else {
				// BufferedReader reached to the end of file
				// Initilize BufferedReader with the next file in the folder
				reinitializeBufferedReader(++fileID);
			}
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	private void reinitializeBufferedReader(int fileID) {
		try {
			br = new BufferedReader(new FileReader(fileList[fileID]));
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}
}
