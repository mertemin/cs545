package cs545.project;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class ProjectTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweet", new TweetSpout());
		builder.setBolt("bag-of-words", new CleanBolt(), 2).noneGrouping("tweet");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(3);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("project-v1", conf, builder.createTopology());
		// Utils.sleep(10000);
		// cluster.killTopology("log-aggregator-v1");
		// cluster.shutdown();
	}

}
