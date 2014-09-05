package br.com.dojo.storm.topology;

import java.util.Arrays;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import br.com.dojo.storm.bolt.PrinterBolt;
import br.com.dojo.storm.spout.FakeTwiterSpout;

public class StormDojoTopology {
	
	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;
	
	private StormDojoTopology(String...args) throws Exception {
		super();
		this.builder.setSpout("FakeTwitterSpout", new FakeTwiterSpout(), 1);
		this.builder.setBolt("PrinterBolt", new PrinterBolt(), 20).shuffleGrouping("FakeTwitterSpout");
	}

	public static void main(String[] args) throws Exception {
		List<String> params = Arrays.asList(args);
		StormDojoTopology topology = new StormDojoTopology(args);
		if(!params.contains("CLUSTER")){
			topology.runLocal(240000);
		}else{
			topology.runCluster("DOJO");
		}
	}
	
	public void runLocal(int runTime) {
		conf.setDebug(false);
		conf.setMaxTaskParallelism(100);
		cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		if (runTime > 0) {
			Utils.sleep(runTime);
			shutDownLocal();
		}
	}
	
	public void shutDownLocal() {
		if (cluster != null) {
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
	
	public void runCluster(String name) throws AlreadyAliveException, InvalidTopologyException {
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}
}
