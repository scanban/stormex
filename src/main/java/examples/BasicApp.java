package examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import examples.storm.CdrSpout;
import examples.storm.PrintOutBolt;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class BasicApp
{
    public static void main( String[] args ) throws Throwable
    {
        Logger.getRootLogger().setLevel(Level.ERROR);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("CdrReader", new CdrSpout());
        builder.setBolt("PrintOutBolt", new PrintOutBolt(), 2).shuffleGrouping("CdrReader");

        Config config = new Config();
        config.setDebug(false);
        //config.setNumWorkers(32);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("T1", config, builder.createTopology());
        Thread.sleep(1000*10);
        cluster.shutdown();

    }
}
