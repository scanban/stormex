package examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import examples.tx.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import storm.trident.TridentTopology;

public class TridentTransactionApp
{
    public static void main( String[] args ) throws Throwable
    {
        Logger.getRootLogger().setLevel(Level.ERROR);

        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newStream("TridentTxSpout", new TridentTxSpout()).
                shuffle().each(new Fields("msg"), new OpPrintout()).parallelismHint(2).
                global().partitionPersist(new TxDatabaseFactory(),
                        new Fields("msg"), new TxDatabaseUpdater());




        Config config = new Config();
        config.setDebug(false);
        //config.setNumWorkers(32);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("T2", config, tridentTopology.build());
        Thread.sleep(1000*100);
        cluster.shutdown();

    }
}
