package examples.tx;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class TxDatabaseFactory implements StateFactory {
    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext,
                           int partitionIndex, int numPartitions) {

        return new TxDatabase();
    }
}
