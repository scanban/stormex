package examples.tx;

import backtype.storm.topology.FailedException;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class TxDatabaseUpdater extends BaseStateUpdater<TxDatabase> {
    int count;

    @Override
    public void updateState(TxDatabase txDatabase, List<TridentTuple> tridentTuples,
                            TridentCollector tridentCollector) {

        if(++count == 2) throw new FailedException("YYYY");

        for(TridentTuple t: tridentTuples) {
            System.out.println("Updating: " + t.getString(0));
        }
    }
}
