package examples.tx;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ICommitterTridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.util.Map;

public class TridentTxSpout implements ICommitterTridentSpout<TxMeta> {

    static class BCoordinator implements BatchCoordinator<TxMeta> {
        private static final int TRANSACTION_COUNT = 5;
        private static final int TRANSACTION_ELEMENT_COUNT = 5;

        @Override
        public TxMeta initializeTransaction(long l, TxMeta txMeta) {
            if(txMeta != null) {

            System.out.println(String.format("Initializing transaction id: %08d, " +
                    "start: %04d, count: %04d", l, txMeta.getStart() + txMeta.getCount(),
                    txMeta.getCount()));

            return new TxMeta(txMeta.getStart() + txMeta.getCount(),
                    TRANSACTION_ELEMENT_COUNT);
            } else {
                return new TxMeta(0, TRANSACTION_ELEMENT_COUNT);
            }
        }

        @Override
        public void success(long l) {
        }

        @Override
        public boolean isReady(long l) {
            if(l <= TRANSACTION_COUNT) {
                System.out.println("ISREADY " + l);
                return true;
            }
            return false;
        }

        @Override
        public void close() {
        }
    }

    static class BEmitter implements Emitter {

        @Override
        public void emitBatch(TransactionAttempt transactionAttempt, Object coordinatorMeta,
                              TridentCollector tridentCollector) {

            TxMeta txMeta = (TxMeta) coordinatorMeta;

            System.out.println("Emitting transaction id: " +
                    transactionAttempt.getTransactionId() + " attempt:" +
                    transactionAttempt.getAttemptId()
            );
            for(int i = 0; i < txMeta.getCount(); ++i) {
                tridentCollector.emit(new Values("TRANS [" +
                        transactionAttempt.getAttemptId() + "] [" + (txMeta.getStart() + i) + "]")
                );
            }

        }


        @Override
        public void success(TransactionAttempt transactionAttempt) {
            System.out.println("BEmitter:Transaction success id:" + transactionAttempt.getTransactionId());
        }

        @Override
        public void close() {
        }

        @Override
        public void commit(TransactionAttempt transactionAttempt) {
            System.out.println("BEmitter:Transaction commit id:" + transactionAttempt.getTransactionId());
        }
    }


    @Override
    public BatchCoordinator<TxMeta> getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return new BCoordinator();
    }

    @Override
    public Emitter getEmitter(String s, Map map, TopologyContext topologyContext) {
        return new BEmitter();
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }
}
