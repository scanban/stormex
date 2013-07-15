package examples.tx;

import storm.trident.state.State;

public class TxDatabase implements State {
    @Override
    public void beginCommit(Long txId) {
        System.out.println("beginCommit [" + Thread.currentThread().getId() + "] " + txId);
    }

    @Override
    public void commit(Long txId) {
        System.out.println("commit [" + Thread.currentThread().getId() + "] " + txId);
    }
}
