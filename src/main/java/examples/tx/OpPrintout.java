package examples.tx;

import storm.trident.operation.*;
import storm.trident.tuple.TridentTuple;

public class OpPrintout extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.out.println("PROUT>> [" + Thread.currentThread().getId() + "] " +
                tuple.getString(0));
        return true;
    }
}

