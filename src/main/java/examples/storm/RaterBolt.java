package examples.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import examples.data.Cdr;
import examples.data.Rater;

import java.util.Map;

public class RaterBolt extends BaseBasicBolt {
    private Rater rater;

    public RaterBolt() {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        rater = new Rater();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Cdr cdr = (Cdr) tuple.getValue(0);
        cdr.setPrice(rater.calculate(cdr.getClientId(),
                cdr.getCallTime(), cdr.getCallDestination()));
        try {
            Thread.sleep(cdr.getClientId() * 100);
        } catch (InterruptedException e) {}
        basicOutputCollector.emit(new Values(cdr));
        System.out.println("RATE>> [" + Thread.currentThread().getId() + "]" + cdr);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cdr"));
    }
}
