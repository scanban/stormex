package examples.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import examples.data.Cdr;
import examples.data.Clients;

import java.util.Map;
import java.util.Random;

public class CdrSpout extends BaseRichSpout {
    private int count;
    private SpoutOutputCollector collector;
    private Random random;
    private static final String NUMBERS[] =
            Clients.getClients().keySet().toArray(new String[0]);

    private Cdr generate() {
        return new Cdr(NUMBERS[random.nextInt(NUMBERS.length)],
                Long.toString(Math.abs(random.nextLong())),
                random.nextInt(32768));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cdr"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        random = new Random();
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(100);
            if(++count > 20) { return; }
        } catch (InterruptedException e) {}
        collector.emit(new Values(generate()));
    }
}
