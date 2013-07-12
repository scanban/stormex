package examples.faults;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;
import java.util.Random;

public class FailingBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    private Random random = new Random();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(random.nextInt(10) > 3) {
            System.out.println("OUT>> [" + Thread.currentThread().getId() + "] processing message " +
                    tuple.getString(0));
            outputCollector.ack(tuple);
        }
        else {
            System.out.println("OUT>> [" + Thread.currentThread().getId() + "] failing message " +
                    tuple.getString(0));
            outputCollector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
