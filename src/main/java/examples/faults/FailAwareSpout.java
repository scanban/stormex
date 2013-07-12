package examples.faults;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.util.*;

public class FailAwareSpout extends BaseRichSpout {
    private static final int MAX_RETRY_COUNT = 10;

    private SpoutOutputCollector outputCollector;

    private Message[] messages = {
            new Message("Message 000"),
            new Message("Message 001"),
            new Message("Message 002"),
            new Message("Message 003"),
            new Message("Message 004"),
            new Message("Message 005")
    };

    private final Deque<Integer> sendQueue = new ArrayDeque<>();

    private static class Message implements Serializable {
        private String message;
        private int failCount;

        private Message(String message) {
            this.message = message;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("msg"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        outputCollector = spoutOutputCollector;
        for(int i = 0; i < messages.length; ++i) { sendQueue.addLast(i);}
    }

    @Override
    public void nextTuple() {
        if(sendQueue.isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {}
            return;
        }

        Integer messageId = sendQueue.removeFirst();
        outputCollector.emit(new Values(messages[messageId].message), messageId);
    }

    @Override
    public void ack(Object msgId) {
        Message m = messages[(Integer) msgId];

        System.out.println("IN>> [" + Thread.currentThread().getId() + "] message " +
                m.message + " processed successfully");
    }

    @Override
    public void fail(Object msgId) {
        Message m = messages[(Integer) msgId];
        if(++m.failCount > MAX_RETRY_COUNT) {
            throw new IllegalStateException("Too many message processing errors");
        }
        System.out.println("IN>> [" + Thread.currentThread().getId() + "] message " +
                m.message + " processing failed " + "[" + m.failCount + "]");
        sendQueue.addLast((Integer) msgId);
    }
}
