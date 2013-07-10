package examples.storm;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import examples.data.Cdr;

import java.util.ArrayList;
import java.util.List;

public class ClientIdGrouper implements CustomStreamGrouping {
    private List<Integer> tasks;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId,
                        List<Integer> integers) {
        tasks = new ArrayList<>(integers);
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> objects) {
        List<Integer> rvalue = new ArrayList<>(objects.size());
        for(Object o: objects) {
            Cdr cdr = (Cdr) o;

            rvalue.add(tasks.get(cdr.getClientId() % tasks.size()));
        }
        return rvalue;
    }
}
