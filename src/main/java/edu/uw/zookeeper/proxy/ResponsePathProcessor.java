package edu.uw.zookeeper.proxy;

import java.util.List;
import java.util.ListIterator;

import com.google.common.base.Function;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.util.Processor;

public class ResponsePathProcessor implements Processor<Operation.Response, Operation.Response> {

    public static ResponsePathProcessor newInstance(Function<String, String> transform) {
        return new ResponsePathProcessor(transform);
    }

    protected final Function<String, String> transform;

    protected ResponsePathProcessor(Function<String, String> transform) {
        super();
        this.transform = transform;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Operation.Response apply(Operation.Response input) {
        Operation.Response output = input;
        Records.ResponseRecord record;
        if (input instanceof Records.ResponseRecord) {
            // Modify in place
            record = (Records.ResponseRecord)input;
            if (record instanceof IMultiResponse) {
                for (Operation.Response request: (IMultiResponse)record) {
                    apply(request);
                }
            } else if (record instanceof Records.PathRecord) {
                Records.PathRecord pathed = (Records.PathRecord)record;
                String path = pathed.getPath();
                String transformed = transform.apply(path);
                if (path != transformed) {
                    pathed.setPath(transformed);
                }
            } else if (record instanceof Records.ChildrenHolder) {
                List<String> children = ((Records.ChildrenHolder)record).getChildren();
                ListIterator<String> itr = children.listIterator();
                while (itr.hasNext()) {
                    String path = itr.next();
                    String transformed = transform.apply(path);
                    if (path != transformed) {
                        itr.set(transformed);
                    }
                }
            }
        } else if (input instanceof Operation.RecordHolder) {
            record = ((Operation.RecordHolder<? extends Records.ResponseRecord>)input).asRecord();
            apply(record);
        }
        return output;
    }
}
