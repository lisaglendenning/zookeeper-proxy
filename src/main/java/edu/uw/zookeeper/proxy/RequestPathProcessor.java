package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.util.Processor;

public class RequestPathProcessor implements Processor<Operation.Request, Operation.Request> {

    public static RequestPathProcessor newInstance(Function<String, String> transform) {
        return new RequestPathProcessor(transform);
    }
    
    protected final Function<String, String> transform;
    
    protected RequestPathProcessor(Function<String, String> transform) {
        super();
        this.transform = transform;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Operation.Request apply(Operation.Request input) throws Exception {
        Operation.Request output = input;
        Records.RequestRecord record;
        if (input instanceof Records.RequestRecord) {
            // Modify in place
            record = (Records.RequestRecord)input;
            if (record instanceof IMultiRequest) {
                for (Operation.Request request: (IMultiRequest)record) {
                    apply(request);
                }
            } else if (record instanceof Records.PathRecord) {
                Records.PathRecord pathed = (Records.PathRecord)record;
                String path = pathed.getPath();
                String transformed = transform.apply(path);
                if (path != transformed) {
                    pathed.setPath(transformed);
                }
            }
        } else if (input instanceof Operation.RecordHolder) {
            record = ((Operation.RecordHolder<? extends Records.RequestRecord>)input).asRecord();
            apply(record);
        }
        return output;
    }

}
