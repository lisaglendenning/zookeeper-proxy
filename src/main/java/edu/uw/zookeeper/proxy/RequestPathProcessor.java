package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;

public class RequestPathProcessor implements Processor<Operation.Request, Operation.Request> {

    public static RequestPathProcessor newInstance(Function<ZNodeLabel.Path, ZNodeLabel.Path> transform) {
        return new RequestPathProcessor(transform);
    }
    
    protected final Function<ZNodeLabel.Path, ZNodeLabel.Path> transform;
    
    protected RequestPathProcessor(Function<ZNodeLabel.Path, ZNodeLabel.Path> transform) {
        super();
        this.transform = transform;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Operation.Request apply(Operation.Request input) throws Exception {
        Operation.Request output = input;
        if (input instanceof IMultiRequest) {
            IMultiRequest record = new IMultiRequest();
            for (Records.MultiOpRequest e: (IMultiRequest)input) {
                record.add((Records.MultiOpRequest) apply(e));
            }
            output = record;
        } else if (input instanceof Records.PathHolder) {
            Operations.PathBuilder<Operation.Request> builder = (Operations.PathBuilder<Operation.Request>) Operations.Requests.fromRecord(input);
            ZNodeLabel.Path path = builder.getPath();
            ZNodeLabel.Path transformed = transform.apply(path);
            if (! path.equals(transformed)) {
                builder.setPath(transformed);
                output = builder.build();
            }
        }
        return output;
    }
}
