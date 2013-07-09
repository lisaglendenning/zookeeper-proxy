package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;

public class RequestPathProcessor implements Processor<Records.Request, Records.Request> {

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
    public Records.Request apply(Records.Request input) throws Exception {
        Records.Request output = input;
        if (input instanceof IMultiRequest) {
            IMultiRequest record = new IMultiRequest();
            for (Records.MultiOpRequest e: (IMultiRequest)input) {
                record.add((Records.MultiOpRequest) apply(e));
            }
            output = record;
        } else if (input instanceof Records.PathGetter) {
            Operations.PathBuilder<Records.Request> builder = (Operations.PathBuilder<Records.Request>) Operations.Requests.fromRecord(input);
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
