package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;

public class ResponsePathProcessor implements Processor<Records.Response, Records.Response> {

    public static ResponsePathProcessor newInstance(Function<ZNodeLabel.Path, ZNodeLabel.Path> transform) {
        return new ResponsePathProcessor(transform);
    }
    
    protected final Function<ZNodeLabel.Path, ZNodeLabel.Path> transform;

    protected ResponsePathProcessor(Function<ZNodeLabel.Path, ZNodeLabel.Path> transform) {
        super();
        this.transform = transform;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Records.Response apply(Records.Response input) {
        Records.Response output = input;
        if (input instanceof IMultiResponse) {
            IMultiResponse record = new IMultiResponse();
            for (Records.MultiOpResponse e: (IMultiResponse)input) {
                record.add((Records.MultiOpResponse) apply(e));
            }
            output = record;
        } else if (input instanceof Records.PathGetter) {
            Operations.PathBuilder<Records.Response> builder = (Operations.PathBuilder<Records.Response>) Operations.Responses.fromRecord(input);
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
