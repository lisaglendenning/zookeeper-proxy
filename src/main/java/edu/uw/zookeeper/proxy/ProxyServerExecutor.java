package edu.uw.zookeeper.proxy;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;

import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionResponseMessage;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.ServerExecutor;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Processors;
import edu.uw.zookeeper.util.Publisher;

public class ProxyServerExecutor<C extends Connection<? super Message.ClientSession>> extends ServerExecutor {

    public static <C extends Connection<? super Message.ClientSession>> ProxyServerExecutor<C> newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            Factory<ClientConnectionExecutor<C>> clientFactory) {
        AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
        AssignXidProcessor xids = AssignXidProcessor.newInstance();
        return newInstance(
                executor,
                publisherFactory,
                sessions, 
                zxids,
                xids,
                clientFactory);
    }
    
    public static <C extends Connection<? super Message.ClientSession>> ProxyServerExecutor<C> newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids,
            AssignXidProcessor xids,
            Factory<ClientConnectionExecutor<C>> clientFactory) {
        return new ProxyServerExecutor<C>(
                executor,
                publisherFactory,
                sessions,
                zxids, 
                xids, 
                Processors.<Records.Request>identity(),
                ProxyReplyProcessor.newInstance(zxids),
                clientFactory);
    }

    public static class ProxyReplyProcessor implements Processor<Pair<Optional<Message.ClientRequest>, Message.ServerResponse>, Message.ServerResponse> {

        public static ProxyReplyProcessor newInstance(
                AssignZxidProcessor zxids) {
            return new ProxyReplyProcessor(zxids);
        }
        
        protected final AssignZxidProcessor zxids;
        
        protected ProxyReplyProcessor(
                AssignZxidProcessor zxids) {
            this.zxids = zxids;
        }
        
        @Override
        public Message.ServerResponse apply(Pair<Optional<Message.ClientRequest>, Message.ServerResponse> input) throws Exception {
            Optional<Message.ClientRequest> request = input.first();
            Message.ServerResponse reply = input.second();
            
            int xid;
            if (request.isPresent()){
                xid = request.get().xid();
            } else {
                xid = reply.xid();
            }
            
            Records.Response payload = reply.response();
            Long zxid = zxids.apply(payload);
            return SessionResponseMessage.newInstance(xid, zxid, payload);
        }
    }

    protected final Factory<ClientConnectionExecutor<C>> clientFactory;
    protected final AssignXidProcessor xids;
    protected final Processor<Records.Request, Records.Request> requestProcessor;
    protected final Processor<Pair<Optional<Message.ClientRequest>, Message.ServerResponse>, Message.ServerResponse> replyProcessor;
    
    protected ProxyServerExecutor(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids,
            AssignXidProcessor xids,
            Processor<Records.Request, Records.Request> requestProcessor,
            Processor<Pair<Optional<Message.ClientRequest>, Message.ServerResponse>, Message.ServerResponse> replyProcessor,
            Factory<ClientConnectionExecutor<C>> clientFactory) {
        super(executor, publisherFactory, sessions, zxids);
        this.xids = xids;
        this.clientFactory = clientFactory;
        this.requestProcessor = requestProcessor;
        this.replyProcessor = replyProcessor;
    }
    
    public AssignXidProcessor xids() {
        return xids;
    }
    
    public Processor<Records.Request, Records.Request> asRequestProcessor() {
        return requestProcessor;
    }
    
    public Processor<Pair<Optional<Message.ClientRequest>, Message.ServerResponse>, Message.ServerResponse> asReplyProcessor() {
        return replyProcessor;
    }
    
    @Override
    protected PublishingSessionRequestExecutor newSessionRequestExecutor(Long sessionId) {
        ClientConnectionExecutor<C> client = clientFactory.get();
        return ProxyRequestExecutor.newInstance(
                publisherFactory.get(), this, sessionId, client);
    }
}
