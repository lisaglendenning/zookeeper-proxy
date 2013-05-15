package edu.uw.zookeeper.proxy;

import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListeningExecutorService;

import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionReplyWrapper;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.ServerExecutor;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Processors;
import edu.uw.zookeeper.util.Publisher;

public class ProxyServerExecutor extends ServerExecutor {

    public static ProxyServerExecutor newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            Factory<ClientProtocolConnection> clientFactory) {
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
    
    public static ProxyServerExecutor newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids,
            AssignXidProcessor xids,
            Factory<ClientProtocolConnection> clientFactory) {
        return new ProxyServerExecutor(
                executor,
                publisherFactory,
                sessions,
                zxids, 
                xids, 
                Processors.<Operation.Request>identity(),
                ProxyReplyProcessor.newInstance(zxids),
                clientFactory);
    }

    public static class ProxyReplyProcessor implements Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> {

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
        public Operation.SessionReply apply(Pair<Optional<Operation.SessionRequest>, Operation.SessionReply> input) throws Exception {
            Optional<Operation.SessionRequest> request = input.first();
            Operation.SessionReply reply = input.second();
            
            int xid;
            if (request.isPresent()){
                xid = request.get().xid();
            } else {
                xid = reply.xid();
            }
            
            Operation.Reply payload = reply.reply();
            Long zxid = zxids.apply(payload);
            return SessionReplyWrapper.create(xid, zxid, payload);
        }
    }

    protected final Factory<ClientProtocolConnection> clientFactory;
    protected final AssignXidProcessor xids;
    protected final Processor<Operation.Request, Operation.Request> requestProcessor;
    protected final Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> replyProcessor;
    
    protected ProxyServerExecutor(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids,
            AssignXidProcessor xids,
            Processor<Operation.Request, Operation.Request> requestProcessor,
            Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> replyProcessor,
            Factory<ClientProtocolConnection> clientFactory) {
        super(executor, publisherFactory, sessions, zxids);
        this.xids = xids;
        this.clientFactory = clientFactory;
        this.requestProcessor = requestProcessor;
        this.replyProcessor = replyProcessor;
    }
    
    public AssignXidProcessor xids() {
        return xids;
    }
    
    public Processor<Operation.Request, Operation.Request> asRequestProcessor() {
        return requestProcessor;
    }
    
    public Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> asReplyProcessor() {
        return replyProcessor;
    }
    
    @Override
    protected PublishingSessionRequestExecutor newSessionRequestExecutor(Long sessionId) {
        ClientProtocolConnection client = clientFactory.get();
        try {
            return ProxyRequestExecutor.newInstance(
                    publisherFactory.get(), this, sessionId, client);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
    
    public static class ChrootedProxyServerExecutor extends ProxyServerExecutor {

        public static ChrootedProxyServerExecutor newInstance(
                ListeningExecutorService executor,
                Factory<Publisher> publisherFactory,
                ExpiringSessionManager sessions,
                Factory<ClientProtocolConnection> clientFactory,
                ZNodeLabel.Path chroot) {
            AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
            AssignXidProcessor xids = AssignXidProcessor.newInstance();
            return newInstance(
                    executor,
                    publisherFactory,
                    sessions, 
                    zxids,
                    xids,
                    clientFactory,
                    chroot);
        }
        
        public static ChrootedProxyServerExecutor newInstance(
                ListeningExecutorService executor,
                Factory<Publisher> publisherFactory,
                ExpiringSessionManager sessions,
                AssignZxidProcessor zxids,
                AssignXidProcessor xids,
                Factory<ClientProtocolConnection> clientFactory,
                ZNodeLabel.Path chroot) {
            return new ChrootedProxyServerExecutor(
                    executor,
                    publisherFactory,
                    sessions,
                    zxids, 
                    xids, 
                    ChrootRequestProcessor.newInstance(chroot),
                    ChrootedProxyReplyProcessor.newInstance(zxids, chroot),
                    clientFactory,
                    chroot);
        }

        public static class ChrootedProxyReplyProcessor extends ProxyReplyProcessor {

            public static ChrootedProxyReplyProcessor newInstance(
                    AssignZxidProcessor zxids,
                    ZNodeLabel.Path chroot) {
                return new ChrootedProxyReplyProcessor(
                        zxids,
                        ChrootResponseProcessor.newInstance(chroot));
            }
            
            protected final ChrootResponseProcessor chroots;
            
            protected ChrootedProxyReplyProcessor(
                    AssignZxidProcessor zxids,
                    ChrootResponseProcessor chroots) {
                super(zxids);
                this.chroots = chroots;
            }
            
            @Override
            public Operation.SessionReply apply(Pair<Optional<Operation.SessionRequest>, Operation.SessionReply> input) throws Exception {
                Optional<Operation.SessionRequest> request = input.first();
                Operation.SessionReply reply = input.second();
                
                int xid;
                if (request.isPresent()){
                    xid = request.get().xid();
                } else {
                    xid = reply.xid();
                }
                
                Operation.Reply payload = reply.reply();
                if (payload instanceof Operation.Response) {
                    payload = chroots.apply((Operation.Response)payload);
                }
                Long zxid = zxids.apply(payload);
                return SessionReplyWrapper.create(xid, zxid, payload);
            }
        }
        
        protected final ZNodeLabel.Path chroot;
        
        protected ChrootedProxyServerExecutor(
                ListeningExecutorService executor,
                Factory<Publisher> publisherFactory,
                ExpiringSessionManager sessions, 
                AssignZxidProcessor zxids,
                AssignXidProcessor xids, 
                Processor<Operation.Request, Operation.Request> requestProcessor,
                Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> replyProcessor,
                Factory<ClientProtocolConnection> clientFactory,
                ZNodeLabel.Path chroot) {
            super(executor, publisherFactory, sessions, zxids, xids, requestProcessor, replyProcessor, clientFactory);
            this.chroot = chroot;
        }
    }
}
