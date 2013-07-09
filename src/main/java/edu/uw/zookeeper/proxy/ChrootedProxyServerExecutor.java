package edu.uw.zookeeper.proxy;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;

import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionResponseMessage;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Publisher;

public class ChrootedProxyServerExecutor<C extends Connection<? super Message.ClientSession>> extends ProxyServerExecutor<C> {

    public static <C extends Connection<? super Message.ClientSession>> ChrootedProxyServerExecutor<C> newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            Factory<ClientConnectionExecutor<C>> clientFactory,
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
    
    public static <C extends Connection<? super Message.ClientSession>> ChrootedProxyServerExecutor<C> newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids,
            AssignXidProcessor xids,
            Factory<ClientConnectionExecutor<C>> clientFactory,
            ZNodeLabel.Path chroot) {
        return new ChrootedProxyServerExecutor<C>(
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

        public static ChrootedProxyServerExecutor.ChrootedProxyReplyProcessor newInstance(
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
        public Message.ServerResponse apply(Pair<Optional<Message.ClientRequest>, Message.ServerResponse> input) throws Exception {
            Optional<Message.ClientRequest> request = input.first();
            Message.ServerResponse reply = input.second();
            
            int xid;
            if (request.isPresent()){
                xid = request.get().xid();
            } else {
                xid = reply.xid();
            }
            
            Records.Response payload = chroots.apply(reply.response());
            Long zxid = zxids.apply(payload);
            return SessionResponseMessage.newInstance(xid, zxid, payload);
        }
    }
    
    protected final ZNodeLabel.Path chroot;
    
    protected ChrootedProxyServerExecutor(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions, 
            AssignZxidProcessor zxids,
            AssignXidProcessor xids, 
            Processor<Records.Request, Records.Request> requestProcessor,
            Processor<Pair<Optional<Message.ClientRequest>, Message.ServerResponse>, Message.ServerResponse> replyProcessor,
            Factory<ClientConnectionExecutor<C>> clientFactory,
            ZNodeLabel.Path chroot) {
        super(executor, publisherFactory, sessions, zxids, xids, requestProcessor, replyProcessor, clientFactory);
        this.chroot = chroot;
    }
}