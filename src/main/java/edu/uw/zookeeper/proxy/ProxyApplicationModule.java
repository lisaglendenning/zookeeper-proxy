package edu.uw.zookeeper.proxy;


import com.google.common.base.Optional;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.EnsembleQuorumView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ClientProtocolExecutorsService;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.Connection.CodecFactory;
import edu.uw.zookeeper.protocol.CodecConnection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.proxy.netty.NettyModule;
import edu.uw.zookeeper.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpireSessionsTask;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.Server;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.server.SessionParametersPolicy;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceApplication;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.TimeValue;

public enum ProxyApplicationModule implements ParameterizedFactory<RuntimeModule, Application> {
    INSTANCE;
    
    public static ProxyApplicationModule getInstance() {
        return INSTANCE;
    }

    public static String CHROOT_ARG = "chroot";
    public static ZNodeLabel.Path EMPTY_CHROOT = ZNodeLabel.Path.root();
    
    @Override
    public Application get(RuntimeModule runtime) {
        ServiceMonitor monitor = runtime.serviceMonitor();
        AbstractMain.MonitorServiceFactory monitorsFactory = AbstractMain.monitors(monitor);

        NettyModule netModule = NettyModule.newInstance(runtime);
        
        // Client
        TimeValue timeOut = ClientApplicationModule.TimeoutFactory.newInstance().get(runtime.configuration());
        ParameterizedFactory<Connection<Message.ClientSessionMessage>, PingingClientCodecConnection> clientCodecConnectionFactory = 
                PingingClientCodecConnection.factory(timeOut, runtime.executors().asScheduledExecutorServiceFactory().get());
        CodecFactory<Message.ClientSessionMessage, Message.ServerSessionMessage, PingingClientCodecConnection> clientCodecFactory = CodecConnection.factory(clientCodecConnectionFactory);
        ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections = monitorsFactory.apply(netModule.clientConnectionFactory(clientCodecFactory).get());

        EnsembleQuorumView<?> ensemble = ClientApplicationModule.ConfigurableEnsembleViewFactory.newInstance().get(runtime.configuration());
        AssignXidProcessor xids = AssignXidProcessor.newInstance();
        EnsembleViewFactory ensembleFactory = EnsembleViewFactory.newInstance(clientConnections, xids, ensemble, timeOut);
        ClientProtocolExecutorsService clients = monitorsFactory.apply(
                ClientProtocolExecutorsService.newInstance(ensembleFactory));
        
        // Server
        ParameterizedFactory<Connection<Message.ServerMessage>, ServerCodecConnection> serverCodecConnectionFactory = 
                ServerCodecConnection.factory(runtime.publisherFactory());
        CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection> serverCodecFactory = CodecConnection.factory(serverCodecConnectionFactory);
        ServerView.Address<?> address = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration());
        ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections = monitorsFactory.apply(netModule.serverConnectionFactory(serverCodecFactory).get(address.get()));
        SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(runtime.configuration());
        ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(runtime.publisherFactory().get(), policy);
        ExpireSessionsTask expires = monitorsFactory.apply(ExpireSessionsTask.newInstance(sessions, runtime.executors().asScheduledExecutorServiceFactory().get(), runtime.configuration()));
        final AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
        
        Arguments arguments = runtime.configuration().asArguments();
        if (! arguments.has(CHROOT_ARG)) {
            arguments.add(arguments.newOption(CHROOT_ARG, Optional.of("Path"), Optional.of(EMPTY_CHROOT.toString())));
        }
        arguments.parse();
        ZNodeLabel.Path chroot = ZNodeLabel.Path.of(arguments.getValue(CHROOT_ARG));
        final ProxyServerExecutor serverExecutor = EMPTY_CHROOT.equals(chroot)
                ? ProxyServerExecutor.newInstance(
                        runtime.executors().asListeningExecutorServiceFactory().get(), runtime.publisherFactory(), sessions, zxids, xids, clients)
                : ProxyServerExecutor.ChrootedProxyServerExecutor.newInstance(
                        runtime.executors().asListeningExecutorServiceFactory().get(), runtime.publisherFactory(), sessions, zxids, xids, clients, chroot);
        final Server server = Server.newInstance(runtime.publisherFactory(), serverConnections, serverExecutor);

        return ServiceApplication.newInstance(runtime.serviceMonitor());
    }
}
