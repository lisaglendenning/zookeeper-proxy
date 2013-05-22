package edu.uw.zookeeper.proxy;


import com.google.common.base.Optional;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.EnsembleQuorumView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ClientProtocolConnectionsService;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
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

public enum ProxyApplicationModule implements ParameterizedFactory<AbstractMain, Application> {
    INSTANCE;
    
    public static ProxyApplicationModule getInstance() {
        return INSTANCE;
    }

    public static String CHROOT_ARG = "chroot";
    public static ZNodeLabel.Path EMPTY_CHROOT = ZNodeLabel.Path.root();
    
    @Override
    public Application get(AbstractMain main) {
        ServiceMonitor monitor = main.serviceMonitor();
        AbstractMain.MonitorServiceFactory monitorsFactory = AbstractMain.monitors(monitor);

        NettyModule netModule = NettyModule.newInstance(main);
        
        // Client
        ClientConnectionFactory clientConnections = monitorsFactory.apply(netModule.clientConnectionFactory().get());
        TimeValue timeOut = ClientApplicationModule.TimeoutFactory.newInstance().get(main.configuration());
        EnsembleQuorumView<?> ensemble = ClientApplicationModule.ConfigurableEnsembleViewFactory.newInstance().get(main.configuration());
        ParameterizedFactory<Connection, PingingClientCodecConnection> codecFactory = PingingClientCodecConnection.factory(
                main.publisherFactory(), timeOut, main.executors().asScheduledExecutorServiceFactory().get());
        AssignXidProcessor xids = AssignXidProcessor.newInstance();
        EnsembleViewFactory ensembleFactory = EnsembleViewFactory.newInstance(clientConnections, main.publisherFactory(), codecFactory, xids, ensemble, timeOut);
        ClientProtocolConnectionsService clients = monitorsFactory.apply(
                ClientProtocolConnectionsService.newInstance(ensembleFactory));
        
        
        // Server
        ServerView.Address<?> address = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(main.configuration());
        ServerConnectionFactory serverConnections = monitorsFactory.apply(netModule.serverConnectionFactory().get(address.get()));
        SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(main.configuration());
        ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(main.publisherFactory().get(), policy);
        ExpireSessionsTask expires = monitorsFactory.apply(ExpireSessionsTask.newInstance(sessions, main.executors().asScheduledExecutorServiceFactory().get(), main.configuration()));
        final AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
        
        Arguments arguments = main.configuration().asArguments();
        if (! arguments.has(CHROOT_ARG)) {
            arguments.add(arguments.newOption(CHROOT_ARG, Optional.of("Path"), Optional.of(EMPTY_CHROOT.toString())));
        }
        arguments.parse();
        ZNodeLabel.Path chroot = ZNodeLabel.Path.of(arguments.getValue(CHROOT_ARG));
        final ProxyServerExecutor serverExecutor = EMPTY_CHROOT.equals(chroot)
                ? ProxyServerExecutor.newInstance(
                        main.executors().asListeningExecutorServiceFactory().get(), main.publisherFactory(), sessions, zxids, xids, clients)
                : ProxyServerExecutor.ChrootedProxyServerExecutor.newInstance(
                        main.executors().asListeningExecutorServiceFactory().get(), main.publisherFactory(), sessions, zxids, xids, clients, chroot);
        final Server server = Server.newInstance(main.publisherFactory(), serverConnections, serverExecutor);

        return ServiceApplication.newInstance(main.serviceMonitor());
    }
}
