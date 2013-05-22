package edu.uw.zookeeper.proxy;


import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.util.ConfigurableMain;
import edu.uw.zookeeper.util.Configuration;

public class Main extends AbstractMain {

    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.ConfigurableApplicationFactory.newInstance(Main.class));
    }

    public Main(Configuration configuration) {
        super(configuration, ProxyApplicationModule.getInstance());
    }
}
