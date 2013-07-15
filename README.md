Simple ZooKeeper server proxy based on [zookeeper-lite](http://github.com/lisaglendenning/zookeeper-lite).

## Quickstart

<pre>
$ mvn exec:java -Dexec.classpathScope=test -Dexec.mainClass="edu.uw.zookeeper.proxy.netty.Main" -Dexec.args="--help"
Usage: edu.uw.zookeeper.proxy.netty.Main [--ensemble=Ensemble] [--help] [--clientAddress=Address]
</pre>

## Building

Both zookeeper-proxy and zookeeper-lite are Maven projects. Build zookeeper-lite first and install it into your local Maven repository.

Both zookeeper-proxy and zookeeper-lite use SLF4J for logging. Apache Log4J2 is used as the SLF4J backend in test scope.

## Running

1. First, start a vanilla ZooKeeper standalone server or ensemble.
2. Then, start the ZooKeeper proxy. Specify the backend server network address with the ``--ensemble=HOSTNAME:PORT`` command line argument. Specify the network address that clients will connect to with the ``--clientAddress=:PORT`` command line argument. The default client address is ``*/2181``.
3. Finally, connect to the proxy with your choice of ZooKeeper client.
