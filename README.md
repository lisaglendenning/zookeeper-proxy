ZooKeeper server tracing proxy based on [zookeeper-clients](http://github.com/lisaglendenning/zookeeper-clients).

## Quickstart

<pre>
$ mvn exec:java -Dexec.mainClass="edu.uw.zookeeper.proxy.Main" -Dexec.args="--help"
Usage: edu.uw.zookeeper.proxy.Main [--clientAddress=Address:Port] [--ensemble=Address:Port,...] [--help] [--output=Path] [--timeout=Time] [--trace=BOOLEAN]
</pre>

## Building

zookeeper-proxy is a Maven project.

## Running

1. First, start a vanilla ZooKeeper standalone server or ensemble.
2. Then, start the ZooKeeper proxy. Specify the backend server network address with the ``--ensemble=HOSTNAME:PORT`` command line argument. Specify the network address that clients will connect to with the ``--clientAddress=:PORT`` command line argument. The default client address is ``*/2181``.
3. Finally, connect to the proxy with your choice of ZooKeeper client.
