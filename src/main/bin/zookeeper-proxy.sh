#! /bin/sh
# Author: Lisa Glendenning <lglenden@cs.washington.edu>

: ${ZOOKEEPER_PROXY_PREFIX=$(dirname $0)/..}
: ${ZOOKEEPER_PROXY_LIB=${ZOOKEEPER_PROXY_PREFIX}/lib}
: ${ZOOKEEPER_PROXY_ETC=${ZOOKEEPER_PROXY_PREFIX}/etc}
: ${ZOOKEEPER_PROXY_LOG=${ZOOKEEPER_PROXY_PREFIX}/log}

: ${JAVA=java}
: ${JAVA_CLASSPATH="${ZOOKEEPER_PROXY_LIB}/*"}
: ${JAVA_MAIN="org.apache.zookeeper.proxy.netty.NettyProxyMain"}
: ${JAVA_MAIN_ARGS=""}

if [ ! -z ${ZOOKEEPER_PROXY_LOG} ]
then
	if [ -f ${ZOOKEEPER_PROXY_ETC}/log4j2-test.xml ]
	then
	    ZOOKEEPER_PROXY_LOG="-Dlog4j.configurationFile=${ZOOKEEPER_PROXY_ETC}/log4j2-test.xml"
	elif [ -f ${ZOOKEEPER_PROXY_ETC}/log4j2.xml ]
	then
	    ZOOKEEPER_PROXY_LOG="-Dlog4j.configurationFile=${ZOOKEEPER_PROXY_ETC}/log4j2.xml"
	else
		ZOOKEEPER_PROXY_LOG=""
	fi
fi

# this isn't an ideal way to run the command, but it's a bit tricky
# otherwise to avoid expanding the * in the classpath

: ${JAVA_ARGS="-classpath \"${JAVA_CLASSPATH}\" ${ZOOKEEPER_PROXY_LOG}"}

COMMAND="${JAVA} ${JAVA_ARGS} ${JAVA_MAIN} ${JAVA_MAIN_ARGS}"
eval "${COMMAND}"
