package org.example;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader;
import org.apache.kafka.connect.runtime.isolation.Plugins;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class App {


    public static void main(String[] args) {
//        DelegatingClassLoader delegatingClassLoader = new DelegatingClassLoader(Collections.singletonList("/usr/share/java"));

        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("plugin.path", "/usr/share/java");

        Plugins plugins = new Plugins(workerProps);
        Connector connector = plugins.newConnector("io.confluent.connect.jdbc.JdbcSourceConnector");
        System.out.println(connector.toString());
    }
}
