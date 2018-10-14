package com.blueasdfg.es_springboot;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class MyConfig {

     @Bean
     public TransportClient client() throws UnknownHostException {

          // es地址和端口号,若集群的话,可配置多个
          InetSocketTransportAddress node = new InetSocketTransportAddress(
                  InetAddress.getByName("127.0.0.1"),
                  9300
          );

          // 配置
          Settings settings = Settings.builder()
                  .put("cluster.name", "elasticsearch")
                  .build();


          // 构造client实例
          TransportClient client = new PreBuiltTransportClient(settings);

          //去某个结点发起请求
          client.addTransportAddress(node);

          return client;
     }
}





















