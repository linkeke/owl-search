package com.es.api;

import java.net.InetAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;



public class ESBase {
	
	protected static TransportClient client;
	
	
	/**
	 * 创建客户端
	 * @param clusterName
	 * @param host
	 * @param port
	 * @return
	 * @throws Exception
	 */
	public static void getClient(String clusterName,String host,Integer port) throws  Exception{
	
		Settings settings = Settings.builder()
		        .put("cluster.name", clusterName).build();
		 client = new PreBuiltTransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
		
	}
	
	/**
	 * 关闭客户端
	 */
	public void closeClient(){
		client.close();
	}
	
	
	
	



}
