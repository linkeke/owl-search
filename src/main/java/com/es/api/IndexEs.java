package com.es.api;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;

import com.owl.common.log.LogTool;



public class IndexEs extends ESBase {	
	private LogTool log = LogTool.getInstance(IndexEs.class);
	
	/**
	 * 初始化客户端
	 * @param clusterName
	 * @param ip
	 * @param port
	 */
	public void initClient(String clusterName,String ip,int port){
	
			log.debug("创建elasticSearch客户端。。");
			try {
				getClient(clusterName,ip,port);
				log.debug("创建elasticSearch客户端成功。。");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("创建elasticSearch客户端失败！！！！", e);
			}

	}
	
	/**
	 * 创建索引（指定索引、类型、id、数据json）
	 * @param index 索引名字
	 * @param type 类型名称
	 * @param id 唯一性，取唯一标识
	 * @param json （一个对象的json字符）
	 */
	public void generateIndex(String index,String type,String id,String json){
		IndexResponse response = client.prepareIndex(index, type, id)
		        .setSource(json)
		        .get();		
	}
	
	/**
	 * 删除索引数据
	 * @param index 索引名字
	 * @param type 类型名称
	 * @param id 唯一性，取唯一标识
	 */
	public void deleteIndex(String index,String type,String id){
		DeleteResponse response = client.prepareDelete(index, type, id)
		        .get();
	}
	
	/**
	 * 更新索引数据
	 * @param index 索引名字
	 * @param type 类型名称
	 * @param id 唯一性，取唯一标识
	 * @param json （一个对象的json字符）
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void updateIndex(String index,String type,String id,String json) throws InterruptedException, ExecutionException{
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(index);
		updateRequest.type(type);
		updateRequest.id(id);
		updateRequest.doc(json);
		client.update(updateRequest).get();
	}
	
	/**
	 * 批量导入数据创建索引
	 * @param index
	 * @param type
	 * @param id
	 * @param json
	 */
	public boolean bulkIndex(BulkService service){	    
		return service.invoker();
	}

	public static void main(String[] args) {
		try {
			
			System.out.println("创建elasticSearch客户端。。");
			getClient("my-application","10.0.0.6",9300);
			System.out.println("创建elasticSearch客户端成功。。");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
