package com.es.api;

import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import com.owl.common.entity.OwlProject;
import com.owl.common.log.LogTool;
import com.owl.common.util.GsonUtil;

/**
 * 根据不同索引名字定义借口类
 * @author admin
 *
 */
public class ProjectBulkServiceImpl extends ESBase implements BulkService {
	private LogTool log = LogTool.getInstance(ProjectBulkServiceImpl.class);
	
    private String index;
    private String type;
    private List<OwlProject> list;
    public ProjectBulkServiceImpl(String index,String type,List<OwlProject> list){
    	this.index=index;
    	this.type=type;
    	this.list=list;
    }
	public boolean invoker() {
		// TODO Auto-generated method stub
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		
		for(OwlProject json: list){
			String objectToJson = GsonUtil.objectToJson(json);
			bulkRequest.add(client.prepareIndex(index, type, json.getnProjectId().toString())
			        .setSource(objectToJson)
					);
		}
		
		BulkResponse bulkResponse = bulkRequest.get();
		
		if (bulkResponse.hasFailures()) {
			log.debug("批量索引失败");
			return false;
		}
		return false;
	}

}
