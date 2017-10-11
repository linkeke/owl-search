package com.es.api;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import com.owl.common.log.LogTool;



public class SearchEs extends ESBase {	
	private LogTool log = LogTool.getInstance(SearchEs.class);
	
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
	 * 一般的分页查询
	 * @param index 索引名字
	 * @param type 索引类型
	 * @param from 从0开始
	 * @param size 每页大小
	 * @param sortBuilder 排序
	 * @param queryBuilders 查询信息
	 * @param filterBuilders 过滤信息
	 * @return
	 */
	public SearchResponse  search(String index,String type,int from, int size, SortBuilder sortBuilder, List<QueryBuilder> queryBuilders, List<QueryBuilder> filterBuilders){
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);  	
		//设置查询类型  
		searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);  
		//设置分页信息  
		searchRequestBuilder.setFrom(from).setSize(size);  		  		
		//设置查询信息  
		for(QueryBuilder builder:queryBuilders){
			searchRequestBuilder.setQuery(builder);
		}
		//设置过滤信息  
		for(QueryBuilder builder:filterBuilders){
			searchRequestBuilder.setPostFilter(builder);
		}		
		// 降序  
		if(null != sortBuilder){
			searchRequestBuilder.addSort(sortBuilder);  		  
		}	
		// 设置是否按查询匹配度排序  
		searchRequestBuilder.setExplain(true);  
		
		log.debug("查询条件："+searchRequestBuilder);
	
		//执行查询
		SearchResponse response = searchRequestBuilder.execute().actionGet();

		return response;
	}
	
	/**
	 * scrollSearch
	 * @param index
	 * @param type
	 * @param size
	 * @param sortBuilder
	 * @param queryBuilders
	 * @return
	 */
	public SearchResponse  scrollSearch(String index,String type, int size, SortBuilder sortBuilder, QueryBuilder queryBuilder){
		SearchResponse scrollResp = client.prepareSearch(index).setTypes(type)
		        .addSort(sortBuilder)
		        .setScroll(new TimeValue(60000))
		        .setQuery(queryBuilder)
		        .setSize(size).execute().actionGet(); //max of 100 hits will be returned for each scroll
		//Scroll until no hits are returned
		do {
			
			Long count = scrollResp.getHits().getTotalHits();
			System.out.println("命中总数："+count);
			
		    for (SearchHit hit : scrollResp.getHits().getHits()) {
		        //Handle the hit...
		    	System.out.println(hit);
		    }

		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
		} while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
		return scrollResp;
	} 
	
	
	public void  multiSearch(String index,String type, int size, SortBuilder sortBuilder, QueryBuilder queryBuilder){
		SearchRequestBuilder srb1 = client
			    .prepareSearch().setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(1);
			SearchRequestBuilder srb2 = client
			    .prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);

			MultiSearchResponse sr = client.prepareMultiSearch()
			        .add(srb1)
			        .add(srb2)
			        .execute().actionGet();

			// You will get all individual responses from MultiSearchResponse#getResponses()
			long nbHits = 0;
			for (MultiSearchResponse.Item item : sr.getResponses()) {
			    SearchResponse response = item.getResponse();
			    nbHits += response.getHits().getTotalHits();
			}
	} 

	/**
	 * 分组
	 * @param index
	 * @param type
	 */
	public void aggregations(String index,String type){
		SearchResponse sr = client.prepareSearch(index).setTypes(type)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)  
			    .setQuery(QueryBuilders.matchAllQuery())
			    .addAggregation(
			            AggregationBuilders.terms("group_stage").field("nStageId").size(20)
			    )
			    .execute().actionGet();

			// Get your facet results
			Terms agg = sr.getAggregations().get("group_stage");
			List<Bucket> buckets = agg.getBuckets();
			for(Bucket bucket: buckets){
				 System.out.println("行业ID："+bucket.getKey() + " 对应有：" + bucket.getDocCount() + "个");  
			}
		
	}
	
	public void aggregationsAndSub(String index,String type){
		SearchResponse sr = client.prepareSearch(index).setTypes(type)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)  
			    .setQuery(QueryBuilders.matchAllQuery())
			    .addAggregation(
//			            AggregationBuilders.terms("group_stage").field("nStageId").size(20).subAggregation(AggregationBuilders.sum("group_innerScore").field("nInnerScore"))
			    		AggregationBuilders.terms("group_stage").field("nStageId").size(20).subAggregation(AggregationBuilders.min("group_innerScore").field("nInnerScore"))
//			    		AggregationBuilders.terms("group_stage").field("nStageId").size(20).subAggregation(AggregationBuilders.max("group_innerScore").field("nInnerScore"))
//			    		AggregationBuilders.terms("group_stage").field("nStageId").size(20).subAggregation(AggregationBuilders.avg("group_innerScore").field("nInnerScore"))	
			    		
//			    		AggregationBuilders.terms("group_stage").field("nStageId").size(20).subAggregation(AggregationBuilders.stats("group_innerScore").field("nInnerScore"))
			    		
			    )
			    .execute().actionGet();

			// Get your facet results
			Terms agg = sr.getAggregations().get("group_stage");
			List<Bucket> buckets = agg.getBuckets();
			for(Bucket bt : buckets)  
	        {  
//	            Sum sum = bt.getAggregations().get("group_innerScore");  
	            Min min = bt.getAggregations().get("group_innerScore");  
//				Max max = bt.getAggregations().get("group_innerScore"); 
//				Avg avg = bt.getAggregations().get("group_innerScore"); 
//	            System.out.println(bt.getKey() + "  " + bt.getDocCount() + " "+ sum.getValue()); 
	            System.out.println(bt.getKey() + "  " + bt.getDocCount() + " "+ min.getValue());  
//				System.out.println(bt.getKey() + "  " + bt.getDocCount() + " "+ max.getValue()); 
//				System.out.println(bt.getKey() + "  " + bt.getDocCount() + " "+ avg.getValue()); 
				
//				Stats stats =  bt.getAggregations().get("group_innerScore"); 
//				double min = stats.getMin();
//				double max = stats.getMax();
//				double avg = stats.getAvg();
//				double sum = stats.getSum();
//				long count = stats.getCount();
//				
//				System.out.println(min + "  " + max + " "+ avg + "  " + sum + " "+ count); 
	        }  
		
		
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
