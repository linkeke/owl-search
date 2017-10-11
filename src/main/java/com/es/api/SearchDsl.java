package com.es.api;

import static org.elasticsearch.index.query.QueryBuilders.*;

import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class SearchDsl extends ESBase {		
	
	//full text query(analyze the query string before executing)
	/**
	 * 匹配所有的文档
	 * @return
	 */
	public QueryBuilder matchAll(){
		return  matchAllQuery();
	}
	
	/**
	 * 标准查询，包括模糊匹配和短语或接近查询
	 * @param field 
	 * @param text
	 * @return
	 */
	public QueryBuilder match_query(String field,String text){
		QueryBuilder qb = matchQuery(
				field,                  
				text   
			);
		return qb;
	}
	
	/**
	 * 多个字段的字符查询
	 * @param text
	 * @param fields
	 * @return
	 */
	public QueryBuilder multi_match_query(String text,String... fields){
		QueryBuilder qb = multiMatchQuery(
				text, 
				fields    
			);
		return qb;
	}
	

	public QueryBuilder query_string_query(String text){
		QueryBuilder qb = queryStringQuery("+"+text);   
		return qb;
	}

	public QueryBuilder simple_query_string_query(String text){
		QueryBuilder qb = simpleQueryStringQuery("+"+text);   
		return qb;
	}
	
	
	//term-level queries
	/**
	 * 查询指定字段的文档(中文搜索不出来，不知道什么原因)
	 * @param field
	 * @param text
	 * @return
	 */
	public QueryBuilder term_query(String field,String text){
		QueryBuilder qb = termQuery(
				field,    
				text   
			); 
		return qb;
	}
	
	/**
	 * 查询多个指定字段的文档
	 * @param field
	 * @param text
	 * @return
	 */
	public QueryBuilder terms_query(String field,String... text){
		QueryBuilder qb = termsQuery(field,    
				text);        
		return qb;
	}
	
	/**
	 * 范围查询
	 * include lower value means that from is gt(>) when false or gte(>=) when true
	 * include upper value means that to is lt(<) when false or lte(<=) when true
	 * @param field
	 * @param from
	 * @param to
	 * @return
	 */
	public QueryBuilder range_query(String field,int from,int to){
		QueryBuilder qb = rangeQuery(field)   
			    .from(from)                            
			    .to(to)                             
			    .includeLower(true)                 
			    .includeUpper(true);      
		return qb;
	}
	
	/**
	 * 查找非null字段的文档
	 * @param field
	 * @return
	 */
	public QueryBuilder exists_query(String field){
		QueryBuilder qb = existsQuery(field);     
		return qb;
	}
	
	/**
	 * 查找指定前缀的文档(中文搜索不出来，不知道什么原因)
	 * @param field
	 * @param text
	 * @return
	 */
	public QueryBuilder prefix_query(String field,String text){
		QueryBuilder qb = prefixQuery(
				field,    
				text     
			);
		return qb;
	}

	/**
	 * 支持单个字符通配符 （？） 和多字符通配符 （*）
	 * QueryBuilder qb = wildcardQuery("user", "k?mc*");
	 * @param field
	 * @param text
	 * @return
	 */
	public QueryBuilder wildcard_query(String field,String text){
		QueryBuilder qb = wildcardQuery(field, text);
		return qb;
	}
	
	/**
	 * 模糊查询（Deprecated in 3.0.0.）
	 * @param field
	 * @param text
	 * @return
	 */
	public QueryBuilder fuzzy_query(String field,String text){
		QueryBuilder qb = fuzzyQuery(
				field,     
				text    
			);
		return qb;
	}
	
	/**
	 * 查找指定类型的文档
	 * @param type
	 * @return
	 */
	public QueryBuilder type_query(String type){
		QueryBuilder qb = typeQuery(type);
		return qb;
	}
	
	/**
	 * 指定类型指定ID查询
	 * @param type
	 * @param ids
	 * @return
	 */
	public QueryBuilder ids_query(String type,String... ids){
		QueryBuilder qb = idsQuery(type)
			    .addIds(ids);
		return qb;
	}
	
	//复合查询Compound queries
	/**
	 * 
	 * @param queryBuilder
	 * @param score
	 * @return
	 */
	public QueryBuilder constant_score_query(QueryBuilder queryBuilder,float score){
		QueryBuilder qb = constantScoreQuery(
				queryBuilder      
		    )
		    .boost(score);     
		return qb;
	}
	
	/**
	 * 
	 * @return
	 */
	public QueryBuilder bool_query(){
		QueryBuilder qb = boolQuery()
			    .must(matchQuery("cCompanyName", "州国枫会"))    
			    .must(matchQuery("cStageName", "天使轮"))    
			    .mustNot(matchQuery("cCompanyName", "老虎")) 
			    .should(matchQuery("content", "test3"))  
			    .filter(matchQuery("content", "test5"));   
		return qb;
	}
	
	/**
	 * 
	 * @return
	 */
	public QueryBuilder dismax_query(){
		QueryBuilder qb = disMaxQuery()
			    .add(matchQuery("cCompanyName", "州国枫会"))        
			    .add(matchQuery("cStageName", "天使轮")) 
			    .boost(1.2f)                             
			    .tieBreaker(1f); 
		return qb;
	}
	
	/**
	 * 
	 * @return
	 */
	public QueryBuilder boosting_query(){
		QueryBuilder qb = boostingQuery(
				matchQuery("cCompanyName", "州国枫会"),    
				matchQuery("cStageName", "天使轮"))  
		    .negativeBoost(0.2f);
		return qb;
	}
	
	/**
	 * 基于内容的推介
	 * fields 查询字段
	 * likeTexts 匹配的推介内容
	 * min_term_freq：一篇文档中一个词语至少出现次数，小于这个值的词将被忽略，默认是2
	 * max_query_terms：一条查询语句中允许最多查询词语的个数，默认是25
	 * @return
	 */
	public QueryBuilder morelike_query(){
		String[] fields = {"cCompanyName", "cAddr"};                 
		String[] likeTexts = {"国枫会"};                       
		Item[] items = null;
		
		QueryBuilder qb = moreLikeThisQuery(fields, likeTexts, items)
			    .minTermFreq(1)                                            
			    .maxQueryTerms(12);  
		
		return qb;
	}
	
	
	//Span queries
	/**
	 * (好像中文不起作用)
	 * @param field
	 * @param value
	 * @return
	 */
	public QueryBuilder spanterm_query(String field,String value){
		QueryBuilder qb = spanTermQuery(
				field,                                     
			    value                                    
			);		
		return qb;
	}
	
	
	public QueryBuilder spanMulti_query(){
		QueryBuilder qb = spanMultiTermQueryBuilder(
			    prefixQuery("user", "ki")                   
			);
		return qb;
	}
	

	
}
