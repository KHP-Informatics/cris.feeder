package kcl.iop.brc.core.kconnect.crisfeeder;

import gate.cloud.batch.DocumentID;
import gate.util.GateException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import kcl.iop.brc.core.kconnect.utils.JSONUtils;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;

import ws.nuist.util.Configurator;

public class ESClientWorker {
	private CloseableHttpClient _httpClient;
	private static ESClientWorker _instance;
	private int esDocSeqId = 1;
	
	public synchronized static ESClientWorker getInstance(){
		if (_instance == null){
			_instance = new ESClientWorker();
		}
		return _instance;
	}
	
	private ESClientWorker(){
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		_httpClient = HttpClients.custom()
		        .setConnectionManager(cm)
		        .build();
	}
	
	public String httpGetJSON(String url, Map<String, String> dataMap) throws ClientProtocolException, IOException, URISyntaxException{
		HttpContext context = HttpClientContext.create();
		URIBuilder builder = new URIBuilder();
	    builder = builder.setPath(url);
	    if (dataMap != null){
	    	for (String key : dataMap.keySet()){
	    		builder.setParameter(key, dataMap.get(key));
	    	}
	    }
		HttpGet httpget = new HttpGet(builder.build());
		CloseableHttpResponse response = _httpClient.execute(
                httpget, context);
        try {
            HttpEntity entity = response.getEntity();
            return IOUtils.toString(entity.getContent(), "UTF-8");
        } finally {
            response.close();
        }
	}
	
	/**
	 * get the elasticsearch document number
	 * @param esAPIUrl
	 * @return
	 * @throws Exception
	 */
	public int getESDocNum(String esAPIUrl) throws Exception{
		String jsonRet = null;
		try {
			Map<String, String> params = new LinkedHashMap<String, String>();
			params.put("query", "{\"match_all\":{}}");
			params.put("size", "0"); //return nothing
			params.put("fields", "[]"); 
			jsonRet = httpGetJSON(esAPIUrl + "/_search", params);
			Map<String, Object> retMap = (Map<String, Object>)JSONUtils.fromJSON(jsonRet);
			if (retMap.containsKey("hits")){
				Map hits = (Map)retMap.get("hits");
				if (hits.containsKey("total"))
					return Integer.parseInt(hits.get("total").toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		throw new Exception(String.format("cannot extract document number from [%s]", jsonRet));
	}
	
	/**
	 * get elasticsearch document IDs
	 * @param esAPIUrl
	 * @param from
	 * @param size
	 * @return
	 * @throws Exception
	 */
	public List<String> getESDocIDs(String esAPIUrl, int from, int size) throws Exception{
		String jsonRet = null;
		try {
			Map<String, String> params = new LinkedHashMap<String, String>();
			params.put("query", "{\"match_all\":{}}");
			params.put("from", "" + from); //offset
			params.put("size", "" + size); //page size
			params.put("fields", "[]"); 
			
			jsonRet = httpGetJSON(esAPIUrl + "/_search", params);
			Map<String, Object> retMap = (Map<String, Object>)JSONUtils.fromJSON(jsonRet);
			if (retMap.containsKey("hits")){
				Map hits = (Map)retMap.get("hits");
				if (hits.containsKey("hits")){
					List<String> docIdList = new LinkedList<String>();
					List<Map<String, String>> docList = (List<Map<String, String>>)hits.get("hits");
					for(Map<String, String> kvs : docList){
						docIdList.add(kvs.get("_id"));
					}
					System.out.println(String.format("read ids %s, %s", from, size));
					return docIdList;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		throw new Exception(String.format("cannot extract document IDs from [%s]", jsonRet));
	}
	
	public Map getESDocumentDetail(String esAPIUrl, String docId) throws Exception{
		String jsonRet = null;
		try {
			jsonRet = httpGetJSON(esAPIUrl + "/" + docId, null);
			System.out.println(jsonRet);
			Map<String, Object> retMap = (Map<String, Object>)JSONUtils.fromJSON(jsonRet);
			if (retMap.containsKey("_source")){
				return (Map)retMap.get("_source");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		throw new Exception(String.format("cannot extract document number from [%s]", jsonRet));
	}
	
	public synchronized void setESDocSeqId(int id){
		this.esDocSeqId = id;
	}
	
	public synchronized int nextESDocSeqId(){
		return this.esDocSeqId++;
	}
	
	public boolean saveESDoc(String esStoreUrl, String jsonData, int docId) throws IOException{
		CloseableHttpResponse response = null;
		try {
			HttpPost httpPost = new HttpPost(esStoreUrl + "/" + docId);
			StringEntity entity = new StringEntity(jsonData);
		    httpPost.setEntity(entity);
		    response = _httpClient.execute(httpPost);
			System.out.println(IOUtils.toString(response.getEntity().getContent(), "UTF-8"));
			return response.getStatusLine().getStatusCode() == 200;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
            response.close();
        }
		return false;
	}
	
	public static void main(String[] args){
		try {
			ESClientWorker w = ESClientWorker.getInstance();
			for(int i=0;i<100;i++){
				Map<String, String> data = new LinkedHashMap<String, String>();
				data.put("ann", "abcdefg");
				System.out.println(w.saveESDoc(Configurator.getConfig("es_annotation_storage_url"), 
						JSONUtils.toJSON(data), w.nextESDocSeqId()));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
