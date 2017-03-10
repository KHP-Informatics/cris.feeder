package kcl.iop.brc.core.kconnect.crisfeeder;

import gate.cloud.batch.DocumentID;
import gate.util.GateException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.xml.bind.DatatypeConverter;

import kcl.iop.brc.core.kconnect.utils.JSONUtils;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.log4j.Logger;

import ws.nuist.util.Configurator;

public class ESClientWorker {
	static Logger _logger = Logger.getLogger(ESClientWorker.class);
	private CloseableHttpClient _httpClient;
	private static ESClientWorker _instance;
	private int esDocSeqId = 1;
	private boolean _needHttpAuthentication = false;
	private String _httpUser, _httpPwd, _authString;
	
	public synchronized static ESClientWorker getInstance(){
		if (_instance == null){
			_instance = new ESClientWorker();
		}
		return _instance;
	}
	
	private ESClientWorker(){
		_needHttpAuthentication = 
				(null == Configurator.getConfig("http_authentication_needed") ? false : Configurator.getConfig("http_authentication_needed").equals("1"));
		_httpUser = Configurator.getConfig("http_user");
		_httpPwd = Configurator.getConfig("http_pwd");
		if (_needHttpAuthentication){
			try {
				_authString =  DatatypeConverter.printBase64Binary(String.format("%s:%s", _httpUser, _httpPwd).getBytes("utf-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			
			//deal with the https certificate issue
			HttpClientBuilder b = HttpClientBuilder.create();
			
			//setup the credential
			if (_needHttpAuthentication){
				CredentialsProvider provider = new BasicCredentialsProvider();
				UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(_httpUser, _httpPwd);
				provider.setCredentials(AuthScope.ANY, credentials);
				b.setDefaultCredentialsProvider(provider);
				_logger.info(String.format("setup http authentication for %s", _httpUser));
			}

			SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
			    public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
			        return true;
			    }
			}).build();
			b.setSslcontext( sslContext);

			// or SSLConnectionSocketFactory.getDefaultHostnameVerifier(), if you don't want to weaken
			HostnameVerifier hostnameVerifier = SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER;

			SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
			Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
			        .register("http", PlainConnectionSocketFactory.getSocketFactory())
			        .register("https", sslSocketFactory)
			        .build();
			// allows multi-threaded use
			PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager( socketFactoryRegistry);
			b.setConnectionManager( connMgr);
			_httpClient = b.build();
		} catch (KeyManagementException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (KeyStoreException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
//		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
//		_httpClient = HttpClients.custom()
//		        .setConnectionManager(cm)		        
//				.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
//		        .build();
		
	}
	
	public String getHttpAuthString(){
		return _authString;
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
//		httpget.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + getHttpAuthString());
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
	
	public List<String> getAllDocIds(String esAPIUrl, int offset, int size) throws Exception{
		String jsonRet = null;
		try {
			Map<String, String> params = new LinkedHashMap<String, String>();
			params.put("search_type", "scan");
			params.put("scroll", "1m" ); //time alive
			params.put("size", "100000"); //page size
			params.put("fields", "[]"); 
			
			jsonRet = httpGetJSON(esAPIUrl + "/_search", params);

			Map<String, Object> retMap = (Map<String, Object>)JSONUtils.fromJSON(jsonRet);
			if (retMap.containsKey("_scroll_id")){
				String scrollId = retMap.get("_scroll_id").toString();
				int pos1 = esAPIUrl.indexOf("://") ;
				String hostUrl = esAPIUrl.substring(0, esAPIUrl.substring(pos1 + 3).indexOf("/") + pos1 + 4);
				params.clear();
				params.put("scroll", "1m" ); //time alive
				params.put("scroll_id", scrollId );
				

				List<String> docIdList = new LinkedList<String>();
				boolean bHaveNext = true;
				int curRead = 0;
				while(bHaveNext){
					jsonRet = httpGetJSON(hostUrl + "/_search/scroll", params);
					retMap = (Map<String, Object>)JSONUtils.fromJSON(jsonRet);
					if (retMap.containsKey("hits")){
						Map hits = (Map)retMap.get("hits");
						if (hits.containsKey("hits") && ((List<Map<String, String>>)hits.get("hits")).size() > 0){
							List<Map<String, String>> docList = (List<Map<String, String>>)hits.get("hits");
							for(Map<String, String> kvs : docList){
								if (curRead < offset + size){
									if (curRead >= offset){
										docIdList.add(kvs.get("_id"));
									}
								}else{
									//read all needed
									bHaveNext = false;
									break;
								}
								curRead++;
							}
						}else{
							bHaveNext = false;
						}
					}else{
						bHaveNext = false;
					}
				}
				return docIdList;
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
//			httpPost.setHeader("Authorization", "Basic " + getHttpAuthString());
			StringEntity entity = new StringEntity(jsonData, "application/json",
				    "UTF-8");
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
			System.out.println(JSONUtils.toJSON(w.getAllDocIds(
					"http://timeline2016-silverash.rhcloud.com/mock/doc",
					500, 100)));
//			for(int i=0;i<100;i++){
//				Map<String, String> data = new LinkedHashMap<String, String>();
//				data.put("ann", "abcdefg");
//				System.out.println(w.saveESDoc(Configurator.getConfig("es_annotation_storage_url"), 
//						JSONUtils.toJSON(data), w.nextESDocSeqId()));
//			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
