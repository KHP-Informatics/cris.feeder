package kcl.iop.brc.core.kconnect.crisfeeder;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import ws.nuist.util.Configurator;
import ws.nuist.util.DBCPPool;
import ws.nuist.util.DBHelper;
import ws.nuist.util.DBUtil;
import ws.nuist.util.exception.DBExecutionException;
import gate.Document;
import gate.Factory;
import gate.FeatureMap;
import gate.cloud.batch.DocumentID;
import gate.cloud.io.DocumentData;
import gate.cloud.io.InputHandler;
import gate.util.GateException;

public class CRISDocInputHandler implements InputHandler {
	static Logger _logger = Logger.getLogger(CRISDocInputHandler.class);
	static String mimeType = Configurator.getConfig("mimieType");
	static String encoding = Configurator.getConfig("encoding");
	static String sqlPrefix = "select "
			+ DBUtil.escapeString(Configurator.getConfig("DocTableContentCol")) + ","
			+ DBUtil.escapeString(Configurator.getConfig("DocTableDocDateCol")) + ","
			+ DBUtil.escapeString(Configurator.getConfig("DocTableDocIDCol"))
			+ ", src_table, src_col, DateModified from "
			+ DBUtil.escapeString(Configurator.getConfig("DocTableName"))
			+ " where "
			+ DBUtil.escapeString(Configurator.getConfig("DocTableDocIDCol"))
			+ "=";
	boolean useXMLConfig = false;
	@Override
	public void close() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

	@Override
	public void config(Map<String, String> settings) throws IOException,
			GateException {
		if (settings.containsKey("dbSettingImportant")){
			useXMLConfig = true;
			try {
				Class.forName(settings.get("db_driver"));
				DBCPPool.setupDefaultDriver(settings.get("db_url"), settings.get("user"), settings.get("password"));
				sqlPrefix = settings.get("get_doc_sql_prefix");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public DocumentData getInputDocument(DocumentID docId) throws IOException,
			GateException {
		if (null == docId || docId.getIdText() == null) return null;
		String sql = sqlPrefix + "'" + DBUtil.escapeString(docId.getIdText()) + "'";
		try {
			String docSql = "";
			String cnDocId = DBUtil.escapeString(docId.getIdText());
			
			FeatureMap params = Factory.newFeatureMap();
		    if(mimeType != null && mimeType.length() > 0) {
		      params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
		    }
		    if(encoding!= null && encoding.length() > 0){
		      params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
		    }
		    params.put(Document.DOCUMENT_MARKUP_AWARE_PARAMETER_NAME, Boolean.TRUE);
		    
			Document doc = null;
			String docContent = "";
			
			if (null == Configurator.getConfig("FixedDocTable")){
				String[] dataRow = useXMLConfig? DBHelper.getStringArray(sql, 7): DBHelper.getStringArray(sql, 6);
				if (!useXMLConfig){
					String tbl = DBUtil.escapeString(dataRow[3]).replaceAll(" ", "_");
					if (tbl.equalsIgnoreCase("CAMHS_Event")){
						cnDocId = "CEV" + cnDocId;
					}
					docSql = "select " + DBUtil.escapeString(dataRow[4]) + 
							" from SQLCRIS.dbo." + tbl + 
							" where CN_Doc_ID='" + cnDocId + "'";
					docContent = DBHelper.getScalar(docSql);
				}else{
					docContent = dataRow[0];
				}		
				

				params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, processBeforeNLP(docContent));
				doc = (Document)Factory.createResource("gate.corpora.DocumentImpl",
		                params, Factory.newFeatureMap(), docId.toString());			    
			    
			    if (dataRow[1] == null || dataRow[1].equalsIgnoreCase("null")){
			    	if (dataRow[5] != null && !dataRow[5].equalsIgnoreCase("null")){
			    		try {
							SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
							Date date = df.parse(dataRow[5]);
							long epoch = date.getTime();
							doc.getFeatures().put("publicationDate", epoch);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
			    	}else{
			    		doc.getFeatures().put("publicationDate", 0);
			    	}
			    	
			    }else{
			    	doc.getFeatures().put("publicationDate", Long.parseLong(dataRow[1]));
			    }
			    doc.getFeatures().put("id", dataRow[2]);
			    if (dataRow.length >= 7)
			    	doc.getFeatures().put("brcid", dataRow[6]);
			    
			}else{
				docSql = "select TextContent, DateModified, BrcId " + 
						" from " + DBUtil.escapeString(Configurator.getConfig("FixedDocTable")) + 
						" where CN_Doc_ID='" + cnDocId + "'";
				String[] docData = DBHelper.getStringArray(docSql, 3);
				docContent = docData[0];
				params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, processBeforeNLP(docContent));
				doc = (Document)Factory.createResource("gate.corpora.DocumentImpl",
		                params, Factory.newFeatureMap(), docId.toString());	
				
				try {
					SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					Date date = df.parse(docData[1]);
					long epoch = date.getTime();
					doc.getFeatures().put("publicationDate", epoch);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				doc.getFeatures().put("id", cnDocId);
				doc.getFeatures().put("brcid", docData[2]);
			}
			

			_logger.info(String.format("====retrieving sql====>%s", docSql));
			

		    DocumentData docData = new DocumentData(
		            doc, docId);
		    _logger.info("Doc(" + docId + ") read, length:" + docContent.length());
		    return docData;

		} catch (DBExecutionException e) {
			_logger.error(e.getMessage() + "\n" + sql);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	static String processBeforeNLP(String text){
		return text.replaceAll("\\s{2,}", " ");
	}

	@Override
	public void init() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

}
