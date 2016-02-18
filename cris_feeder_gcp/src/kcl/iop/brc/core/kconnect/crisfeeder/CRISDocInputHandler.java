package kcl.iop.brc.core.kconnect.crisfeeder;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import ws.nuist.util.Configurator;
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
			+ DBUtil.escapeString(Configurator.getConfig("DocTableContentCol"))
			+ " from "
			+ DBUtil.escapeString(Configurator.getConfig("DocTableName"))
			+ " where "
			+ DBUtil.escapeString(Configurator.getConfig("DocTableDocIDCol"))
			+ "=";
	@Override
	public void close() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

	@Override
	public void config(Map<String, String> arg0) throws IOException,
			GateException {
		  
	}

	@Override
	public DocumentData getInputDocument(DocumentID docId) throws IOException,
			GateException {
		if (null == docId || docId.getIdText() == null) return null;
		String sql = sqlPrefix + "'" + DBUtil.escapeString(docId.getIdText()) + "'";
		try {
			String content = DBHelper.getScalar(sql);

			FeatureMap params = Factory.newFeatureMap();
		    if(mimeType != null && mimeType.length() > 0) {
		      params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
		    }
		    if(encoding!= null && encoding.length() > 0){
		      params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
		    }
		    params.put(Document.DOCUMENT_MARKUP_AWARE_PARAMETER_NAME, Boolean.TRUE);
		    params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content);

		    DocumentData docData = new DocumentData(
		            (Document)Factory.createResource("gate.corpora.DocumentImpl",
		                params, Factory.newFeatureMap(), docId.toString()), docId);
		    _logger.info("Doc(" + docId + ") read, length:" + content.length());
		    return docData;

		} catch (DBExecutionException e) {
			_logger.error(e.getMessage() + "\n" + sql);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void init() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

}
