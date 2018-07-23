package kcl.iop.brc.core.kconnect.crisfeeder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import gate.Document;
import gate.Factory;
import gate.FeatureMap;
import gate.cloud.batch.DocumentID;
import gate.cloud.io.DocumentData;
import gate.cloud.io.InputHandler;
import gate.util.GateException;

public class PlainTextInputHandler implements InputHandler {
	static Logger _logger = Logger.getLogger(PlainTextInputHandler.class);
	String _filePath = "./";
	@Override
	public void close() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

	@Override
	public void config(Map<String, String> params) throws IOException,
			GateException {
		if (params.containsKey("data_path")){
			_filePath = params.get("data_path");
		}

	}

	@Override
	public DocumentData getInputDocument(DocumentID docId) throws IOException,
			GateException {
		File f = new File(_filePath + File.separator + docId.toString());
		if (f.exists()){
			String content = FileUtils.readFileToString(f);
			FeatureMap params = Factory.newFeatureMap();
		    params.put(Document.DOCUMENT_MARKUP_AWARE_PARAMETER_NAME, Boolean.TRUE);
		    params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content);
		    
		    Document doc = (Document)Factory.createResource("gate.corpora.DocumentImpl",
	                params, Factory.newFeatureMap(), docId.toString());

		    
			DocumentData docData = new DocumentData(
		            doc, docId);
		    _logger.info("Doc(" + docId + ") read, length:" + content.length());
		    return docData;
		}		
		return null;
	}

	@Override
	public void init() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

}
