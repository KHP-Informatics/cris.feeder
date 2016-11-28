package kcl.iop.brc.core.kconnect.crisfeeder;

import java.io.IOException;
import java.util.Map;

import kcl.iop.brc.core.kconnect.utils.JSONUtils;

import org.apache.log4j.Logger;

import ws.nuist.util.Configurator;
import gate.Document;
import gate.Factory;
import gate.FeatureMap;
import gate.cloud.batch.DocumentID;
import gate.cloud.io.DocumentData;
import gate.cloud.io.InputHandler;
import gate.util.GateException;

public class ESDocInputHandler implements InputHandler {
	static Logger _logger = Logger.getLogger(ESDocInputHandler.class);
	private ESClientWorker _instance;
	private String _textFiledName, _docGUID, _mimeType, _encoding, _docCreatedDateField;
	@Override
	public void close() throws IOException, GateException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void config(Map<String, String> arg0) throws IOException,
			GateException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DocumentData getInputDocument(DocumentID docId) throws IOException,
			GateException {

		
		try {
			Map data = _instance.getESDocumentDetail(Configurator.getConfig("es_doc_url"), docId.getIdText());
			if (data != null && data.containsKey(_textFiledName) && data.containsKey(_docGUID)){
				FeatureMap params = Factory.newFeatureMap();
			    if(_mimeType != null && _mimeType.length() > 0) {
			      params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, _mimeType);
			    }
			    if(_encoding!= null && _encoding.length() > 0){
			      params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, _encoding);
			    }
			    params.put(Document.DOCUMENT_MARKUP_AWARE_PARAMETER_NAME, Boolean.TRUE);
			    params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, data.get(_textFiledName).toString());
			    
			    Document doc = (Document)Factory.createResource("gate.corpora.DocumentImpl",
			            params, Factory.newFeatureMap(), docId.toString());
			    doc.getFeatures().put("esDocDetail", JSONUtils.toJSON(data));
			    if (data.containsKey(_docCreatedDateField))
			    	doc.getFeatures().put("publicationDate", data.get(_docCreatedDateField).toString());
			    doc.getFeatures().put("id", data.get(_docGUID).toString());
			    _logger.info(
			    		String.format("doc [%s] read with length [%d]", 
			    				docId.getIdText(), data.get(_textFiledName).toString().length()));
			    return new DocumentData(
			            doc, docId);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void init() throws IOException, GateException {
		_instance = ESClientWorker.getInstance();
		_textFiledName = Configurator.getConfig("main_text_field");
		_docGUID = Configurator.getConfig("doc_guid_field");
		_docCreatedDateField = Configurator.getConfig("doc_created_date_field");
		_mimeType = Configurator.getConfig("mimieType");
		_encoding = Configurator.getConfig("encoding");
	}

}
