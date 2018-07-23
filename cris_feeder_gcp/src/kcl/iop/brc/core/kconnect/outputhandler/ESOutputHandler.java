package kcl.iop.brc.core.kconnect.outputhandler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import kcl.iop.brc.core.kconnect.crisfeeder.ESClientWorker;
import kcl.iop.brc.core.kconnect.outputhandler.YodieOutputHandler.OutputData;
import kcl.iop.brc.core.kconnect.outputhandler.YodieOutputHandler.OutputSetting;
import kcl.iop.brc.core.kconnect.utils.JSONUtils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import ws.nuist.util.Configurator;
import gate.AnnotationSet;
import gate.Document;
import gate.cloud.batch.AnnotationSetDefinition;
import gate.cloud.batch.DocumentID;
import gate.cloud.io.OutputHandler;
import gate.util.GateException;

public class ESOutputHandler implements OutputHandler {
	static Logger _logger = Logger.getLogger(ESOutputHandler.class);
	private ESClientWorker _instance;
	String _esStoreUrl;
	List<AnnotationSetDefinition> annotationDefs = null;
	OutputSetting[] anns;
	String[] outputAttributes;
	@Override
	public void close() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

	@Override
	public void config(Map<String, String> params) throws IOException,
			GateException {
		if (params.containsKey("output_field_names")){
			outputAttributes = params.get("output_field_names").split(",");
		}
		_esStoreUrl = params.get("es_annotation_storage_url");
	}

	@Override
	public List<AnnotationSetDefinition> getAnnSetDefinitions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init() throws IOException, GateException {
		_instance = ESClientWorker.getInstance();
//		_esStoreUrl = Configurator.getConfig("es_annotation_storage_url");
		anns = JSONUtils.fromJSON( Configurator.getConfig("annotationOutputSettings"), 
				OutputSetting[].class );
//		String outputDocAttrs = Configurator.getConfig("output_field_names");
//		if (null != outputDocAttrs){
//			outputAttributes = outputDocAttrs.split(",");
//		}
		
	}

	@Override
	public void outputDocument(Document doc, DocumentID docId)
			throws IOException, GateException {
		if (anns != null){
			List<AnnotationSet> outputAnns = new LinkedList<AnnotationSet>();
			
			//output based on settings
			for(OutputSetting os : anns){
				AnnotationSet as = doc.getAnnotations(os.getAnnotationSet());
				for (String type : os.getAnnotationType()){
					outputAnns.add(as.get(type));
				}
			}
			
			Map origDoc = (Map)JSONUtils.fromJSON(doc.getFeatures().get("esDocDetail").toString());
			Map resultDoc = new LinkedHashMap<String, Object>();
			if (outputAttributes != null && outputAttributes.length > 0){
				for(String attr : outputAttributes){
					resultDoc.put(attr,origDoc. get(attr).toString());
				}
			}
			OutputData od = new OutputData();
			if (doc.getFeatures()!=null && doc.getFeatures().get("id")!=null)
					od.setDocId(doc.getFeatures().get("id").toString());
			else
				od.setDocId(doc.getName());
			od.setAnnotations(outputAnns);
			int seqId = _instance.nextESDocSeqId();
			if (seqId % 1000 == 0){
				_logger.info(String.format("%s docs indexed", seqId));
			}
			
			//pull out annotations to the upper level
			resultDoc.put("yodie_ann", outputAnns.get(0));
			boolean saved = _instance.saveESDoc(_esStoreUrl, JSONUtils.toJSON(resultDoc), seqId);
			if (!saved){
				_logger.error(String.format("annotations of doc %s not saved [seqid: %s]", docId.getIdText(), seqId));
			}
		}
	}

	@Override
	public void setAnnSetDefinitions(List<AnnotationSetDefinition> arg0) {
		this.annotationDefs = arg0;
	}

}
