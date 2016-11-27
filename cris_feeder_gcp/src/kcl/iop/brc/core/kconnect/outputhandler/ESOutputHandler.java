package kcl.iop.brc.core.kconnect.outputhandler;

import java.io.File;
import java.io.IOException;
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
	public List<AnnotationSetDefinition> getAnnSetDefinitions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init() throws IOException, GateException {
		_instance = ESClientWorker.getInstance();
		_esStoreUrl = Configurator.getConfig("es_annotation_storage_url");
		anns = JSONUtils.fromJSON( Configurator.getConfig("annotationOutputSettings"), 
				OutputSetting[].class );
		
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
			boolean saved = _instance.saveESDoc(_esStoreUrl, JSONUtils.toJSON(od), seqId);
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
