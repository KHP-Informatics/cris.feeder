package kcl.iop.brc.core.kconnect.outputhandler;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import kcl.iop.brc.core.kconnect.utils.JSONUtils;
import ws.nuist.util.Configurator;
import gate.Annotation;
import gate.AnnotationSet;
import gate.Document;
import gate.cloud.batch.AnnotationSetDefinition;
import gate.cloud.batch.DocumentID;
import gate.cloud.io.OutputHandler;
import gate.util.GateException;

public class YodieOutputHandler implements OutputHandler{
	OutputSetting[] anns;
	String outputFilePrefix;
	List<AnnotationSetDefinition> annotationDefs = null;

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
		return annotationDefs;
	}

	@Override
	public void init() throws IOException, GateException {
		anns = 
				JSONUtils.fromJSON( Configurator.getConfig("annotationOutputSettings"), OutputSetting[].class );
		outputFilePrefix = Configurator.getConfig("outputFilePrefix");
	}

	@Override
	public void outputDocument(Document doc, DocumentID docId)
			throws IOException, GateException {
		if (anns != null){
			List<AnnotationSet> outputAnns = new LinkedList<AnnotationSet>();
			for(OutputSetting os : anns){
				AnnotationSet as = doc.getAnnotations(os.getAnnotationSet());
				for (String type : os.getAnnotationType()){
					outputAnns.add(as.get(type));
//					Iterator<Annotation> ait = as.get(type).iterator();
//					while(ait.hasNext()){
//						Annotation an = ait.next();
//						for(Object key : an.getFeatures().keySet()){
//							an.getFeatures().get(key);
//						}
//					}
				}
			}
			OutputData od = new OutputData();
			if (doc.getFeatures()!=null && doc.getFeatures().get("id")!=null)
					od.setDocId(doc.getFeatures().get("id").toString());
			od.setAnnotations(outputAnns);
			String s = JSONUtils.toJSON(od) + "\n";
			
			File f = new File(outputFilePrefix + Thread.currentThread().getId());
			FileUtils.writeStringToFile(f, s, true);
		}
	}

	@Override
	public void setAnnSetDefinitions(List<AnnotationSetDefinition> arg0) {
		this.annotationDefs = arg0;
	}

	static class OutputSetting{
		String annotationSet;
		String[] annotationType;
		public String getAnnotationSet() {
			return annotationSet;
		}
		public void setAnnotationSet(String annotationSet) {
			this.annotationSet = annotationSet;
		}
		public String[] getAnnotationType() {
			return annotationType;
		}
		public void setAnnotationType(String[] annotatinoType) {
			this.annotationType = annotatinoType;
		}
		
	}
	
	static class OutputData{
		String docId;
		List<AnnotationSet> annotations;
		public String getDocId() {
			return docId;
		}
		public void setDocId(String docId) {
			this.docId = docId;
		}
		public List<AnnotationSet> getAnnotations() {
			return annotations;
		}
		public void setAnnotations(List<AnnotationSet> annotations) {
			this.annotations = annotations;
		}
		
	}
}
