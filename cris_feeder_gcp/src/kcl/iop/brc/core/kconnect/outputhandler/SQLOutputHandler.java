package kcl.iop.brc.core.kconnect.outputhandler;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import kcl.iop.brc.core.kconnect.crisfeeder.CRISDocEnumerator;
import kcl.iop.brc.core.kconnect.outputhandler.YodieOutputHandler.OutputData;
import kcl.iop.brc.core.kconnect.outputhandler.YodieOutputHandler.OutputSetting;
import kcl.iop.brc.core.kconnect.utils.JSONUtils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import ws.nuist.util.Configurator;
import ws.nuist.util.DBCPPool;
import ws.nuist.util.DBUtil;
import gate.Annotation;
import gate.AnnotationSet;
import gate.Document;
import gate.FeatureMap;
import gate.cloud.batch.AnnotationSetDefinition;
import gate.cloud.batch.DocumentID;
import gate.cloud.io.OutputHandler;
import gate.util.GateException;

public class SQLOutputHandler implements OutputHandler {
	OutputSetting[] anns;
	static Logger _logger = Logger.getLogger(SQLOutputHandler.class);
	String conceptString = null;
	String dbConnURI = null;
	String outputTable = "kconnect_annotations";
	@Override
	public void close() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

	@Override
	public void config(Map<String, String> settings) throws IOException,
			GateException {
		if (settings.containsKey("concept_filter")){
			File fConcepts = new File(settings.get("concept_filter"));
			if (fConcepts.exists()){
				conceptString = FileUtils.readFileToString(fConcepts);
			}
		}
		
		if (settings.containsKey("dbSettingImportant")){
			try {
				Class.forName(settings.get("db_driver"));
				dbConnURI = settings.get("db_url");
				DBCPPool.setupDriver(dbConnURI, settings.get("user"), settings.get("password"));
				outputTable = settings.get("output_table");
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
	public List<AnnotationSetDefinition> getAnnSetDefinitions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init() throws IOException, GateException {
		anns = 
				JSONUtils.fromJSON( Configurator.getConfig("annotationOutputSettings"), OutputSetting[].class );
	}
	
	void convertAnnotations2SQLs(AnnotationSet annSet, List<String> sqls, String docId, String brcId){
		MessageFormat fmt = new MessageFormat("insert into " + outputTable
				+ "(CN_Doc_ID, start_offset, end_offset, experiencer, inst_uri, string_orig, pref_label, sty, negation, temporality, brcid) values "
				+ "(''{0}'', {1}, {2}, ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'');");
		 Iterator<Annotation> annIt = annSet.iterator();
		 while(annIt.hasNext()){
			 Annotation ann = annIt.next();
			 List<Object> args = new LinkedList<Object>();
			 FeatureMap fm = ann.getFeatures();
			 if (conceptString != null && conceptString.indexOf(fm.get("inst").toString()) < 0){
				 continue;
			 }
			 args.add(docId);
			 args.add(ann.getStartNode().getOffset().toString());
			 args.add(ann.getEndNode().getOffset().toString());
			 args.add(fm.get("Experiencer"));
			 args.add(fm.get("inst"));
			 args.add(DBUtil.escapeString(fm.get("string_orig").toString()));
			 args.add(DBUtil.escapeString(fm.get("PREF").toString()));
			 args.add(DBUtil.escapeString(fm.get("STY").toString()));
			 args.add(fm.get("Negation"));
			 args.add(fm.get("Temporality"));
			 args.add(brcId);
			 sqls.add(fmt.format(args.toArray(new Object[0])));
		 }
	}

	@Override
	public void outputDocument(Document doc, DocumentID did)
			throws IOException, GateException {
		if (anns != null){
			String docId = null, brcId = null;
			if (doc.getFeatures()!=null && doc.getFeatures().get("id")!=null)
				docId = doc.getFeatures().get("id").toString();
			if (doc.getFeatures()!=null && doc.getFeatures().get("brcid")!=null)
				brcId = doc.getFeatures().get("brcid").toString();
			
			List<String> sqls = new LinkedList<String>();
			
			//output based on settings
			for(OutputSetting os : anns){
				AnnotationSet as = doc.getAnnotations(os.getAnnotationSet());
				for (String type : os.getAnnotationType()){
					convertAnnotations2SQLs(as.get(type), sqls, docId, brcId);
				}
			}
			
			try {
				if (dbConnURI == null)
					DBUtil.executeBatchUpdate(sqls.toArray(new String[0]));
				else
					DBUtil.executeBatchUpdateByURI(sqls.toArray(new String[0]), dbConnURI);
				_logger.info(String.format("doc %s indexed with %d annotations", did.toString(), sqls.size()));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				_logger.info(String.format("doc %s indexing failed [%s]", did.toString(), sqls.get(0)));
			}
			
		}
	}

	@Override
	public void setAnnSetDefinitions(List<AnnotationSetDefinition> arg0) {
		// TODO Auto-generated method stub

	}
}
