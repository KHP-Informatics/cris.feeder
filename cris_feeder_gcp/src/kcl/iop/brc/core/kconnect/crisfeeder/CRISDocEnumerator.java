package kcl.iop.brc.core.kconnect.crisfeeder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ws.nuist.util.Configurator;
import ws.nuist.util.DBCPPool;
import ws.nuist.util.DBHelper;
import ws.nuist.util.DBUtil;
import ws.nuist.util.exception.DBExecutionException;
import gate.cloud.batch.DocumentID;
import gate.cloud.io.DocumentEnumerator;
import gate.util.GateException;

public class CRISDocEnumerator implements DocumentEnumerator {
	static Logger _logger = Logger.getLogger(CRISDocEnumerator.class);
	static long maxSeq = 10000;
	static int cacheSize = 3000, offset = 0;
	static String sqlPrefix = "", sqlAll = "";
	
	static{
		maxSeq = Long.parseLong(Configurator.getConfig("cris_max_seq"));
		cacheSize = Integer.parseInt(Configurator.getConfig("cris_batch_docid_num"));
		offset = Integer.parseInt(Configurator.getConfig("cris_batch_offset"));
		sqlPrefix = "SELECT docid FROM ( " +
			    "SELECT " + DBUtil.escapeString(Configurator.getConfig("DocTableDocIDCol")) + " docid, ROW_NUMBER() OVER (ORDER BY " + 
			    DBUtil.escapeString(Configurator.getConfig("DocTableDocIDCol")) + ") AS RowNum " +
			    "FROM " + DBUtil.escapeString(Configurator.getConfig("DocTableName")) +
			    ") AS MyDerivedTable WHERE MyDerivedTable.RowNum BETWEEN ";
		sqlAll = "SELECT " + DBUtil.escapeString(Configurator.getConfig("DocTableDocIDCol")) + " docid " +
			    "FROM " + DBUtil.escapeString(Configurator.getConfig("DocTableName"));
		_logger.info(sqlAll);
		try {
			DBCPPool.printDriverStats();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static long curSeq = 0;
	private static List<String> cachedDocIds = new LinkedList<String>();
	private static boolean cacheRead = false;
	
	public CRISDocEnumerator(){
		curSeq = offset;
	}
	
	static synchronized void readCacheFromDB(){
		if (cacheRead)
			return;
		String sql = sqlAll;
//				sqlPrefix + curSeq + " AND " + Math.min(maxSeq, curSeq + cacheSize);
		try {
			cachedDocIds.clear();
			_logger.info(String.format("reading cache... [%s]", sql));
			ArrayList<List<String>> results = DBUtil.getSQLQueryResult(sql);
			_logger.info(results.size());
			if (results != null && results.size() > 0){
				for(int i=0;i<results.size();i++){
					cachedDocIds.add(DBUtil.getRSMatrixElemByLocation(results, i, 0));
				}
			}else{
				maxSeq = -1;// no result anymore
			}
			cacheRead = true;
		} catch (Exception e) {
			_logger.error(e.getMessage() + "\n" + sql);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static synchronized String getNextDocId() throws Exception{
		if (cachedDocIds.size() <= 0)
			readCacheFromDB();
		if (cachedDocIds.size() > 0){
			curSeq++;
			return cachedDocIds.remove(0);
		}else{
			maxSeq = -1;
			return null;
		}
	}
	
	@Override
	public boolean hasNext() {
		return curSeq<maxSeq;
	}

	@Override
	public DocumentID next() {
		try {
			String docId = getNextDocId();
			return new DocumentID(docId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void config(Map<String, String> arg0) throws IOException,
			GateException {
		// TODO Auto-generated method stub

	}

	@Override
	public void init() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args){
		CRISDocEnumerator ce = new CRISDocEnumerator();
		ce.next();
	}
}
