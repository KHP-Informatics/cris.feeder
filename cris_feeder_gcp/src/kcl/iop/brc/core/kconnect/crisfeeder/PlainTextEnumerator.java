package kcl.iop.brc.core.kconnect.crisfeeder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import gate.cloud.batch.DocumentID;
import gate.cloud.io.DocumentEnumerator;
import gate.util.GateException;

public class PlainTextEnumerator implements DocumentEnumerator {
	private List<String> _docIds = null;
	private int _curIndex = 0;
	@Override
	public boolean hasNext() {
		if (_docIds != null && _docIds.size() > _curIndex)
			return true;
		else
			return false;
	}

	@Override
	public DocumentID next() {
		if (hasNext()){
			return new DocumentID(_docIds.get(_curIndex++));
		}
		return null;
	}

	@Override
	public void config(Map<String, String> params) throws IOException,
			GateException {
		if (params.containsKey("doc_id_file")){
			_docIds = FileUtils.readLines(new File(params.get("doc_id_file")));
		}
		if (params.containsKey("store_doc_start_id")){
			ESClientWorker.getInstance().setESDocSeqId(Integer.parseInt(params.get("store_doc_start_id")));
		}

	}

	@Override
	public void init() throws IOException, GateException {
		// TODO Auto-generated method stub

	}

}
