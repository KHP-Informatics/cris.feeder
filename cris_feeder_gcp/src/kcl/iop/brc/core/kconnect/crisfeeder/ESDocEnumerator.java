package kcl.iop.brc.core.kconnect.crisfeeder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import ws.nuist.util.Configurator;
import gate.cloud.batch.DocumentID;
import gate.cloud.io.DocumentEnumerator;
import gate.util.GateException;

public class ESDocEnumerator implements DocumentEnumerator{
	private int cache_size = 50000;
	private ESClientWorker _esWorker;
	private int _totalDocNum = -1;
	private int _curIndex = 0;
	private int _curOffset = 0;
	private List<String> _cachedDocIds = null;
	
	@Override
	public boolean hasNext() {
		return _curIndex < _totalDocNum;
	}

	@Override
	public DocumentID next() {
		if (!hasNext()){
			return null;
		}
		return getNextDocId();
	}
	
	/**
	 * make it thread safe to get next document ID
	 * @return
	 */
	synchronized DocumentID getNextDocId(){
		if (_cachedDocIds!=null && _curOffset + _cachedDocIds.size() > _curIndex){
			int idx = _curIndex - _curOffset;
			_curIndex++;
			return new DocumentID(_cachedDocIds.get(idx));
		}else{
			int curCachedSize = _cachedDocIds == null ? 0 : _cachedDocIds.size();
			try {
				_cachedDocIds = _esWorker.getESDocIDs(Configurator.getConfig("es_doc_url"), 
						_curOffset + curCachedSize, cache_size);
				_curOffset += curCachedSize;
				int idx = _curIndex - _curOffset;
				_curIndex++;
				return new DocumentID(_cachedDocIds.get(idx));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		_esWorker = ESClientWorker.getInstance();
		try {
			_totalDocNum = _esWorker.getESDocNum(Configurator.getConfig("es_doc_url"));
			System.out.println(_totalDocNum);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
