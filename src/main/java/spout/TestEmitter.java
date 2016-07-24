package spout;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

public class TestEmitter implements ITransactionalSpout.Emitter<TestMetaData>{

	private Map<Long, String> _dbMap = null;
	
	public TestEmitter(Map<Long, String> dbMap){
		System.out.println("start TestEmitter");
		this._dbMap = dbMap;
	}
	
	public void emitBatch(TransactionAttempt tx, TestMetaData coordinatorMeta,
			BatchOutputCollector collector) {
		// TODO Auto-generated method stub
		long index = coordinatorMeta.get_index();
		long size = index + coordinatorMeta.get_size();
		System.out.println("TestEmitter emitBatch size: " + size +
				", _dbMap size: " + _dbMap.size());
		if(size > _dbMap.size())
			return;
		for(; index < size; index++){
			if(null == _dbMap.get(index)){
				System.out.println("TestEmitter continue");
				continue;
			}
			System.out.println("TestEmitter emitBatch index: " + index);
			collector.emit(new Values(tx, _dbMap.get(index)));
		}
	}

	public void cleanupBefore(BigInteger txid) {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}
	

}
