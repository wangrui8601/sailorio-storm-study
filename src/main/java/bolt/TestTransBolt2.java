package bolt;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;

public class TestTransBolt2 extends BaseTransactionalBolt
	implements ICommitter{

	private int sum = 0;
	private TransactionAttempt _tx;
	private static int _result = 0;
	private static BigInteger _curtxid = null;	
	
	public TestTransBolt2(){
		System.out.println("TestTransBolt2 start£¡");
	}
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		// TODO Auto-generated method stub
		this._tx = id;
		System.out.println("TestTransBolt2 prepare TransactionId: " + id);
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		this._tx = (TransactionAttempt)tuple.getValueByField("tx");
		sum += tuple.getIntegerByField("count");
		System.out.println("TestTransBolt2 execute TransactionAttempt: " + this._tx);
	}

	public void finishBatch() {
		// TODO Auto-generated method stub
		System.out.println("finishBatch _curtxid: " + _curtxid
				+ ", getTransactionId: " + this._tx.getTransactionId());
		if(_curtxid == null ||  !_curtxid.equals(_tx.getTransactionId())){
			System.out.println("***** 1 _curtxid: " + _curtxid + 
					", +tx.getTransactionId(): " + _tx.getTransactionId());
			if(null == _curtxid){
				_result = sum;
			} else{
				_result += sum;
			}
			
			_curtxid = _tx.getTransactionId();
			System.out.println("***** 2 _curtxid: " + _curtxid
					+ ", _tx.getTransactionId(): " + _tx.getTransactionId());
		}
		
		System.out.println("total==============: " + _result);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	

}
