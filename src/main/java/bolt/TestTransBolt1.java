package bolt;

import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestTransBolt1 extends BaseTransactionalBolt {

	private BatchOutputCollector _outputCollector;
	private TransactionAttempt _tx;
	private int count = 0;
	private TopologyContext _context;
	
	public TestTransBolt1(){
		System.out.println("start TestTransBolt1");
	}
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		// TODO Auto-generated method stub
		this._context = context;
		this._outputCollector = collector;
		System.out.println("1 prepare TestTransBolt1 TransactionId:" + 
				id.getTransactionId() + ", AttemptId: " + id.getAttemptId());
		
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		this._tx = (TransactionAttempt) tuple.getValueByField("tx");
		String content = tuple.getStringByField("count");
		System.out.println("1 TaskId: " + this._context.getThisTaskId() + 
				", TestTransBolt1 TransactionAttempt " + this._tx.getTransactionId()
				+ " attemptid" + this._tx.getAttemptId());
		if(content != null && !content.isEmpty()){
			count++;
		}
	}

	public void finishBatch() {
		// TODO Auto-generated method stub
		System.out.println("1 TaskId: " + this._context.getThisTaskId() + ", finishBatch count: "
				+ count);
		this._outputCollector.emit(new Values(_tx, count));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tx", "count"));
		
	}

}
