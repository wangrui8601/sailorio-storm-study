package spout;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.tuple.Fields;

public class TestTransactionSpout implements ITransactionalSpout<TestMetaData>{

	private Map<Long, String> DATA_BASE = null;
	
	public TestTransactionSpout(){
		DATA_BASE = new HashMap<Long, String>();
		for(long i = 0; i < 50; ++i){
			DATA_BASE.put(i, "TestTransaction: " + i);
		}
		System.out.println("TestTransactionSpout start");
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tx", "count"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public backtype.storm.transactional.ITransactionalSpout.Coordinator<TestMetaData> getCoordinator(
			Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		System.out.println("TestTransaction getCoordinator start");
		return new TestCoordinator();
	}

	public backtype.storm.transactional.ITransactionalSpout.Emitter<TestMetaData> getEmitter(
			Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		System.out.println("TestTransaction getEmitter start");
		return new TestEmitter(DATA_BASE);
	}

}
