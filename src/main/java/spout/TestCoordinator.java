package spout;

import java.math.BigInteger;

import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.utils.Utils;

public class TestCoordinator implements ITransactionalSpout.Coordinator<TestMetaData>{

	public TestCoordinator(){
		System.out.println("TestCoordinator start");
	}
	
	public TestMetaData initializeTransaction(BigInteger txid,
			TestMetaData prevMetadata) {
		// TODO Auto-generated method stub
		long index = 0L;
		if(null == prevMetadata){
			index = 0L;
		} else{
			index = prevMetadata.get_index() + prevMetadata.get_size();
		}
		TestMetaData metadata = new TestMetaData();
		metadata.set_index(index);
		metadata.set_size(10);
		System.out.println("开始事务: " + metadata.toString());
		return metadata;
	}

	public boolean isReady() {
		// TODO Auto-generated method stub
		Utils.sleep(1000);
		return true;
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

}
