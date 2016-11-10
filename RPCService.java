package rpc;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * @author saivenkatesh.k
 *
 */
public class RPCService {
	
	private static final Logger logger = Logger.getLogger(rabbitmq.RMQRPCServer.class.getName());
	
	/**
	 * @param queuename 	Queue name to wait for requests
	 * @param numInstances 	Number of workers waiting for messages from this queue.
	 */
	public RPCService(String queuename, int numInstances){
		ExecutorService spoolexec = Executors.newFixedThreadPool(numInstances);
		String language = "python";
		String basedir = "/home/saivenkatesh.k/pkl/";
		
		String[] models = { basedir + "mlp_Maxout_10pieces_epoch_390.pkl",
		                	basedir + "mlp_maxcol_1.93_80.pkl",
		                	basedir + "mlp_minzero_epoch_750.pkl",
		                	basedir + "mlp_RLU_hid1_100_hid2_100_hid3_0_epoch_140_reducedvar.pkl",
		                	basedir + "mlp_RLU_colnorm1.9_hid1_100_hid2_100_hid3_0_epoch_225_newdata_san_800var.pkl"};
		
		String[] indices = {"index_10pieces_390epochs",
		                	"index_maxout_3pieces_80epochs",
		                	"index_minzero3_750epochs",
		                	"index_rlu_140epochs",
		                	"index_rlu_225epochs"};
		
		String initscript = "/home/saivenkatesh.k/Workspace/RPC/src/scoring/scoring4.py";
		
		try {
				FileHandler fh = new FileHandler("logs/rabbitmq_server.log.txt",true);
				logger.addHandler(fh);
				SimpleFormatter formatter = new SimpleFormatter();
				fh.setFormatter(formatter);
				
				
				//Here parameters are passed to each thread which acts as service.
				for(int i=0 ;i < numInstances; i++){
				    HashMap<String, Object> parammap = new HashMap<String, Object>();
				    	
				    //Hard coded here to get keys.
				    //global variables in initscript
				    parammap.put("modelloc", models[i]);
				    parammap.put("indexloc", indices[i]);
				    				    	
				    RPCServiceWorker curthread = new RPCServiceWorker(queuename,
				    													parammap,
				    													initscript,
				    													language);
				    spoolexec.execute(curthread);
				}
			} catch (Exception e1) {
					logger.log(Level.SEVERE, "Error in Starting Services", e1);
					spoolexec.shutdown();
			}
	}
	
	public static void main(String[] args) {
		new RPCService("service" , 5);
	}
}
