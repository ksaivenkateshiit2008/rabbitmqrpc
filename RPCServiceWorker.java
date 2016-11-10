package rpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jep.Jep;
import jep.JepException;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * @author saivenkatesh.k
 *
 */
public class RPCServiceWorker implements Runnable {
	
	private static final Logger logger = Logger.getLogger(rabbitmq.RMQRPCServer.class.getName());
	private String queuename;
	private RMQWrapper rabbitmqobj;
	private Jep jepobj;
	private HashMap<String, Object> jepinitdict;
	private String pyscriptloc;
	private String language;

	/**
	 * @throws IOException if unable to send or receive messages from RabbitMQ queues.
	 * @throws InterruptedException if unable to get msg from queue
	 * @throws ClassNotFoundException if error in deserialize message body
	 */
	public void waitForReq() throws IOException, ClassNotFoundException, InterruptedException{
		
		System.out.println(" [x] Awaiting RPC requests");
		logger.log(Level.INFO, "Waiting for RPC requests");
		
	    while (true)
	    {
	    	//Waits till it gets a request.
	    	QueueingConsumer.Delivery delivery = rabbitmqobj.getResponse(null);
	    	
	    	//Process message
	    	Object message = null;
			message = MsgSerialize.deserialize((delivery.getBody()));
			
			//Message parameters received along with body
			BasicProperties prop = delivery.getProperties();
			Map<String, Object> headermap = prop.getHeaders();
			String replyqueuename = prop.getReplyTo();
			String corrid = prop.getCorrelationId();
			long deliverytag = delivery.getEnvelope().getDeliveryTag();
			
			logger.log(Level.INFO, "ThreadID: "+Thread.currentThread().getId()+" Received:"+message);
			
			//response is sent even if exception is raised during execution
			//Using this resultheadermap, it identifies whether exception occurred or not.
			Map<String, Object> resultheadermap = new HashMap<String, Object>();
			
			Object response = null;
			try{
				//service name is passed via message headers
				//language level execution
				if(language.equals("java")){
					response = execJavaFunc(headermap.get("service").toString(), message);			
				}
				else if(language.equals("python")){
						response = execPyFunc(headermap.get("service").toString(), message);
				}
				
			    resultheadermap.put("exception_raised", false);
			    logger.log(Level.INFO, "ThreadID: "+Thread.currentThread().getId() +" Sending:"+response);
				
			} catch(Exception | JepException cause){
				
				//If any exception raised during execution of service, then exception
				//message is sent as response
				StackTraceElement elements[] = cause.getStackTrace();
				logger.log(Level.SEVERE, "Exception thrown in executing service function", cause);
				for (int i = 0, n = elements.length; i < n; i++) {
					response = (String) response + elements[i].getFileName()
				            + ":" + elements[i].getLineNumber() 
				            + ">> "
				            + elements[i].getMethodName() + "()\n";
			    }
			    resultheadermap.put("exception_raised", true);
			}
			
			//Sending response
			rabbitmqobj.sendRequest(response, resultheadermap, null, corrid, deliverytag, replyqueuename, null, null);
		}
	}
		
	/**
	 * Execute Python function received from RPCClient
	 * TODO Currently casting to strings to avoid sending as JObject
	 * 
	 * @param servicename service name which is used to decide python function 
	 * 					  to execute
	 * @param message	  Body of message which is used as arguments to function
	 * 
	 * @return output of function
	 * @throws JepException
	 */
	public Object execPyFunc(String servicename, Object message) throws JepException {
		return 4*6/0;
		//return jepobj.invoke(servicename, (String) message);
	}

	/**
	 * Execute Java Function received from RPCClient
	 * 
	 * @param servicename service name which is used to decide java function 
	 * 					  to execute
	 * @param message	  Body of message which is used as arguments to function
	 * @return output of the function
	 */
	public Object execJavaFunc(String servicename, Object message) {
		return null;
	}

	/**
	 * @param path Python path to be used for imports
	 * @throws JepException when it is unable to create Jep Object or unable
	 * 						to run script.
	 */
	public void preprocess(String path) throws JepException{
		//Adds path to jep object
		jepobj = new Jep(false, path);
		if(jepinitdict != null){
			
			//Set all parameters
			Iterator<String> paramitr = jepinitdict.keySet().iterator();
			while(paramitr.hasNext()){
				String key = paramitr.next();
				Object value = jepinitdict.get(key);
				jepobj.set(key, (String) value);
			}
			
			//Run script
			jepobj.runScript(pyscriptloc);
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			//Adds path to python execution for import in scripts
			preprocess(pyscriptloc.substring(0,pyscriptloc.lastIndexOf('/')));
		} catch (JepException e1) {
			logger.log(Level.SEVERE, "Error in Creation of Jep Object. Cannot execute Python codes", e1);
		}
		
		try {
			rabbitmqobj = new RMQWrapper();
			rabbitmqobj.setConsumer(queuename, false);
			
			waitForReq();
			rabbitmqobj.close();
		} catch (ShutdownSignalException
				| ConsumerCancelledException | ClassNotFoundException
				| IOException | InterruptedException e) {
			logger.log(Level.SEVERE, "Exception in RabbitMQ", e);
		}
	}
	
	/**
	 * @param queuename 	RabbitMQ queue to wait for messages
	 * @param jepinitdict	Key Value pair to set as global variables in
	 * 						pyscriptloc
	 * @param pyscriptloc	Script to be executed when starting worker before
	 * 						receiving messages as it is one time.
	 * @param language		python or java function to be executed for requests
	 */
	public RPCServiceWorker(String queuename, HashMap<String, Object> jepinitdict, String pyscriptloc, String language){
		this.queuename = queuename;
		this.jepinitdict = jepinitdict;
		this.pyscriptloc = pyscriptloc;
		this.language = language;
	}
}
