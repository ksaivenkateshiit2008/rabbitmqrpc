package rpc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

import rpc.RequestStatus;

/**
 * @author saivenkatesh.k
 *
 */
public class RPCClient {
	
	private static final Logger logger = Logger.getLogger(RPCClient.class.getName());
	private static final int MAXTHREADS = 10;
	private static RPCClient myobj;
	
	private Map<Integer, RPCRequest> requestobjmap = new HashMap<Integer, RPCRequest>();
	private Map<Integer, Future<?>> tpoolfuturelist = new HashMap<Integer, Future<?>>();
	
	private Callback finalcb = null;
	private ExecutorService tpoolexecutor;
	private RMQWrapper rabbitmqobj;
	
	private String exchange;
	private String routingkey;
	private String replyrpcqueue;
	private Long PollInterval;
	
	private RPCClient(){
		this.tpoolexecutor = Executors.newFixedThreadPool(MAXTHREADS);
		try {
			this.rabbitmqobj = new RMQWrapper();
			logger.log(Level.INFO, "Created new RabbitMQ Connection");
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error in connecting RabbitMQ server", e);
			System.exit(1);
		}
	}

	/**
	 * @param exchange        RabbitMQ exchange name used for pushing messages
	 * @param routingkey      routing key for exchange which selects queue 
	 * 						  where services consume
	 * @param replyrpcqueue   Queue name to wait for messages by RPCClient
	 * @param PollInterval    Interval where this thread waits for messages in 
	 * 						  queue.
	 */
	public void setParams(String exchange, String routingkey, 
			String replyrpcqueue, long PollInterval){
		this.exchange = exchange;
		this.routingkey = routingkey;
		this.replyrpcqueue = replyrpcqueue;
		this.PollInterval = PollInterval;
						
		FileHandler fh;
		try {
			fh = new FileHandler("logs/rabbitmq_client.log.txt", true);
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
		} catch (SecurityException | IOException e) {
			e.printStackTrace();
			System.out.println("Cannot write to log file logs/rabbitmq_client.log.txt");
		}
		
		try {
			this.rabbitmqobj.setConsumer(replyrpcqueue, true);
		} catch (IOException e1) {
			logger.log(Level.SEVERE, "Error in connecting RabbitMQ server", e1);
			System.exit(1);
		}
	}
	
	/**
	 * Making RPCClient singleton. User has to call this method for using 
	 * RPCClient
	 * 
	 * @param maxthreads      maximum number of threads allowed in threadpool
	 * @param exchange        RabbitMQ exchange name used for pushing messages
	 * @param routingkey      routing key for exchange which selects queue 
	 * 						  where services consume
	 * @param replyrpcqueue   Queue name to wait for messages by RPCClient
	 * @param PollInterval    Interval where this thread waits for messages in 
	 * 						  queue.
	 * 
	 * @return RPCClient Object which is newly created or already created before
	 */
	public static RPCClient getinstance(String exchange, String routingkey, 
			         String replyrpcqueue, long PollInterval){
		
		if(myobj == null){
			logger.log(Level.INFO, "Creating new RPCClient object");
			myobj = new RPCClient();
			myobj.setParams(exchange, routingkey, replyrpcqueue, PollInterval);
		}
		else{
			logger.log(Level.INFO, "Reusing existing RPCClient object");
		}
		return myobj;
	}
	
	/**
	 * Used when sending Asynchronous RPC requests.
	 * Create Request object from parameters and add this object to RPCClient 
	 * object for later use.
	 * 
	 * @param servicename Name of service to send request
	 * @param cb          Service object implementing Callback Interface to be 
	 * 					  executed after response is obtained.
	 * @param timeout     Maximum Time allowed to wait for response for this 
	 * 					  request. Long type in order of milliseconds has to 
	 * 					  be given
	 * @param maxattempts Maximum number of retries allowed for this request 
	 * 					  in case time is available
	 * @param message     message which is passed to service.
	 */
	public void add(String servicename, Callback cb, Long timeout, 
								int maxattempts, Object message){
		
		RPCRequest requestobj = new RPCRequest(servicename, cb, timeout, maxattempts, message);
		logger.log(Level.INFO, "New Request"+requestobj+"Added");
		requestobjmap.put(requestobj.hashCode(), requestobj);
	}

	
	/**
	 * 	Adds final callback Object to RPCClient which is executed after 
	 * 	callback of all requests are executed.
	 * 
	 * @param cb Any Object implementing Callback Interface
	 */
	public void addFinalCallback(Callback cb){
		this.finalcb = cb;
		logger.log(Level.INFO, "Final Callback Added");
	}

	
	/**
	 *
	 *	Final method to be called after all requests and callbacks are added 
	 *	during Asynchronous RPC requests. This method actually sends requests 
	 *	to RMQ server and monitors and process requests.
	 * 
	 * @return null if final Callback is not given.
	 *         Output of final Callback onSuccess or onFailure methods
	 */
	public Object send(){
		
		//Sending all the Requests
		sendAllReqAsync();

		//monitoring Requests
		monitorReq();
		
		//Final callback
		Object result = null;
		if(this.finalcb != null){
			result = execFinalCB();
		}
		
		requestobjmap.clear();
		tpoolfuturelist.clear();
		
		return result;
	}
	
	/**
	 * Method to be called for synchronous RPC requests execution. It will send 
	 * request via exchange and wait for response to be obtained independent of 
	 * other requests.
	 * 
	 * @param servicename   Service Name to send the request.
	 * @param timeout       Maximum Time allowed to wait for response for this 
	 * 						request. Long type in order of milliseconds has to 
	 * 						be given
	 * @param maxattempts	Maximum number of retries allowed for this request 
	 * 						in case time is available
	 * @param message		message which is passed to service.
	 * @return Object[]{status, response} If status is error, response is null
	 *                                              else, response is returned.       
	 */
	public Object[] call(String servicename, Long timeout, int maxattempts, Object message){
		
		//Adding service name to header
		Map<String, Object> headermap = new HashMap<String, Object>();
	    headermap.put("service", servicename);
	    
	    
	    int attmpts = 0;
	    Long timeelapsed = (long) 0;
	    while(attmpts < maxattempts){
	    	Delivery delivery  = null;
	    	try {
	    		
	    		//Sending and waiting for response
	    		rabbitmqobj.sendRequest(message, headermap, replyrpcqueue, null, Long.MAX_VALUE, null, exchange, routingkey);
	    		
	    		//Wait till next response is obtained
	    		while(delivery == null && timeelapsed < timeout-PollInterval){
	    			delivery = rabbitmqobj.getResponse(PollInterval);
	    			timeelapsed += PollInterval;
	    		}
	    		
	    		//Waited for response till timeout, can't wait further. So returning with timeout
	    		if(timeelapsed >= timeout-PollInterval){
	    			logger.log(Level.WARNING, "Request Timedout");
	    	    	return new Object[]{RequestStatus.TIMEOUT, null};
	    		}
	    		
	    		else if(delivery != null){
	    			Object resultmsg = null;
	    		    try {
	    				resultmsg = MsgSerialize.deserialize(delivery.getBody());
	    			} catch (ClassNotFoundException | IOException e) {
	    				logger.log(Level.SEVERE, "Error in message conversion", e);
	    			}
	    		    
					Boolean exception_raised = (Boolean) delivery.getProperties().getHeaders().get("exception_raised");
					
					//Exception raised in server. so retrying
					if(exception_raised){
						logger.log(Level.WARNING, "Exception raised in Service\n"+resultmsg);
					}
					
					//Got correct Response so exiting with result
					else{
						return new Object[]{RequestStatus.DONE, resultmsg};
					}

	    		}

	    	} catch (IOException | NumberFormatException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
	    		logger.log(Level.WARNING, "Error while Sending/Receiving message from RabbitMQ", e);
	    	}
	    	
	    	attmpts++;
	    }
	    return new Object[]{RequestStatus.SERVICEEXCEPTION, null};
	}
	
	
	/**
	 * Sends the request to RabbitMQ exchange
	 * 
	 * @param requestobj Object where request parameters are stored. Here used
	 * 					 to get service name, message
	 * @param requestid	 Used as correlationID to identify requests when 
	 * 					 response is obtained.
	 * 
	 * @throws UnsupportedEncodingException Thrown in case of errors in sending
	 * 									    RabbitMQ messages.
	 * @throws IOException Thrown in case of errors in sending RabbitMQ messages.
	 */
	public void sendSingleReq(RPCRequest requestobj, Integer requestid) throws UnsupportedEncodingException, IOException{
		
		//Send service name in Header for identifying app. function name to run
		Map<String, Object> headermap = new HashMap<String, Object>();
	    headermap.put("service", requestobj.getService());
	    
	    logger.log(Level.INFO, requestobj.getService());
		rabbitmqobj.sendRequest(requestobj.getMsg(), 
								headermap, 
								replyrpcqueue, 
								requestid.toString(),
								Long.MAX_VALUE,
								null,
								exchange, routingkey);
	}
	
	/**
	 * Iterates over all requests and calls sendSingleReq
	 * 
	 */
	public void sendAllReqAsync(){
		Iterator<Integer> reqitr = requestobjmap.keySet().iterator();
		while(reqitr.hasNext()){
			Integer requestid = reqitr.next();
			try {
				sendSingleReq(requestobjmap.get(requestid), requestid);
			} catch (IOException e) {
				requestobjmap.get(requestid).setStatus(RequestStatus.RABBITMQEXCEPTION);
				logger.log(Level.SEVERE, "Error in Sending Request", e);
			}
		}
	}
	
	/**
	 * Continuously wait for responses from reply queue by waiting for
	 * PollInterval time. If a response is obtained, it identifies request
	 * using request-id, and sets corresponding statuses in case of errors.
	 * If response is not obtained, it will reduce timeouts for all active 
	 * requests.
	 * 
	 */
	public void monitorReq(){
		
		//Continuously check for completion before waiting for response in reply to queue.
		while(!allReqDone()){
			
			Delivery delivery = null;
			
			try {
				//Waits till PollInterval time for next message.
				delivery = rabbitmqobj.getResponse(PollInterval);
			} catch (ShutdownSignalException | ConsumerCancelledException
					| IOException | InterruptedException e1) {
				logger.log(Level.SEVERE, "Error in Waiting for Response from RabbitMQ", e1);
			}
			
			//Timed out i.e message did not appear till PollInterval time.
			if(delivery == null){
				//Reducing Timeout for all active requests by PollInterval
				Iterator<Integer> reqiditr = requestobjmap.keySet().iterator();
				while(reqiditr.hasNext()){
					
					Integer requestid = reqiditr.next();
					RPCRequest curreq = requestobjmap.get(requestid);
					if(	curreq.getCurrentattempt() == curreq.getmaxAttempts() + 1 || 
							curreq.getStatus() == RequestStatus.TIMEOUT ||
							curreq.getStatus() == RequestStatus.DONE){
						continue;
					}
					else if(!curreq.addTimeElapsed(PollInterval)){
						curreq.setStatus(RequestStatus.TIMEOUT);
						logger.log(Level.INFO, "Request timedout" + curreq.toPrettyString());
						
						//Execute Failure callback in worker
						RPCWorker cbhandler = new RPCWorker(curreq.getcallback(), null, RequestStatus.TIMEOUT);
						tpoolfuturelist.put(requestid, this.tpoolexecutor.submit(cbhandler));
					}
				}
			}
			
			//Response obtained
			else{
				
				//Using correlationId to identify the request and gets the Request object.
				Integer responsecorrid = Integer.parseInt(delivery.getProperties().getCorrelationId());
				RPCRequest curreq = requestobjmap.get(responsecorrid);
				
				//First condition fails if response obtained is not present in requests of this iteration
				//Second condition fails if duplicate response is obtained.
				//Perform this only when above conditions are true
				if(curreq != null && curreq.getStatus() != RequestStatus.DONE){

					Object response = "";
						try {
							response = MsgSerialize.deserialize(delivery.getBody());
						} catch (ClassNotFoundException | IOException e1) {
							logger.log(Level.SEVERE, "Unable to process response from delivery", e1);
						}

					Boolean exception_raised = false;
					exception_raised = (Boolean) delivery.getProperties().getHeaders().get("exception_raised");
					
					//Exception raised in service
					if(exception_raised){
						
						logger.log(Level.WARNING, "Exception Raised in service \n"+response);
						curreq.setStatus(RequestStatus.SERVICEEXCEPTION);
						
						//Wait till request is sent again or all attempts exhausted.
						while(true){
							try {
								//Re Sending request
								if(curreq.getCurrentattempt() < curreq.getmaxAttempts()){
									
									//This part only throws exception so wont go to infinite loop
									logger.info("Resending Request"+curreq.toPrettyString());
									sendSingleReq(curreq, responsecorrid);
								}
								else{
									logger.info("All attempts exhausted. So executing Callback");
									//Execute Failure callback in available worker
									RPCWorker cbhandler = new RPCWorker(curreq.getcallback(), null, 
																		RequestStatus.SERVICEEXCEPTION);
									tpoolfuturelist.put(responsecorrid, this.tpoolexecutor.submit(cbhandler));
								}
								curreq.incCurrentAttempts();
								break;
								
							} catch (IOException e) {
								logger.log(Level.SEVERE, "Error in re sending request"+curreq.toPrettyString(), e);
							}
						}
					}
					
					//Response is obtained as expected
					else{
						curreq.setStatus(RequestStatus.DONE);
						
						//Executing callback in available Worker
						RPCWorker cbhandler = new RPCWorker(curreq.getcallback(), response, curreq.getStatus());
						tpoolfuturelist.put(responsecorrid, this.tpoolexecutor.submit(cbhandler));
					}
				}
			}
		}
		
		//Wait till all workers are completed
		Integer numfutures = tpoolfuturelist.size();
		Integer count = 0;
		while(count != numfutures){
			count = 0;
			for(Future<?> future : tpoolfuturelist.values()){
				try {
					if(future.get() == null){
						count += 1;
					}
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
					break;
				}
			}
		}
	}
		
	/**
	 * Close ThreadPool after all Requests are sent using RPCClient object
	 * It opens when a new RPCClient object is created and same pool is used
	 * by any number of iterations.
	 */
	public void closeThreadPool(){
		this.tpoolexecutor.shutdown();
		try {
			this.tpoolexecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, "Error in closing ThreadPool", e);
		}
		
		//closing RabbitMQ connection
		try {
			rabbitmqobj.close();
			logger.log(Level.INFO, "Closing RabbitMQ Connection");
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error in closing RabbitMQ Connection", e);
		}
		
	}
	
	/**
	 * Iterate over all requests status and returns true if any further
	 * sending of requests needed overall and false if all of the attempts are
	 * exhausted.
	 * 
	 * @return true if all attempts are exhausted
	 * 		   false if further attempts for any request(s) is present. 
	 */
	public Boolean allReqDone() {
		Iterator<Integer> reqiditr = requestobjmap.keySet().iterator();
		int count = 0;
		while(reqiditr.hasNext()){
			RPCRequest curreq = requestobjmap.get(reqiditr.next());
			if(	curreq.getCurrentattempt() == curreq.getmaxAttempts() + 1 ||
					curreq.getStatus() == RequestStatus.TIMEOUT ||
					curreq.getStatus() == RequestStatus.DONE){
				count += 1;
			}
		}
		if (count == requestobjmap.size())
			return true;
		return false;
	}
	
	
	/**
	 * If all statuses are done call onSuccess,
	 * else call onFailure
	 * 
	 * @return null when failure method is called.
	 *         Object when success method is called.
	 */
	public Object execFinalCB(){
		
		Iterator<Integer> reqitr = requestobjmap.keySet().iterator();
		int count = 0;
		logger.info("Executing final callback");
		
		//Iterates over all requests to check for done statuses. 
		while(reqitr.hasNext()){
			RPCRequest curobj = requestobjmap.get(reqitr.next());
			if(curobj.getStatus() == RequestStatus.DONE){
				count += 1;
			}
		}
		if(count == requestobjmap.size()){
			return this.finalcb.onSuccess(null);
		}
		else{
			return this.finalcb.onFailure();
		}
	}
	
	public static void main(String[] args) {
		
		//RPCClient rpcclient = new RPCClient(10, "testexchange", "service", "replyrpc", (long) 10);
		RPCClient rpcclient = RPCClient.getinstance("testexchange", "service", "replyrpc", (long) 100);
		
		/*
		 * 
		maxthreads = 10;
		exchange = "testexchange";
		routingkey = "service";
		replyrpcqueue = "replyrpc";
		PollInterval = (long) 10;
		
		
		Callback cb1 = new IntegerSum(1,2,3);
		Callback cb2 = new StringSum("a","bc","def");
		Callback cb3 = new StringSum("abcdefghijklmn","opqrstuvwxyz");
		Callback cb4 = new RepeatString("abcdefghijklmnopqrstuvwxyz", 10);
		Callback cb5 = new IntegerSum(1,2,3);
		Callback cb6 = new StringSum("sai","ven","kat","esh");
		
		
		Date Startdate = new Date();
		//Async RPC
		rpcclient.add("service", cb1, 0, "10000", 3, "stark");
		rpcclient.add("service", cb2, 0, "10000", 3, "lannister");
		rpcclient.add("service", cb3, 0, "10000", 3, "targeriyan");
		rpcclient.add("service", cb4, 0, "10000", 3, "baratheon");
		rpcclient.add("service", cb5, 0, "10000", 3, "tyrell");
		rpcclient.addFinalCallback(cb6);
		rpcclient.send();
		
		Date AsyncReqdonedate = new Date();"10pieces_390epochs.csv"
		
		//Sync RPC
		cb1.onSuccess(rpcclient.call("service","10000", 3, "stark"));
		cb2.onSuccess(rpcclient.call("service","10000", 3, "lannister"));
		cb3.onSuccess(rpcclient.call("service","10000", 3, "targeriyan"));
		cb4.onSuccess(rpcclient.call("service","10000", 3, "bartatheon"));
		cb5.onSuccess(rpcclient.call("service","10000", 3, "tyrell"));
		cb6.onSuccess(null);
		
		Date syncreqdonedate = new Date();		
		System.out.println(AsyncReqdonedate.getTime() - Startdate.getTime());
		System.out.println(syncreqdonedate.getTime() - AsyncReqdonedate.getTime());
		*/
		
		String dirloc = "/home/saivenkatesh.k/pkl/output/";
		try {
			Callback cb1 = new FileWrite(dirloc+"10pieces_390epochs.csv");
			Callback cb2 = new FileWrite(dirloc+"maxout_3pieces_80epochs.csv");
			Callback cb3 = new FileWrite(dirloc+"minzero3_750epochs.csv");
			Callback cb4 = new FileWrite(dirloc+"rlu_140epochs.csv");
			Callback cb5 = new FileWrite(dirloc+"rlu_225epochs.csv");

			Callback rcb = new FileRead("/home/saivenkatesh.k/pkl/data_sample.csv");
			
			String line = (String) rcb.onSuccess(null);
			
			Object[] resulttuple;
			//while(line != null){
				resulttuple = rpcclient.call("service", (long) 10000, 3, line);
				if((RequestStatus) resulttuple[0] == RequestStatus.DONE){
					cb1.onSuccess(resulttuple[1]);
				}
				else{
					cb1.onFailure();
				}
				
				resulttuple = rpcclient.call("service", (long) 10000, 3, line);
				if((RequestStatus) resulttuple[0] == RequestStatus.DONE){
					cb2.onSuccess(resulttuple[1]);
				}
				else{
					cb2.onFailure();
				}
				
				resulttuple = rpcclient.call("service", (long) 10000, 3, line);
				if((RequestStatus) resulttuple[0] == RequestStatus.DONE){
					cb3.onSuccess(resulttuple[1]);
				}
				else{
					cb3.onFailure();
				}
				
				resulttuple = rpcclient.call("service", (long) 10000, 3, line);
				if((RequestStatus) resulttuple[0] == RequestStatus.DONE){
					cb4.onSuccess(resulttuple[1]);
				}
				else{
					cb4.onFailure();
				}
				
				resulttuple = rpcclient.call("service", (long) 10000, 3, line);
				if((RequestStatus) resulttuple[0] == RequestStatus.DONE){
					cb5.onSuccess(resulttuple[1]);
				}
				else{
					cb5.onFailure();
				}
				
				line = (String) rcb.onSuccess(null);
			//}
			
			
			//while(line != null){
				rpcclient.add("service", cb1, (long) 10000, 3, line);
				rpcclient.add("service", cb2, (long) 10000, 3, line);
				rpcclient.add("service", cb3, (long) 10000, 3, line);
				rpcclient.add("service", cb4, (long) 10000, 3, line);
				rpcclient.add("service", cb5, (long) 10000, 3, line);
				rpcclient.addFinalCallback(rcb);
				line = (String) rpcclient.send();
				
			/*	System.out.println("First line completed");
				
			
			
			/*RPCRequest r1 = new RPCRequest("service", cb1, Long.parseLong("10000", 10), 3, line);
			System.out.println(r1);
			System.out.println(r1.hashCode());
			RPCRequest r2 = new RPCRequest("service", cb1, Long.parseLong("10000", 10), 3, line);
			System.out.println(r2);
			System.out.println(r2.hashCode());
			RPCRequest r3 = new RPCRequest("service", cb1, Long.parseLong("10000", 10), 3, line);
			System.out.println(r3);
			System.out.println(r3.hashCode());
			RPCRequest r4 = new RPCRequest("service", cb1, Long.parseLong("10000", 10), 3, line);
			System.out.println(r4);
			System.out.println(r4.hashCode());
			RPCRequest r5 = new RPCRequest("service", cb1, Long.parseLong("10000", 10), 3, line);
			System.out.println(r5);
			System.out.println(r5.hashCode());
		
			System.out.println(r1.equals(r2));
			System.out.println(r1.equals(r3));
			System.out.println(r1.equals(r4));
			System.out.println(r1.equals(r5));*/
				
			//}
			
			((FileWrite) cb1).close();
			((FileWrite) cb2).close();
			((FileWrite) cb3).close();
			((FileWrite) cb4).close();
			((FileWrite) cb5).close();
			((FileRead) rcb).close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		rpcclient.closeThreadPool();
	}

}
