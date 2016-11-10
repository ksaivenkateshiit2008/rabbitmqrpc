package rpc;

import rpc.RequestStatus;

/**
 * @author saivenkatesh.k
 *
 */
public class RPCRequest {
	int currentattempt;
	private int maxattempts;
	private RequestStatus status;
	Long timeout;
	private Callback cb;
	private String servicename;
	Object message;
	private Long timeelapsed = (long) 0;
	
	/**
	 * @param servicename 	service to send this message and get response
	 * @param cb			Callback object to be executed after getting response
	 * @param timeout		Maximum Time to wait for response
	 * @param maxattempts	Maximum number of retries in case of error responses.
	 * @param message		message to be passed as body of request
	 */
	public RPCRequest(String servicename, Callback cb, Long timeout, int maxattempts, Object message){
		this.servicename = servicename;
		this.currentattempt = 1;
		this.status = RequestStatus.START;
		this.timeout = timeout;
		this.cb = cb;
		this.maxattempts = maxattempts;
		this.message = message;
	}
	
	/**
	 * @return gets the current status at time of call
	 */
	public RequestStatus getStatus(){
		return status;
	}
	
	/**
	 * @param status sets this as status for this request
	 */
	public void setStatus(RequestStatus status){
		this.status = status;
	}
		
	/**
	 * @return Maximum number of attempts to be tried
	 */
	public int getmaxAttempts(){
		return this.maxattempts;
	}
		
	/**
	 * @return current attempt at the time of calling 
	 */
	public int getCurrentattempt(){
		return this.currentattempt;
	}

	/**
	 * Increments the current attempt by 1
	 */
	public void incCurrentAttempts(){
		this.currentattempt += 1;
	}
	
	/**
	 * @return Callback object stored in this request
	 */
	public Callback getcallback(){
		return this.cb;
	}
	
	/**
	 * @return service name for this request
	 */
	public String getService(){
		return this.servicename;
	}
	
	/**
	 * @return Message to sent to queue
	 */
	public Object getMsg(){
		return this.message;
	}
	
	/**
	 * @param deltatime Time to be added to timeelapsed which is used for 
	 * 					Timeouts
	 * @return true if timeout time is not reached.
	 * 		   false if timeout time is reached
	 */
	public Boolean addTimeElapsed(Long deltatime){
		if(this.timeelapsed >= this.timeout){
			return false;
		}
		else{
			this.timeelapsed += deltatime;
			return true;
		}
	}
	
	String toPrettyString(){
		return "RPCClient("+servicename+", "+cb+", "+timeout+", "+maxattempts+", "+message+")";
	}
}
