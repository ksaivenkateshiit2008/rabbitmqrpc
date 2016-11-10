package rpc;

/**
 * @author saivenkatesh.k
 *
 *	RPC request statuses
 */
public enum RequestStatus {
	/**
	 * RPC Request is just started
	 */
	START,             
	/**
	 * Non error response is obtained.
	 */
	DONE,
	/**
	 *waits till timeout time and does not receive response.
	 *can be 2 types. Service is down, or Network delay. 
	 */
	TIMEOUT,
	/**
	 * Exception occurred when executing function in service.
	 */
	SERVICEEXCEPTION,
	/**
	 * Exception occurred when connecting to RabbitMQ service
	 */
	RABBITMQEXCEPTION,
	
}
