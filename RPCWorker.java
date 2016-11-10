package rpc;

/**
 * @author saivenkatesh.k
 *
 */
public class RPCWorker implements Runnable {

	private Object response;
	private RequestStatus status;
	private Callback cb;
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		if(this.status == RequestStatus.DONE){
			this.cb.onSuccess(response);
		}
		else{
			this.cb.onFailure();
		}
	}

	/**
	 * @param Callback Service callbacks to be executed after response obtained
	 * @param response Message body received from service
	 * @param status   Status of Response
	 */
	public RPCWorker(Callback cb, Object response, RequestStatus status){
		this.cb = cb;
		this.response = response;
		this.status = status;
	}
}
