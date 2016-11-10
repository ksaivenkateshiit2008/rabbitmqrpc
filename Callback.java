package rpc;

/**
 * @author saivenkatesh.k
 *
 */
public interface Callback {

	/**
	 * @return Output of function when response is not obtained
	 */
	Object onFailure();

	/**
	 * @param response Message received from request 
	 * @return Output of function 
	 */
	Object onSuccess(Object response);

}
