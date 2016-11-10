package rpc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class MsgPyFunc implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String funcname;
	private ArrayList<Object> args;
	private HashMap<String, Object> kwargs;
	
	String getFuncName(){
		return this.funcname;
	}
	
	ArrayList<Object> getargs(){
		return this.args;
	}
	
	HashMap<String, Object> getkwargs(){
		return this.kwargs;
	}
	
	MsgPyFunc(String funcname, ArrayList<Object> args, HashMap<String, Object> kwargs){
		this.funcname = funcname;
		this.args = args;
		this.kwargs = kwargs;
	}

}
