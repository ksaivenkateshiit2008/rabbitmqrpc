package rpc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author saivenkatesh.k
 *
 */
class FileWrite implements Callback{

	private BufferedWriter bw;

	/* (non-Javadoc)
	 * @see rpc.Callback#onFailure()
	 */
	@Override
	public Object onFailure() {
		return null;
	}

	/* (non-Javadoc)
	 * @see rpc.Callback#onSuccess(java.lang.Object)
	 */
	@Override
	public Object onSuccess(Object response) {
		try {
			System.out.println((String) response + "in Thread "+ Thread.currentThread().getId());
			bw.write((String) response + '\n');
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * @throws IOException
	 */
	public void close() throws IOException{
		bw.close();
	}
	
	/**
	 * @param filename
	 * @throws IOException
	 */
	FileWrite(String filename) throws IOException{
		FileWriter out = new FileWriter(filename);
		bw = new BufferedWriter(out);
	}
}

/**
 * @author saivenkatesh.k
 *
 */
class FileRead implements Callback{

	/**
	 * 
	 */
	private BufferedReader br;

	/* (non-Javadoc)
	 * @see rpc.Callback#onFailure()
	 */
	@Override
	public Object onFailure() {
		return null;
	}

	/* (non-Javadoc)
	 * @see rpc.Callback#onSuccess(java.lang.Object)
	 */
	@Override
	public Object onSuccess(Object response) {
		try {
			return br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * @throws IOException
	 */
	public void close() throws IOException{
		br.close();
	}
	
	/**
	 * @param filename
	 * @throws FileNotFoundException
	 */
	FileRead(String filename) throws FileNotFoundException{
		FileReader in = new FileReader(filename);
		br = new BufferedReader(in);
	}
}

/**
 * @author saivenkatesh.k
 *
 */
class IntegerSum implements Callback, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2L;
	int[] intarr;
	
	/* (non-Javadoc)
	 * @see rpc.Callback#onFailure()
	 */
	@Override
	public Object onFailure() {
		System.out.println("Zero");
		return 0;
	}

	/* (non-Javadoc)
	 * @see rpc.Callback#onSuccess(java.lang.Object)
	 */
	@Override
	public Object onSuccess(Object response) {
		int sum = 0;
		for(int i : this.intarr){
			sum += i;
		}
		sum += (Integer) response;
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(sum);
		return sum;
	}
	
	/**
	 * @param a
	 */
	IntegerSum(int ... a){
		this.intarr = a;
	}

}

class StringSum implements Callback, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3L;
	String[] intarr;
	
	@Override
	public String onFailure() {
		System.out.println("Empty sum");
		return "";
	}

	@Override
	public Object onSuccess(Object response) {
		String sum = "";
		for(String i : this.intarr){
			sum += i;
		}
		sum += response;
		System.out.println(sum);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return sum;
	}
	
	StringSum(String ... a){
		this.intarr = a;
	}
}

class RepeatString implements Callback, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String str;
	int num;
	
	@Override
	public Object onSuccess(Object response) {
		String resultstr = "";
		for(int i=0; i < num; i++){
			resultstr += this.str+ response;
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(resultstr);
		return resultstr;
	}

	@Override
	public Object onFailure() {
		String resultstr = "";
		for(int i=0; i < num; i++){
			resultstr += this.str;
		}
		System.out.println(resultstr);
		return resultstr;
	}

	RepeatString(String s, int num){
		this.str = s;
		this.num = num;
	}

}
