package rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author saivenkatesh.k
 *
 */
public class MsgSerialize {
	/**
	 * @param obj Object to be Serialized is sent as message to RabbitMQ server.
	 * @return Byte array conversion
	 * @throws IOException throws if object is not native java type or does not
	 * 					   implement serializable interface
	 */
	public static byte[] serialize(Object obj) throws IOException{
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(obj);
		return b.toByteArray();
	}
	
	/**
	 * @param bytes byte array to be converted back to Object.
	 * @return Original object which is serialized to create this array
	 * @throws IOException if it is not convertible to Object.
	 * @throws ClassNotFoundException if Class is not in class path
	 */
	public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException{
		ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
	}
}
