package rpc;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author saivenkatesh.k
 *
 */
public class RMQWrapper {
	
	private static final Logger logger = Logger.getLogger(RPCClient.class.getName());
	private Connection connection;
	private Channel channel;
	private QueueingConsumer consumer;
	
	/**
	 * Constructor when called creates connection to RabbitMQ server and 
	 * creates channel to consume from queuename.
	 * 
	 * @param queuename 	RabbitMQ Queue from which it has to consume messages.
	 * @param ack			Flag to set channel property for acknowledgment.
	 * @throws IOException  when there is problem in connecting to RabbitMQ server.
	 */
	public RMQWrapper() throws IOException{
		/**
		 * 	All properties are taken from this file username, password, host etc
		 */
		String propFileName = "src/rpc/rabbitmq.properties";
		  
		InputStream inputstream = new FileInputStream(propFileName);
		  
		Properties prop = new Properties();
		prop.load(inputstream);
		  
		String username = prop.getProperty("username");
		String password = prop.getProperty("password");
		String host = prop.getProperty("host");
	    
		logger.info("Following are parameters used for RabbitMQ connection");
		logger.info("username="+username+", password="+password+", host="+host);
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername(username);
	    factory.setPassword(password);
	    factory.setHost(host);
	    connection = factory.newConnection();
	    channel = connection.createChannel();
	    
	}
	
	void setConsumer(String queuename, Boolean ack) throws IOException{
	    consumer = new QueueingConsumer(channel);
	    if(ack){
	    	channel.basicConsume(queuename, true, consumer);
	    }
	    else{
		    channel.basicQos(1);
	    	channel.basicConsume(queuename, false, consumer);  	
	    }
	}
	
	/**
	 * Waits till a new message appears in queue. This waiting depends on
	 * timeout parameter.
	 * 
	 * @param timeout if null, waits for infinite time till next message 
	 * 						   appears in queue.
	 *                else     Waits till maximum timeout time. If message
	 *                		   does not occured, it stops waiting and returns
	 *                         null.
	 *                         
	 * @return Delivery object containing the message, headers and other 
	 *         properties required by either RPCClient or RPCServer. 
	 * 
	 * @throws IOException | ShutdownSignalException | ConsumerCancelledException 
	 * | InterruptedException when delivery is not obtained because of RabbitMQ 
	 * 						  Server issues.   
	 */
	public QueueingConsumer.Delivery getResponse(Long timeout) throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException{
		if(timeout == null){
			
			//Waits till next message appeared in queue
			return consumer.nextDelivery();
		}
		else{
			
			//Waits till timeout time.
			return consumer.nextDelivery(timeout);
		}
	}
	
	/**
	 * Publishing msg to either exchange or queue directly.
	 * Along with msg, it will add headers, replyqueuename, corrid if user
	 * provides any of them. 
	 * Also sends Ack if deliverytag is given.  
	 * 
	 * @param msg				Actual message to be sent to queue or exchange.
	 * 							message will be serialized and sent to queue.
	 * @param headermap			A hashmap containing additional parameters to 
	 * 							be sent along with message.
	 * @param replyqueuename 	ReplyQueue property to be added if not null,
	 * 							Result message will be sent to this queue.
	 * @param corrid			Property to be added along with message if not
	 * 							null. Used for identifying the request if 
	 * 							multiple messages are present in same queue.
	 * @param deliverytag		If given, it will send acknowledgment
	 * @param queue				Queue to which msg has to be delivered. If this
	 * 							is given, exchange and routingkey should be null
	 * @param exchange			Should be given if queue is null. Along with 
	 * 							this routingkey should also have to be present.
	 * @param routingkey		Used for identifying queue if multiple queues 
	 * 							are present in RabbitMQ server.		
	 * 
	 * @throws UnsupportedEncodingException | IOException in case of error in 
	 * 		publishing message to server. 				
	 */
	public void sendRequest(Object msg,
			         Map<String, Object> headermap,
			         String replyqueuename,  //It is added as property to message
			         String corrid,
			         Long deliverytag,
			         String queue,
			         String exchange,
			         String routingkey) throws UnsupportedEncodingException, IOException{
		
		BasicProperties props = null;
		
		//Adding all properties for messages.
		props = new BasicProperties.Builder()
								   .correlationId(corrid)
								   .replyTo(replyqueuename)
								   .headers(headermap)
								   .build();
		
		if(exchange == null){
			//Assigning routing key as queue name as the message goes to queue
			channel.basicPublish("", queue, props, MsgSerialize.serialize(msg));
		}
		else{
			channel.basicPublish(exchange, routingkey, props, MsgSerialize.serialize(msg));
		}
		
		//Acknowledgment
		if(!deliverytag.equals(Long.MAX_VALUE)){
			channel.basicAck(deliverytag, false);
		}
	}
	
	/**
	 * @throws IOException
	 */
	void close() throws IOException{
		connection.close();
	}
		
}
