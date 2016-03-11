package cn.m1c.rocket;
 
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import cn.m1c.core.util.StringUtil;
 
public class PushConsumerSort {
         /**
          * 当前例子是PushConsumer用法，使用方式给用户感觉是消息从RocketMQ服务器推到了应用客户端。<br>
          * 但是实际PushConsumer内部是使用长轮询Pull方式从MetaQ服务器拉消息，然后再回调用户Listener方法<br>
          */
         public static void main(String[] args) throws InterruptedException,
                            MQClientException{
                   /**
                    * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br>
                    * 注意：ConsumerGroupName需要由应用来保证唯一
                    */
                   DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
                                     "ConsumerGroupName1");
                   consumer.setNamesrvAddr("192.168.1.11:9876");
                   consumer.setInstanceName("Consumber1");
                   consumer.setMessageModel(MessageModel.CLUSTERING);
                   /**
                    * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
                    * 如果非第一次启动，那么按照上次消费的位置继续消费
                    */
                   consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
 
                   /**
                    * 订阅指定topic下tags分别等于TagA或TagC或TagD
                    */
                   consumer.subscribe("TopicTest1","TagA || TagC || TagD");
                   /**
                    * 订阅指定topic下所有消息<br>
                    * 注意：一个consumer对象可以订阅多个tag
                    */
                   consumer.subscribe("TopicTest2","*");
 
                   consumer.registerMessageListener(new MessageListenerOrderly() {
 
                            public ConsumeOrderlyStatus  consumeMessage(
                                               List<MessageExt>msgs, ConsumeOrderlyContext  context) {
                            	context.setAutoCommit(true);
                                     System.out.println(Thread.currentThread().getName()
                                                        +" Receive New Messages: " + msgs.size());
 
                                     MessageExt msg = msgs.get(0);
                                     for (MessageExt msgt: msgs) {
//                                         System.out.println(msgt + ", content:" + new String(msgt.getBody()));
                                    	 System.out.println(StringUtil.getString(msg.getBody()));
                                     }
                                     /*try {
										TimeUnit.MILLISECONDS.sleep(1000);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}*/
                                     System.out.println("end");
                                     return ConsumeOrderlyStatus.SUCCESS;
 
                            }
                   });
 
                   /**
                    * Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
                    */
                   consumer.start();
 
                   System.out.println("ConsumerStarted.");
         }
}