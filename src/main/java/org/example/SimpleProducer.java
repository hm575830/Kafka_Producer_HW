package org.example;//import util.properties packages
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.UnixStyleUsageFormatter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

//import util.properties packages
//import simple producer packages
//import KafkaProducer packages
//import ProducerRecord packages
//import simple producer packages
//import KafkaProducer packages
//import ProducerRecord packages

//Create java class named "org.example.SimpleProducer"
public class SimpleProducer {
    @Parameter(names = {"--brokers", "-b"}, order = 1, arity = 1,required = true)
    private String brokers;
    @Parameter(names = {"--topic", "-t"},  order = 2, arity = 1)
    private String topicName="test";
    @Parameter(names = {"--records", "-r"}, order = 3, arity = 1)
    private int numRecords=10;
    @Parameter(names = {"--recordSize", "-rS"},  order = 4, arity = 1)
    private int recordSize=1000;
    @Parameter(names = {"--help","-h"}, help = true)
    private boolean help;


    public SimpleProducer() {
    }


    public static void main(String[] args) throws Exception{

        //Assign topicName to string variable

        SimpleProducer myProducer= new SimpleProducer();
        JCommander jCommander= JCommander.newBuilder()
                .programName("myProducer")
                .addObject(myProducer)
                .build();
        jCommander.setUsageFormatter(new UnixStyleUsageFormatter(jCommander));
        jCommander.parse(args);
        if(myProducer.help){
            System.out.println("Please set the broker address.");
            return;
        }
        if(myProducer.topicName.equals("")){
            System.out.println("Please set the name of topic.");
            return;
        }
        if(myProducer.numRecords<=0){
            System.out.println("Please set the number of records.");
            return;
        }
        if(myProducer.recordSize<=0){
            System.out.println("Please set the length of a record.");
            return;
        }


        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", myProducer.brokers);

        //Set acknowledgements for producer requests.
        props.put("acks", "1");

         //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        for(int i = 0; i < myProducer.numRecords; i++)
            producer.send(new ProducerRecord<String, String>(myProducer.topicName,
                    String.format("key-%010d",i).concat(new String(new byte[myProducer.recordSize/2-14],"UTF-8")),
                    String.format("value-%010d",i).concat(new String(new byte[myProducer.recordSize/2-16],"UTF-8"))
            ));
        System.out.println("Message sent successfully");
        producer.close();
    }
}