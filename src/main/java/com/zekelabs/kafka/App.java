package com.zekelabs.kafka;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.opencsv.CSVWriter;
import com.zekelabs.kafka.constants.IKafkaConstants;
import com.zekelabs.kafka.consumer.ConsumerCreator;
import com.zekelabs.kafka.pojo.CustomObject;
import com.zekelabs.kafka.producer.ProducerCreator;

public class App {
	public static void main(String[] args) throws Exception {
        runProducer();
		runConsumer();
	}

	static void runConsumer() throws Exception {
		Consumer<Long, CustomObject> consumer = ConsumerCreator.createConsumer();
		FileWriter csvWriter = new FileWriter("C:/Users/TanuShriPant/Desktop/output.csv");
		

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, CustomObject> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				try {
					csvWriter.append(record.value().getPassengerId());
					csvWriter.append(",");
					csvWriter.append(record.value().getSurvived());
					csvWriter.append(",");
					csvWriter.append(record.value().getPclass());
					csvWriter.append(",");
					csvWriter.append(record.value().getName());
					
					csvWriter.append("\n");
				}catch(IOException e)	{
					System.out.println(e);
				}
				 
//				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value().getPassengerId());
				System.out.println("Record value " + record.value().getSurvived());
//				System.out.println("Record value " + record.value().getPclass());
				System.out.println("Record value " + record.value().getName());
//				System.out.println("Record partition " + record.partition());
//				System.out.println("Record offset " + record.offset());
			});
					
			consumer.commitAsync();
			consumer.close();
		}
		
		csvWriter.flush();
		csvWriter.close();
		
	}

	
	static void runProducer() throws FileNotFoundException {
		Producer<Long, CustomObject> producer = ProducerCreator.createProducer();
		File file = new File("C:/Users/TanuShriPant/Desktop/Titanic-dataset.csv");
        FileReader fr;
        fr = new FileReader(file);
		
		BufferedReader br = new BufferedReader(fr);
        
        String line = "";  
        String splitBy = ",";  
        try {
			while ((line = br.readLine()) != null) {
//	for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
				String[] passenger = line.split(splitBy);
				CustomObject c = new CustomObject();
				c.setPassengerId(passenger[0]);
				c.setSurvived(passenger[1]);
				c.setPclass(passenger[2]);
				c.setName(passenger[3]);
				
				final ProducerRecord<Long, CustomObject> record = new ProducerRecord<Long, CustomObject>(IKafkaConstants.TOPIC_NAME,
						c);
				try {
					RecordMetadata metadata = producer.send(record).get();
					//producer.send(record, new DemoCallback());
				//	System.out.println(index);
				//	System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
				//			+ " with offset " + metadata.offset());
				} catch (ExecutionException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				} catch (InterruptedException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
