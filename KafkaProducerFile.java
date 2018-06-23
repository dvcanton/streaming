import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


/*
Produces a data stream to a Kafka topic from a file.
Each line from the file is sent as messages to a topic at each interaction
*/
public class KafkaProducerFile {

 public static void main(String[] args) throws InterruptedException, ExecutionException {
   final String fileName = "/resources/SalesJan.csv";
   String line;
   String topicName = test;

   final KafkaProducer < String, String > kafkaProducer;

   Properties properties = new Properties();
   properties.put("bootstrap.servers", "localhost:9092");
   properties.put("client.id", "KafkaFileProducer");
   properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
   properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

   kafkaProducer = new KafkaProducer < String, String > (properties);

   int count = 0;

   try (BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName))) {
    while ((line = bufferedReader.readLine()) != null) {
     count++;
     kafkaProducer.send(new ProducerRecord < String, String > (
      topicName, Integer.toString(count), line));
    }
   } catch (IOException e) {
    e.printStackTrace();
   }
  }
}
