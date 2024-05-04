package project.stream;
import java.lang.annotation.Documented;
import java.util.Arrays;
import java.util.Collections;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;


import org.apache.spark.sql.streaming.Trigger;
import org.bson.Document;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {




        String bootstrapServers = "localhost:9092";
        String topics = "stream";
        String groupId = "spark job stream";

        String mongoUri = "mongodb+srv://firasmosbehi:firas@cluster0.br9usu1.mongodb.net/";
        String mongoDatabase = "big_data";
        String mongoCollection = "stream";

        SparkSession spark = SparkSession
                .builder()
                .appName("jobs offers salaries stream")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from Kafka
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", args[0])
                .option("kafka.group.id", groupId)
                .load();

        df.selectExpr("CAST(value AS STRING)") // Select only the value column and cast to STRING
                .as(Encoders.STRING()) // Encode as STRING
                .flatMap((FlatMapFunction<String, String>) value -> {
                    String[] messages = value.split("/EOL");
                    return Arrays.asList(messages).iterator();
                }, Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) line -> {
                    String[] elements = line.split("\t");
                    if (elements.length >= 3) {
                        return Arrays.asList(elements[1] + "_" + elements[3]).iterator();
                    } else {
                        // Optional: Log or handle lines with insufficient elements
                        System.err.println("Line '" + line + "' has less than 3 elements, skipping.");
                        throw new Exception("dataset format error");
                    }
                }, Encoders.STRING())
                .groupBy(col("value")) // Group by the combined second and third element
                .count() // Count occurrences within each group
                .writeStream()
                .foreach(new ForeachWriter<Row>() {
                    private MongoClient mongoClient;
                    private MongoCollection<Document> collection;

                    @Override
                    public boolean open(long partitionId, long version) {
                        mongoClient = MongoClients.create(mongoUri);
                        collection = mongoClient.getDatabase(mongoDatabase).getCollection(mongoCollection);
                        return true;
                    }

                    @Override
                    public void process(Row value) {
                        try {
                            String[] keyValue = value.getString(0).split("_");
                            String key = keyValue[0];
                            int count = Math.toIntExact(value.getLong(1));
                            Document document = new Document("key", key)
                                    .append("count", count);
                            collection.insertOne(document);
                            System.out.println("Document inserted: " + document);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println("Error processing document: " + e.getMessage());
                        }

                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        if (mongoClient != null) {
                            mongoClient.close();
                        }
                    }
                })
                .outputMode("complete") // Update console output for each message
                .format("console")
                .option("truncate", false) // Prevent truncation
                .start()
                .awaitTermination();
    }
}