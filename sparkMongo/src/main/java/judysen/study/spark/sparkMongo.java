package judysen.study.spark;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import static java.util.Arrays.asList;

public class sparkMongo {
    public static void main(String[] args) {
        /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
        SparkSession spark = SparkSession.builder()
                .master("spark://192.168.61.138:7077")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.myCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.myCollection")
                .config("spark.executor.memory","512m")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.addJar("F:\\github\\java\\spark-examples\\sparkMongo\\target\\sparkMongo-1.0.0.jar");

        // More application logic would go here...

        // Create a RDD of 10 documents
        JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                (new Function<Integer, Document>() {
                    public Document call(final Integer i) throws Exception {
                        return Document.parse("{test: " + i + "}");
                    }
                });

        /*Start Example: Save data from RDD to MongoDB*****************/
        //MongoSpark.save(sparkDocuments, writeConfig);
        MongoSpark.save(documents);
        /*End Example**************************************************/
        jsc.close();
    }
}
