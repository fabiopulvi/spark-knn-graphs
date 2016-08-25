

package info.debatty.spark.knngraphs.example;

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import info.debatty.spark.knngraphs.builder.Online;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;


/**
 *
 * @author Fabio
 * This is very similar to spam_timestamp . The only difference is that in this class the nodes are
 * added to the graph WHEN they arrive. The deletion is done, instead, at the end of the batch.
 * TO DO: remove the nodes live and not at the end of the batch to delete the delay.
 */
public class streaming_fabio_spam_live {

    /**
     * @param args the command line arguments
     * @throws IOException
     */

    // Number of nodes in the initial graph
    static final int N = 200;

    // Number of nodes to add to the graph
    static final int N_TEST = 200;
    static final int PARTITIONS = 4;
    static final int K = 10;
    static final double SUCCESS_RATIO = 0.8;
    private static final String HOST = "localhost";
    private static final int PORT = 9999;
    private static long period = 100000;
    private static int batch=1;


    public static void main(String[] args) throws IOException, Exception {
        if (args.length != 1) {
            System.out.println(
                    "Usage: spark-submit --class " +
                            Search.class.getCanonicalName() + " " +
                            "<dataset>");
        }

        String file = args[0];

        // Read the file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Convert to nodes
        int sequence_number = 0;
        List<Node<String>> data = new ArrayList<Node<String>>();
        for (String s : strings) {
            if (sequence_number == N) {
                break;
            }
            Node<String> node = new Node<String>(String.valueOf(data.size()), s);
            node.setAttribute(Online.NODE_SEQUENCE_KEY, sequence_number);
            java.util.Date date = new java.util.Date();
            node.setAttribute("TIMESTAMP", date.getTime());
            data.add(node);
            sequence_number++;
        }


        SparkConf config = new SparkConf().setAppName("spam");
        config.set("spark.streaming.backpressure.enabled", "true");
        config.set("spark.streaming.stopGracefullyOnShutdown", "true");
        JavaSparkContext sc = new JavaSparkContext(config);
        sc.setLogLevel("OFF");

      

        // Parallelize the dataset in Spark
        JavaRDD<Node<String>> nodes = sc.parallelize(data);

        //Define similarity
        SimilarityInterface<String> similarity =
                new SimilarityInterface<String>() {

                    public double similarity(String value1, String value2) {
                        JaroWinkler jw = new JaroWinkler();
                        return jw.similarity(value1, value2);
                    }
                };

        Brute brute = new Brute();
        brute.setK(K);
        brute.setSimilarity(similarity);

        System.out.println("Compute the graph and force execution");
        JavaPairRDD<Node<String>, NeighborList> graph =
                brute.computeGraph(nodes);
        System.out.println(graph.first());

        System.out.println("Prepare the graph for online processing");
        final Online<String> online_graph =
                new Online<String>(K, similarity, sc, graph, PARTITIONS);

        //online_graph.setWindowSize(N);
        //just add nodes

        final Accumulator<Integer> accum_seq = sc.accumulator(sequence_number);
        final int sequence_number_final = sequence_number;


        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(batch));


        List<String> data2 = new ArrayList<String>();
        JavaRDD<String> data2RDD = sc.parallelize(data2);
        LinkedList<Node<String>> nodes_list= new LinkedList<>();


        // Checkpointing must be enabled to use the updateStateByKey function.
       // ssc.checkpoint("./checkpoints");


        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(HOST, PORT);
        

        JavaDStream<String> events = lines.map(
                new Function<String, String>() {
                    @Override
                    public String call(String rawEvent) throws Exception {
                        String[] strings = rawEvent.split(":");
                        return strings[1];
                    }
                }
        );
        List<String> data3 = new ArrayList<String>();


        events.foreachRDD(       //do it for each node
                new Function2<JavaRDD<String>, Time, Void>() {
                    @Override
                    public Void call(JavaRDD<String> rdd, Time time) throws Exception {
// TODO Auto-generated method stub
                        if (rdd != null) {
                            List<String> result = rdd.collect(); // =(
                            data3.addAll(result);
                            for (String s : result) {
                                java.util.Date date = new java.util.Date();
                                Node<String> node = new Node<String>(String.valueOf(accum_seq.value()), s);
                                node.setAttribute(Online.NODE_SEQUENCE_KEY, accum_seq.value());
                                node.setAttribute("TIMESTAMP", date.getTime());
                                online_graph.fastAdd(node);
                                //  System.out.println("just added a node with ts" + node.getAttribute("TIMESTAMP"));
                                nodes_list.add(node);
                                accum_seq.add(1);
                                //System.out.println("seq is:"+accum_seq.value());
                                //System.out.println("graph size is"+online_graph.getGraph().count());

                            }


                            //System.out.println("while from here data3 is:"+data3.size());
                            //Node<String> node = new Node<String>(String.valueOf(accum_seq.value()), result.);
                            //node.setAttribute(Online.NODE_SEQUENCE_KEY, accum_seq.value());
                            //online_graph.fastAdd(node);
                            //System.out.println("just added a node");

                        }
                        return null;
                    }
                }
        );

        JavaDStream<String> events_window = events.window(
                Durations.seconds(batch), Durations.seconds(batch));
        events_window.count().print();
        //events_window.print();


        events_window.foreachRDD(new Function<JavaRDD<String>, Void>() {

            @Override
            public Void call(JavaRDD<String> rdd) throws Exception {
// TODO Auto-generated method stub
                

                    boolean flag = TRUE;
                    java.util.Date date = new java.util.Date();
                    long now = date.getTime();
                    while (flag) {
                        Node<String> node = nodes_list.getFirst();
                        if ((long) node.getAttribute("TIMESTAMP")<(now-period)) {
                            nodes_list.removeFirst();
                            online_graph.fastRemove(node);
                        }
                        else flag=FALSE;
                    }
                    System.out.println("node_list size is: "+nodes_list.size());
                    //for (Node n : nodes_list) System.out.println("it contains: "+n.getAttribute("TIMESTAMP"));
                    Graph<String> local_approximate_graph =
                            list2graph(online_graph.getGraph().collect());
                    System.out.println("graph size is: "+local_approximate_graph.size());

                    List<Node<String>> data3 = new ArrayList<Node<String>>();
                    for (Node<String> n: local_approximate_graph.getNodes()) {data3.add(n);}
                    Graph<String> local_exact_graph =
                            list2graph(brute.computeGraph(sc.parallelize(data3)).collect());


                    int correct = 0;
                    for (Node<String> node : local_exact_graph.getNodes()) {
                        try {
                            correct += local_exact_graph.get(node).countCommons(
                                    local_approximate_graph.get(node));
                        } catch (Exception ex) {
                            System.out.println("Null neighborlist!");
                        }
                    }
                    System.out.println("Found " + correct + " correct edges");
                    double ratio = 1.0 * correct / (data3.size() * K);
                    System.out.println("= " + ratio * 100 + " %");

                    //System.out.println("while from here data3 is:"+data3.size());
                    //Node<String> node = new Node<String>(String.valueOf(accum_seq.value()), result.);
                    //node.setAttribute(Online.NODE_SEQUENCE_KEY, accum_seq.value());
                    //online_graph.fastAdd(node);
                    //System.out.println("just added a node");


                return null;
            }
        });

/*
        events.foreachRDD(new Function<JavaRDD<String>, String>() {
            @Override
            public String call(JavaRDD<String> rdd) throws Exception {
                if (rdd != null) {
                    List<String> result = rdd.collect();
                    //KafkaUtil.sendString(p,topic,result.get(0));
                    //KafkaUtils.sendDataAsString(MTP,topicName, result.get(0));


                }
                return null;
            }
        });
        */



                Function2<JavaRDD<Tuple2<String,String>>,Time, Void> function1 = new Function2<JavaRDD<Tuple2<String, String>>, Time, Void>() {

            @Override
            public Void call(JavaRDD<Tuple2<String,String>> rdd, Time time) throws Exception {

                rdd.foreach(new VoidFunction<Tuple2<String,String>>(){

                    @Override
                    public void call(Tuple2<String,String> stringData) throws Exception {
// Use this data!
                        //data3.add(stringData);
                    }
                });
            return null;
            }
        };

        //events_window.foreachRDD(Function2 < >          (Function<JavaPairRDD<String, String>, Void>) function1);
/*
        events_window.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {

            @Override
            public Void call(JavaPairRDD<String, String> rdd) throws Exception {
// TODO Auto-generated method stub
                rdd.foreach(new VoidFunction<Tuple2<String, String>>(){

                    @Override
                    public void call(Tuple2 stringData) throws Exception {
// Use this data!
                        //System.out.println("W00t!! Data :" + stringData);
                        data3.add(stringData);
                        System.out.println("data 3 here is:"+data3.size());
                    }
                });
                return null;
            }
        });

*/





/*

        //JavaRDD<Tuple2<String,String>> test = events_window.foreachRDD(
        //data3= events_window.foreachRDD(
        events_window.foreachRDD(
                new Function2<JavaPairRDD<String, String>, Time, List<Tuple2<String,String>>>() {

                    @Override
                    public List<Tuple2<String,String>> call(JavaPairRDD<String, String> stringListJavaPairRDD, Time time) throws Exception {

                        System.out.println( stringListJavaPairRDD.collect().toArray());

    return null;
                        //data2.clear();



                    }
                }
        );

*/


        

/*
        ssc.addStreamingListener(new StreamingListener() {

            @Override
            public void onReceiverStarted(StreamingListenerReceiverStarted streamingListenerReceiverStarted) {

            }

            @Override
            public void onReceiverError(StreamingListenerReceiverError receiverError) {
                System.out.println("Do what u want");
                ssc.stop();
            }

            @Override
            public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
                System.out.println("Do what u want");
                ssc.stop(true, true);
            }

            @Override
            public void onBatchSubmitted(StreamingListenerBatchSubmitted streamingListenerBatchSubmitted) {

            }

            @Override
            public void onBatchStarted(StreamingListenerBatchStarted streamingListenerBatchStarted) {
                System.out.println("Batch started!");
                System.out.println("Data3 is" + data3.size());
               


            }

            @Override
            public void onBatchCompleted(StreamingListenerBatchCompleted streamingListenerBatchCompleted) {

            }

            @Override
            public void onOutputOperationStarted(StreamingListenerOutputOperationStarted streamingListenerOutputOperationStarted) {

            }

            @Override
            public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted streamingListenerOutputOperationCompleted) {

            }
        });
*/

        //userTotals.print();
        ssc.start();
        ssc.awaitTermination(400000);

        Graph<String> local_approximate_graph =
                list2graph(online_graph.getGraph().collect());

        //sc.close();
        //Thread.sleep(3000);


    }

    private static Graph<String> list2graph(
            final List<Tuple2<Node<String>, NeighborList>> list) {

        Graph<String> graph = new Graph<String>();
        for (Tuple2<Node<String>, NeighborList> tuple : list) {
            graph.put(tuple._1, tuple._2);
        }

        return graph;
    }

}
