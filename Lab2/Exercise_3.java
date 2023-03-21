package exercise_3;

        import com.google.common.collect.ImmutableMap;
        import com.google.common.collect.Lists;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.graphx.*;
        import org.apache.spark.storage.StorageLevel;
        import scala.Tuple2;
        import scala.collection.Iterator;
        import scala.collection.JavaConverters;
        import scala.reflect.ClassTag$;
        import scala.runtime.AbstractFunction1;
        import scala.runtime.AbstractFunction2;
        import scala.runtime.AbstractFunction3;

        import java.io.Serializable;
        import java.util.ArrayList;
        import java.util.Arrays;
        import java.util.List;
        import java.util.Map;

public class Exercise_3 {
    private static Map<Long, String> labels = ImmutableMap.<Long, String>builder()
            .put(1l, "A")
            .put(2l, "B")
            .put(3l, "C")
            .put(4l, "D")
            .put(5l, "E")
            .put(6l, "F")
            .build();

    //APPLY
    public static class VProg extends AbstractFunction3<Long, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2> implements Serializable {
        @Override
        public Tuple2 apply(Long vertexID, Tuple2<String, Integer> vertexValue, Tuple2<String, Integer> message) {
            if (message._2 == Integer.MAX_VALUE) {
                return vertexValue;
            } else {
                return new Tuple2(message._1, Math.min(vertexValue._2, message._2));
            }
        }
    }

    //SCATTER
    public static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2, Integer>, Iterator<Tuple2<Object, Tuple2>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2>> apply(EdgeTriplet<Tuple2, Integer> triplet) {
            Tuple2<Object, Tuple2> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Tuple2> destinationVertex = triplet.toTuple()._2();

            if ((Integer) sourceVertex._2._2 != Integer.MAX_VALUE && (Integer) sourceVertex._2._2 + triplet.attr() < (Integer) destinationVertex._2._2) {
                Tuple2 msg = new Tuple2(sourceVertex._2._1 + "," + labels.get(destinationVertex._1), (Integer) sourceVertex._2._2 + triplet.attr());

                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Tuple2>(triplet.dstId(), msg)).iterator()).asScala();
            } else {
                    return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2>>().iterator()).asScala();
            }
        }
    }

    //GATHER
    public static class merge extends AbstractFunction2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> implements Serializable {
        @Override
        public Tuple2<String, Integer> apply(Tuple2<String, Integer> o, Tuple2<String, Integer> o2) {
            Tuple2<String, Integer> result;
            if (o._2 <= o2._2) {
                result = o;
            }else{
                result = o2;
                }
            return result;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        List<Tuple2<Object, Tuple2>> vertices = Lists.newArrayList(
                new Tuple2<Object, Tuple2>(1l, new Tuple2(labels.get(1l), 0)),
                new Tuple2<Object, Tuple2>(2l, new Tuple2(labels.get(2l), Integer.MAX_VALUE)),
                new Tuple2<Object, Tuple2>(3l, new Tuple2(labels.get(3l), Integer.MAX_VALUE)),
                new Tuple2<Object, Tuple2>(4l, new Tuple2(labels.get(4l), Integer.MAX_VALUE)),
                new Tuple2<Object, Tuple2>(5l, new Tuple2(labels.get(5l), Integer.MAX_VALUE)),
                new Tuple2<Object, Tuple2>(6l, new Tuple2(labels.get(6l), Integer.MAX_VALUE))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l, 2l, 4), // A --> B (4)
                new Edge<Integer>(1l, 3l, 2), // A --> C (2)
                new Edge<Integer>(2l, 3l, 5), // B --> C (5)
                new Edge<Integer>(2l, 4l, 10), // B --> D (10)
                new Edge<Integer>(3l, 5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Tuple2>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2, Integer> G = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), new Tuple2(null, null), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new Tuple2("", Integer.MAX_VALUE),
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new Exercise_3.VProg(),
                        new Exercise_3.sendMsg(),
                        new Exercise_3.merge(),
                        ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                .sortBy(v1 -> ((Tuple2) ((Tuple2) v1)._2)._1, true, 1)  
                .foreach(v -> {
                    Tuple2<Object, Tuple2> vertex = (Tuple2<Object, Tuple2>) v;
                    System.out.println("Minimum cost to get from " + labels.get(1l) + " to " + labels.get(vertex._1) + " is [" + vertex._2._1 + "] with cost " + vertex._2._2);
                });
    }
}
