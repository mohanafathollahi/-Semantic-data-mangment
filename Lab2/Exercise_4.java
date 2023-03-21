package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Exercise_4 {

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws IOException {
		String edgesPath =  "src/main/resources/wiki-edges.txt";
		String verticesPath = "src/main/resources/wiki-vertices.txt";


		//Vertices creation
		List<Row> vertices_list = new ArrayList<>();
		BufferedReader brver = new BufferedReader(new FileReader(verticesPath)); //brver: buffered reader vertices
		for (String line = brver.readLine();
			 line != null;
			 line = brver.readLine()) {
			String[] item = line.split("\t");
			Row rowvertice = RowFactory.create(item[0], item[1]); //['a', 'b']
			vertices_list.add(rowvertice);
		}
		brver.close();

		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		Dataset<Row> vertices = sqlCtx.createDataFrame(vertices_rdd, vertices_schema);


		// Edges Creation
		List<Row> edges_list = new ArrayList<>();
		BufferedReader bredg = new BufferedReader(new FileReader(edgesPath)); //bredg: buffered reader edges
		for (String line = bredg.readLine(); line != null; line = bredg.readLine())
			 {
			String[] item = line.split("\t");
			Row rowedge = RowFactory.create(item[0], item[1]); //['a', 'b']
			edges_list.add(rowedge);
		}
		bredg.close();

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
		});
		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);


		//Merge All
		GraphFrame gf = GraphFrame.apply(vertices, edges); //apply(vertices, edges)

		System.out.println(gf);
		gf.edges().show();
		gf.vertices().show();


		// PageRank: d = 0.85 -> resetProbability = 0.15
		PageRank rank = gf.pageRank();
		GraphFrame results = rank.resetProbability(0.15).maxIter(10).run();
		Dataset<Row> pageRankNodes =  results.vertices().orderBy(functions.desc("pagerank"));

		pageRankNodes.show(10);//to present as a table the top10 PageRanks
	}

}
