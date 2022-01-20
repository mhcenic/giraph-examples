import java.io.IOException;
import org.apache.giraph.Algorithm;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class EigenvectorCentrality
  extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

  public static final LongConfOption SOURCE_ID = new LongConfOption(
    "SimpleShortestPathsVertex.sourceId",
    1,
    "The shortest paths id"
  );
  private static final Logger LOG = Logger.getLogger(
    EigenvectorCentrality.class
  );

  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  @Override
  public void compute(
    Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
    Iterable<DoubleWritable> messages
  )
    throws IOException {
    if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      DoubleWritable vertexValue = new DoubleWritable(
        (0.15f / getTotalNumVertices()) + 0.85f * sum
      );
      vertex.setValue(vertexValue);
      aggregate(MAX_AGG, vertexValue);
      aggregate(MIN_AGG, vertexValue);
      aggregate(SUM_AGG, new LongWritable(1));
    } else {
      vertex.setValue(1 / getTotalNumVertices());
    }
    if (getSuperstep() < MAX_SUPERSTEPS) {
      long edges = vertex.getNumEdges();
      sendMessageToAllEdges(
        vertex,
        new DoubleWritable(vertex.getValue().get() / edges)
      );
    } else {
      vertex.voteToHalt();
    }
  }
}
