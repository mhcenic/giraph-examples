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

public class DegreeCentrality
  extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

  public static final LongConfOption SOURCE_ID = new LongConfOption(
    "SimpleShortestPathsVertex.sourceId",
    1,
    "The shortest paths id"
  );
  private static final Logger LOG = Logger.getLogger(
    DegreeCentrality.class
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
    // TODO Auto-generated method stub
    if (getSuperstep() == 0) {
      Iterable<Edge<LongWritable, FloatWritable>> edges = vertex.getEdges();
      for (Edge<LongWritable, FloatWritable> edge : edges) {
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(1.0));
      }
    } else {
      long sum = 0;
      for (DoubleWritable message : messages) {
        sum++;
      }
      DoubleWritable vertexValue = vertex.getValue();
      vertexValue.set(sum + vertex.getNumEdges());
      vertex.setValue(vertexValue);
      vertex.voteToHalt();
    }
  }
}
