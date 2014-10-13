import com.fasterxml.jackson.annotation.JsonUnwrapped
import com.google.common.annotations.VisibleForTesting
import sparkapps.SparkApp






class Tester {

  @org.junit.Test
  def test(){
    //Simple Test
    SparkApp.main(Array("1"));

  }
}
