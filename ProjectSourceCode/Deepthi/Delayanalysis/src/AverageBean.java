import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class AverageBean implements Writable {

    public AverageBean(){}

    public AverageBean(double totalFlightCount, double delayInMinutes) {
        this.delayInMinutes = delayInMinutes;
        this.totalFlightCount = totalFlightCount;
    }

    @Override
    public String toString() {
        return delayInMinutes +" " +totalFlightCount;
    }

    double delayInMinutes;

    public double getDelayInMinutes() {
        return delayInMinutes;
    }

    public void setDelayInMinutes(double delayInMinutes) {
        this.delayInMinutes = delayInMinutes;
    }

    public double getTotalFlightCount() {
        return totalFlightCount;
    }

    public void setTotalFlightCount(double totalFlightCount) {
        this.totalFlightCount = totalFlightCount;
    }

    double totalFlightCount;

   //Deserialization.
    public void readFields(DataInput in) throws IOException {
        totalFlightCount = in.readDouble();
        delayInMinutes = in.readDouble();
    }

    //Serialization.
    public void write(DataOutput out) throws IOException {
        out.writeDouble(totalFlightCount);
        out.writeDouble(delayInMinutes);
    }

}
