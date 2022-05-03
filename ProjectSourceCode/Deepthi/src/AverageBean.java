import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class AverageBean implements Writable {

    double totalFlightCount;
    double delayInMinutes;

    public AverageBean(){}

    public AverageBean(double totalFlightCount, double delayInMinutes) {
        this.delayInMinutes = delayInMinutes;
        this.totalFlightCount = totalFlightCount;
    }

    @Override
    public String toString() {
         return delayInMinutes +" " +totalFlightCount;
    }



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



    public void readFields(DataInput in) throws IOException {
        totalFlightCount = in.readDouble();
        delayInMinutes = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(totalFlightCount);
        out.writeDouble(delayInMinutes);

    }

}
