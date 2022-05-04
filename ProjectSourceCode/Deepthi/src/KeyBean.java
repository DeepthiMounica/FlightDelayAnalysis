import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class KeyBean implements Writable {


    String carrier;
    String delayReason;

    public KeyBean(String carrier, String delayReason) {
        this.carrier = carrier;
        this.delayReason = delayReason;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getDelayReason() {
        return delayReason;
    }

    public void setDelayReason(String delayReason) {
        this.delayReason = delayReason;
    }



    public KeyBean(){}


    @Override
    public String toString() {
        return carrier + " " + delayReason;
    }




    public void readFields(DataInput in) throws IOException {
        carrier = in.readLine();
        delayReason = in.readLine();
    }

    public void write(DataOutput out) throws IOException {
        out.writeChars(carrier);
        out.writeChars(delayReason);
    }

}
