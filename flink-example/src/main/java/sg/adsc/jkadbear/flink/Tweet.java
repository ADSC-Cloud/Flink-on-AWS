package sg.adsc.jkadbear.flink;

import java.io.Serializable;

/**
 * Created by jkadbear on 13/10/16.
 */
public class Tweet implements Serializable {
    public int uselessKey;
    public String hashTag;
    public RegressionPara regPara;

    public Tweet() {}

    public Tweet(String hashTag, RegressionPara regPara) {
        this.uselessKey = 0;
        this.hashTag = hashTag;
        this.regPara = regPara;
    }

    public long getCreateTime() {
        return regPara.ts;
    }

    public long getMarkerId() {
        return regPara.markerId;
    }

    @Override
    public String toString() {
        return "<hashTag:" + this.hashTag + ", a:" + this.regPara.a + ", b:" + this.regPara.b + ">";
    }
}
