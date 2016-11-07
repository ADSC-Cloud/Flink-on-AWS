package sg.adsc.jkadbear.flink;

import java.io.Serializable;

/**
 * Created by jkadbear on 13/10/16.
 */
public class RegressionPara implements Serializable {
    private static final long serialVersionUID = 4056690887020800795L;
    public long ts;
    public long markerId;
    public double p1, p2, p3, p4, p5, p6, p7;
    public double a, b;

    public RegressionPara(Long t, Long n) {
        this.ts = t;
        this.markerId = 0L;
        this.p1 = (double) n;
    }

    public RegressionPara(Long t, Long id, Long n) {
        this.ts = t;
        this.markerId = id;
        this.p1 = (double) n;
    }

    public RegressionPara() {
        this.markerId = 0L;
    }

    public void setLeftRegressionPara(Long t, double p1, double p2, double p3) {
        this.ts = t;
        this.p1 = p1;
        this.p2 = p2;
        this.p3 = p3;
    }

    public void SetRightRegressionPara(Long t, double p1, double p4, double p6) {
        this.ts = t;
        this.p1 = p1;
        this.p4 = p4;
        this.p6 = p6;
    }

    public void setFinalRegressionPara(Long t, double p1, double p2, double p3, double p4, double p5, double p6, double p7) {
        this.ts = t;
        this.p1 = p1;
        this.p2 = p2;
        this.p3 = p3;
        this.p4 = p4;
        this.p5 = p5;
        this.p6 = p6;
        this.p7 = p7;
    }

    public void setAB(double a, double b) {
        this.a = a;
        this.b = b;

    }

    public String toString() {
        return "<a:" + Double.toString(a) + ", b:" + Double.toString(b) + ">";
    }
}
