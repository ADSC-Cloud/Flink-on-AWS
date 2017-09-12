package sg.adsc.jkadbear.flink;

import java.io.Serializable;

/**
 * Created by jkadbear on 13/10/16.
 */
public class RegressionPara implements Serializable {
    public static final long serialVersionUID = 4056690887020800795L;
    public long markerId;

    public String hashTag;
    public long x, y;
    public double xSum, ySum, xSqSum, xCrossY, n, a, b;

    public RegressionPara(String hashTag,long markerId,  long x, long y, double xSum, double ySum, double xSqSum, double xCrossY, double n, double a, double b) {
        this.markerId = markerId;
        this.hashTag = hashTag;
        this.x = x;

        this.y = y;
        this.xSum = xSum;
        this.ySum = ySum;
        this.xSqSum = xSqSum;
        this.xCrossY = xCrossY;
        this.n = n;
        this.a = a;
        this.b = b;
    }

    public RegressionPara(String hashTag, Long markerId, Long x) {
        this(hashTag, markerId, x, 1, x, 1, x*x, 0, 1, 0, 0);
    }

    public RegressionPara() {}

    public void setMarkerId(long markerId) {
        this.markerId = markerId;
    }

    public String getHashTag() {
        return hashTag;
    }

    public void setHashTag(String hashTag) {
        this.hashTag = hashTag;
    }

    public void setX(long x) {
        this.x = x;
    }

    public void setY(long y) {
        this.y = y;
    }

    public double getxSum() {
        return xSum;
    }

    public void setxSum(double xSum) {
        this.xSum = xSum;
    }

    public double getySum() {
        return ySum;
    }

    public void setySum(double ySum) {
        this.ySum = ySum;
    }

    public double getxSqSum() {
        return xSqSum;
    }

    public void setxSqSum(double xSqSum) {
        this.xSqSum = xSqSum;
    }

    public double getxCrossY() {
        return xCrossY;
    }

    public void setxCrossY(double xCrossY) {
        this.xCrossY = xCrossY;
    }

    public double getN() {
        return n;
    }

    public void setN(double n) {
        this.n = n;
    }

    public Long getMarkerId() {
        return this.markerId;
    }

    public Long getX() {
        return this.x;
    }

    public Long getY() {
        return this.y;
    }

    public void setRightRegressionPara(Long x, double xSum, double xSqSum, double n) {
        this.x = x;
        this.xSum = xSum;
        this.xSqSum = xSqSum;
        this.n = n;
    }

    public void setAB(double a, double b) {
        this.a = a;
        this.b = b;

    }

    public String toString() {
        return "<a:" + Double.toString(a) + ", b:" + Double.toString(b) + ">";
    }
}
