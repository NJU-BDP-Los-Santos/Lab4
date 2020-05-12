package basic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair>
{
    private int x;
    private int y;
    // x是第一个数字，y是第二个数字

    public IntPair(int left, int right)
    {
        this.x = left;
        this.y = right;
    }

    public IntPair()
    {
        this.x = -1;
        this.y = -1;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(this.x);
        out.writeInt(this.y);
        return;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.x = in.readInt();
        this.y = in.readInt();
        return;
    }

    @Override
    public int compareTo(IntPair other)
    {
        if (this.x != other.x)
        {
            return this.x < other.x ? -1 : 1;
        }
        else if (this.y != other.y)
        {
            return this.y < other.y ? -1 : 1;
        }
        else
            return 0;
    }

    public void print()
    {
        System.out.println(Integer.toString(x) + " " + Integer.toString(y));
    }

    public Text toText()
    {
        return new Text(Integer.toString(this.x) + "\t" + Integer.toString(this.y));
    }

    public int getX()
    {
        return this.x;
    }

    public int getY()
    {
        return this.y;
    }
}
