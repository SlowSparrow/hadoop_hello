package hadoop;

import java.util.StringTokenizer;

public class TestStringTokenizer {
    public static void main(String[] args) {
        String s = "wo de ma ya\nzhe\tshi\nna";
        StringTokenizer st = new StringTokenizer(s);
        while (st.hasMoreElements()){
            System.out.println(st.nextToken());
        }
    }
}
