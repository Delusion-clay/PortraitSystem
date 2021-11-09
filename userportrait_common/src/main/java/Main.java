import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-09-01 19:52
 */
public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        List<Integer> ints = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ints.add(i, i + 1);
        }
        int countNum = 3;
        while (ints.size()>1){
            if (countNum/3==0){
                Integer remove = ints.remove(countNum / 3);
                if (ints.size()==1) break;
            }
            countNum++;
        }
        System.out.println(ints.toString());

    }
}
