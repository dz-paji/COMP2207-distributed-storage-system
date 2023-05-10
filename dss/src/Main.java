import java.util.HashMap;

public class Main {
    public static void main(String[] args){
        HashMap<String, String> a = new HashMap<>();
        a.put("a", "b 1 2 3");
        System.out.println(a.get("a").split(" ").length);

    }
}
