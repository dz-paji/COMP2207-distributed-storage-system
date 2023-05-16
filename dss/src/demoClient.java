import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class demoClient {
  public static void main(String[] args) {
    try {
      Socket client = new Socket("localhost", 1000);
      PrintWriter out = new PrintWriter(client.getOutputStream(), true);
      out.println("LIST");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
