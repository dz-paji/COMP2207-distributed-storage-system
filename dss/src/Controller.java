import java.io.IOException;
import java.net.ServerSocket;

public class Controller {
    private final int port;
    private final int replicFactor;
    private final long timeout;
    private final int rebalance;
    private ServerSocket ss;

    /**
     * Controller Constructor
     * @param port - port number
     * @param replicFactor - replication factor
     * @param timeout - timeout (milliseconds)
     * @param rebalance - rebalance factor (seconds)
     */
    public Controller(int port, int replicFactor, long timeout, int rebalance) {
        this.port = port;
        this.replicFactor = replicFactor;
        this.timeout = timeout;
        this.rebalance = rebalance;
        try {
            this.ss = new ServerSocket(port);
    } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        int port = Integer.parseInt(args[0]);
        int replicFactor = Integer.parseInt(args[1]);
        long timeout = Long.parseLong(args[2]);
        int rebalance = Integer.parseInt(args[3]);
        Controller controller = new Controller(port, replicFactor, timeout, rebalance);
    }
}
