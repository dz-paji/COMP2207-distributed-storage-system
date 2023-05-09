import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Controller {
    private final int port;
    private final int replicFactor;
    private final long timeout;
    private final int rebalance;
    private ServerSocket ss;
    private ScreenLogger log;
    private HashMap<String, Socket> storeIndex = new HashMap<>();
    private Object storeIndexLock = new Object();
    private HashMap<String, String> fileIndex = new HashMap<>();
    private Object fileIndexLock = new Object();

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
        this.log = new ScreenLogger("Controller");
    }

    /**
     * Handle join request from dstore
     * JOIN <port>
     * @param port - port number
     * @param client - dstore socket
     */
    private void storeJoin(String port, Socket client) {
        log.info("JOIN request received from " + port);
        synchronized (storeIndexLock) {
            storeIndex.put(port, client);
        }
    }

    /**
     * Handle list request from client
     * LIST
     * @param client - client socket
     */
    public void listFiles(Socket client) {
        try {
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            String outLine = "";
            synchronized (fileIndexLock) {
                for (String file : fileIndex.keySet()) {
                    if (!(fileIndex.get(file).equals("Storing") || fileIndex.get(file).equals("Removing"))) {
                        outLine += file;
                    }
                }
            }
            out.println(outLine);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle store request from client
     * STORE <filename> <size>
     * @param client - client socket
     */
    public void clientStore(String file, Socket client) {
        int storePoolSize;
        synchronized (storeIndexLock) {
            storePoolSize = storeIndex.size();
        }

        // check if there are enough dstores
        if (storePoolSize < replicFactor) {
            try {
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);

            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        // update file index
        synchronized (fileIndexLock) {
            fileIndex.put(file, "Storing");
        }

        String[] portsArray;
        synchronized (storeIndexLock) {
            portsArray = storeIndex.keySet().toArray(String[]::new);
        }

        String ports = "";
        for (int i = 0; i < replicFactor; i++) {
            ports += portsArray[i] + " ";
        }
        try {
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            out.println(Protocol.STORE_TO_TOKEN + " " + ports);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        try {
            this.ss = new ServerSocket(port);

            while (true) {
                Socket client = ss.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String line;
                while ((line = in.readLine() ) != null) {
                    String[] args = line.split(" ");
                    switch (args[0]) {
                        case Protocol.JOIN_TOKEN -> storeJoin(args[1], client);
                        case Protocol.LIST_TOKEN -> listFiles(client);
                        case Protocol.STORE_TOKEN -> clientStore(args[1] ,client);
                        default -> {
                            log.error("Invalid command");
                            log.error(line);
                        }
                    }
                }
            }
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
        controller.start();
    }
}
