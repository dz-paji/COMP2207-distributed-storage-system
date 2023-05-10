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
    private final ScreenLogger log;
    private final HashMap<String, Socket> storeIndex = new HashMap<>();
    private final Object storeIndexLock = new Object();
    private final HashMap<String, String> fileIndex = new HashMap<>();
    private final Object fileIndexLock = new Object();
    private final HashMap<String, Socket> fileClientIndex = new HashMap<>();
    private final Object fileClientLock = new Object();
    private final HashMap<String, String> fileStoreLookup = new HashMap<>();
    private final Object fileStoreLock = new Object();

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
                    if (fileIndex.get(file).equals("Stored")) {
                        outLine += file + " ";
                    }
                }
            }
            if (outLine.length() > 0) outLine = outLine.substring(outLine.length() - 1, outLine.length());
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
    public void clientStore(String file, String fileSize, Socket client) {
        log.info("STORE request received for " + file + " size " + fileSize);
        synchronized (fileIndexLock) {
            if (checkExistedFile(client, fileIndex.containsKey(file), file)) return;
        }

        int storePoolSize;
        synchronized (storeIndexLock) {
            storePoolSize = storeIndex.size();
        }
        if (storePoolSize < replicFactor) {
            try {
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.warn(file + ": Insufficient dstore");
            return;
        }

        // update file index
        synchronized (fileIndexLock) {
            fileIndex.replace(file, "Storing 0 " + replicFactor);
            // <FileName, "Storing i j"> where i is the No. of dstore acked, j is the number of acks needed
        }
        synchronized (fileClientLock) {
            fileClientIndex.put(file, client);
        }

        String[] portsArray;
        synchronized (storeIndexLock) {
            portsArray = storeIndex.keySet().toArray(String[]::new);
        }

        String ports = "";
        for (int i = 0; i < replicFactor; i++) {
            ports += portsArray[i] + " ";
        }
        if (ports.length() > 0) ports = ports.substring(0, ports.length() - 1);
        try {
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            out.println(Protocol.STORE_TO_TOKEN + " " + ports);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        synchronized (fileStoreLock) {
            fileStoreLookup.put(file, fileSize + " " + ports);
        }
    }

    private Boolean checkExistedFile(Socket client, boolean b, String file) {
        if (b) {
            try {
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.warn(file + ": Already exists");
            return true;
        }
        log.info(file + ": New file");
        return false;
    }

    private void storeAck(String fileName) {
        Boolean stored = false;
        synchronized (fileIndexLock) {
            if (fileIndex.containsKey(fileName) && fileIndex.get(fileName).startsWith("Storing")) {
                String[] status = fileIndex.get(fileName).split(" ");
                int i = Integer.parseInt(status[1]);
                int r = Integer.parseInt(status[2]);
                i++;
                if (i == r) {
                    fileIndex.replace(fileName, "Stored");
                    stored = true;
                } else {
                    fileIndex.replace(fileName, "Storing " + i + " " + r);
                    log.info(fileName + ": " + i + "/" + r + " ACKs received");
                }
            } else {
                log.error(fileName + ": Non-pending file received ACK");
            }
        }

        if (stored) {
            synchronized (fileClientLock) {
                Socket clientSocket = fileClientIndex.get(fileName);
                try {
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    out.println(Protocol.STORE_COMPLETE_TOKEN);
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void clientLoad(String fileName, Socket client) {
        log.info("LOAD request received for " + fileName);
        synchronized (fileIndexLock) {
            if (!fileIndex.containsKey(fileName)) {
                try {
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                log.warn(fileName + ": File does not exist");
                return;
            }
        }

        String ports = "";
        synchronized (fileStoreLock) {
            ports = fileStoreLookup.get(fileName).split(" ")[1];
        }

        if (ports.length() > 0) ports = ports.substring(0, ports.length() - 1);
        try {
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            out.println(Protocol.LOAD_FROM_TOKEN + " " + ports);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        synchronized (fileLoadLock) {
            fileLoadLookup.put(fileName, ports);
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
                        case Protocol.STORE_TOKEN -> clientStore(args[1], args[2], client);
                        case Protocol.STORE_ACK_TOKEN -> storeAck(args[1]);
                        case Protocol.LOAD_TOKEN -> clientLoad(args[1], client);
                        default -> {
                            log.error("Invalid Token");
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
