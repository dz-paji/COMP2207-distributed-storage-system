import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ControllerHandler implements Runnable {
    private final int replicFactor;
    private final long timeout;
    private final int rebalance;
    private final Socket client;
    private final ScreenLogger log;
    private final HashMap<String, Socket> storeIndex;
    private final Object storeIndexLock;
    private final HashMap<String, String> fileIndex;
    private final Object fileIndexLock;
    private final HashMap<String, Socket> fileClientIndex;
    private final Object fileClientLock;
    private final HashMap<String, String> fileStoreLookup;
    private final Object fileStoreLock;
    public Thread thread;

    /**
     * Controller Constructor
     * @param client - client socket
     * @param replicFactor - replication factor
     * @param timeout - timeout (milliseconds)
     * @param rebalance - rebalance factor (seconds)
     * @param i - thread number
     * @param storeIndex - store index
     * @param storeIndexLock - store index lock
     * @param fileIndex - file index
     * @param fileIndexLock - file index lock
     * @param fileClientIndex - file client index
     * @param fileClientLock - file client lock
     * @param fileStoreLookup - file store lookup
     * @param fileStoreLock - file store lock
     */
    public ControllerHandler(Socket client, int replicFactor, long timeout, int rebalance, int i, HashMap<String, Socket> storeIndex, Object storeIndexLock, HashMap<String, String> fileIndex, Object fileIndexLock, HashMap<String, Socket> fileClientIndex, Object fileClientLock, HashMap<String, String> fileStoreLookup, Object fileStoreLock) {
        this.client = client;
        this.replicFactor = replicFactor;
        this.timeout = timeout;
        this.rebalance = rebalance;
        this.log = new ScreenLogger("Controller-" + i);
        this.storeIndex = storeIndex;
        this.storeIndexLock = storeIndexLock;
        this.fileIndex = fileIndex;
        this.fileIndexLock = fileIndexLock;
        this.fileClientIndex = fileClientIndex;
        this.fileClientLock = fileClientLock;
        this.fileStoreLookup = fileStoreLookup;
        this.fileStoreLock = fileStoreLock;

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
        log.info("LIST request received");
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
            out.println(Protocol.LIST_TOKEN + outLine);
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
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.debug(storePoolSize + " Dstores joined");
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

                } catch (IOException e) {
                    e.printStackTrace();
                }
                fileClientIndex.remove(fileName);
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

                } catch (IOException e) {
                    e.printStackTrace();
                }
                log.warn(fileName + ": File does not exist");
                return;
            }
        }

        int i = 0;
        synchronized (fileIndexLock) {
            if (fileIndex.get(fileName).startsWith("Stored")) {
                fileIndex.replace(fileName, "Loading 0");
            } else if (fileIndex.get(fileName).startsWith("Loading")) {
                i = Integer.parseInt(fileIndex.get(fileName).split(" ")[1]);

                // If no dstore to try
                if (i == fileIndex.get(fileName).split(" ").length - 2) {
                    try {
                        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                        out.println(Protocol.ERROR_LOAD_TOKEN);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    log.warn(fileName + ": No dstore to try");
                    return;
                }
                i++;
                fileIndex.replace(fileName, "Loading " + i);
            } else {
                log.error(fileName + ": Invalid state for loading");
                return;
            }
        }

        String ports = "";
        String fileSize = "";
        synchronized (fileStoreLock) {
            ports = fileStoreLookup.get(fileName).split(" ")[i + 1];
            fileSize = fileStoreLookup.get(fileName).split(" ")[0];
        }

        try {
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            out.println(Protocol.LOAD_FROM_TOKEN + " " + ports + " " + fileSize);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Remove file from dstore
     * @param fileName name of the file
     * @param client client socket
     */
    private void clientRemove(String fileName, Socket client) {
        log.info("REMOVE request received for " + fileName);
        synchronized (fileIndexLock) {
            if (!fileIndex.containsKey(fileName)) {
                try {
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                } catch (IOException e) {
                    e.printStackTrace();
                }
                log.warn(fileName + ": File does not exist");
                return;
            }
        }

        int dstores = 0;
        List<String> dstorePorts;
        synchronized (fileStoreLock) {
            dstores = fileStoreLookup.get(fileName).split(" ").length - 1;
            dstorePorts = Arrays.stream(fileStoreLookup.get(fileName).split(" ")).toList();
        }
        dstorePorts.remove(0);
        synchronized (fileIndexLock) {
            if (fileIndex.get(fileName).startsWith("Stored")) {
                fileIndex.replace(fileName, "Removing 0 " + dstores);
            }
        }

        synchronized (fileClientLock) {
            fileClientIndex.put(fileName, client);
        }

        synchronized (storeIndexLock) {
            for (String port : dstorePorts) {
                Socket dstore = storeIndex.get(port);
                try {
                    PrintWriter out = new PrintWriter(dstore.getOutputStream(), true);
                    out.println(Protocol.REMOVE_TOKEN + " " + fileName);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void dstoreRmAck(String fileName) {
        String[] status = null;
        synchronized (fileIndexLock) {
            if (fileIndex.get(fileName).startsWith("Removing")) {
                status = fileIndex.get(fileName).split(" ");
            } else {
                log.error(fileName + ": Invalid state for remove ack");
            }
        }
        int i = Integer.parseInt(status[1]);
        int r = Integer.parseInt(status[2]);
        i++;
        if (i == r) {
            fileIndex.remove(fileName);
            synchronized (fileClientIndex) {
                // Notify client
                Socket client = fileClientIndex.get(fileName);
                try {
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    out.println(Protocol.REMOVE_COMPLETE_TOKEN);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            log.info(fileName + ": Removed");
        } else {
            fileIndex.replace(fileName, "Removing " + i + " " + r);
            log.info(fileName + ": " + i + "/" + r + " ACKs received");
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                client.setKeepAlive(true);
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String line;
                while ((line = in.readLine() ) != null) {
                    log.info("Received: " + line);
                    String[] args = line.split(" ");
                    switch (args[0]) {
                        case Protocol.JOIN_TOKEN -> storeJoin(args[1], client);
                        case Protocol.LIST_TOKEN -> listFiles(client);
                        case Protocol.STORE_TOKEN -> clientStore(args[1], args[2], client);
                        case Protocol.STORE_ACK_TOKEN -> storeAck(args[1]);
                        case Protocol.LOAD_TOKEN -> clientLoad(args[1], client);
                        case Protocol.RELOAD_TOKEN -> clientLoad(args[1], client);
                        case Protocol.REMOVE_TOKEN -> clientRemove(args[1], client);
                        case Protocol.REMOVE_ACK_TOKEN -> dstoreRmAck(args[1]);
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
        ServerSocket ss = null;
        HashMap<String, String> fileIndex = new HashMap<>();
        HashMap<String, String> fileStoreLookup = new HashMap<>();
        HashMap<String, Socket> storeIndex = new HashMap<>();
        HashMap<String, Socket> fileClientIndex = new HashMap<>();
        Object fileIndexLock = new Object();
        Object fileStoreLock = new Object();
        Object storeIndexLock = new Object();
        Object fileClientLock = new Object();
        int i = 1;
        try {
            ss = new ServerSocket(port);
            while (true) {
                Socket client = ss.accept();
                new Thread(new ControllerHandler(client, replicFactor, timeout, rebalance, i, storeIndex, storeIndexLock, fileIndex, fileIndexLock, fileClientIndex, fileClientLock, fileStoreLookup, fileStoreLock)).start();
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
