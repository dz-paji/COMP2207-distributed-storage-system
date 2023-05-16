import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Controller {
  private final int replicFactor;
  private final int timeout;
  private final int rebalance;
  private final ScreenLogger log;
  private final HashMap<String, Socket> storeIndex;
  private final Object storeIndexLock;
  private final HashMap<String, String> fileIndex;
  private final Object fileIndexLock;
  private final HashMap<String, Socket> fileClientIndex;
  private final Object fileClientLock;
  private final HashMap<String, String> fileStoreLookup;
  private final Object fileStoreLock;
  private final HashMap<String, String> fileLoadLookup;
  private final Object fileLoadLock;
  private LinkedHashMap<String, Integer> storeFileCount;
  private final Object storeFileCountLock;
  private final HashMap<String, CountDownLatch> fileCountdown;
  private final Object fileCountdownLock;
  private final HashMap<String, CountDownLatch> removeCountdown;
    private final Object removeCountdownLock;
  public ServerSocket ss;

  public Controller(int replicFactor, int timeout, int rebalance, int port) {
    this.replicFactor = replicFactor;
    this.timeout = timeout;
    this.rebalance = rebalance;
    this.log = new ScreenLogger("Controller");
    this.storeIndex = new HashMap<>();
    this.storeIndexLock = new Object();
    this.fileIndex = new HashMap<>();
    this.fileIndexLock = new Object();
    this.fileClientIndex = new HashMap<>();
    this.fileClientLock = new Object();
    this.fileStoreLookup = new HashMap<>();
    this.fileStoreLock = new Object();
    this.fileLoadLookup = new HashMap<>();
    this.fileLoadLock = new Object();
    this.storeFileCount = new LinkedHashMap<>();
    this.storeFileCountLock = new Object();
    this.fileCountdown = new HashMap<>();
    this.fileCountdownLock = new Object();
    this.removeCountdown = new HashMap<>();
    this.removeCountdownLock = new Object();
    try {
      ss = new ServerSocket(port);
      //            ss.setSoTimeout(timeout);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Handle join request from dstore JOIN <port>
   *
   * @param port - port number
   * @param client - dstore socket
   */
  private void storeJoin(String port, Socket client) {
    log.info("JOIN request received from " + port);
    synchronized (storeIndexLock) {
      storeIndex.put(port, client);
    }

    synchronized (storeFileCountLock) {
      storeFileCount.put(port, 0);
    }
  }

  /**
   * Handle list request from client LIST
   *
   * @param client - client socket
   */
  public void listFiles(Socket client) {
    log.info("LIST request received");
    synchronized (storeIndexLock) {
      if (storeIndex.size() == 0) {
        log.error("No stores joined but LIST request received");
        try {
          PrintWriter out = new PrintWriter(client.getOutputStream(), true);
          out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } catch (IOException e) {
          e.printStackTrace();
        }
        return;
      }
    }
    try {
      PrintWriter out = new PrintWriter(client.getOutputStream(), true);
      StringBuilder outLine = new StringBuilder(" ");
      synchronized (fileIndexLock) {
        Iterator<Map.Entry<String, String>> iterator = fileIndex.entrySet().iterator();
        log.debug("Total of " + fileIndex.size() + " files stored");
        while (iterator.hasNext()) {
          Map.Entry<String, String> entry = iterator.next();
          if (entry.getValue().startsWith("Stored")) {
            outLine.append(entry.getKey()).append(" ");
          }
          log.debug(entry.getKey() + ": status " + entry.getValue());
        }
      }
      if (outLine.length() > 0) {
        outLine.deleteCharAt(outLine.length() - 1);
      }
      out.println(Protocol.LIST_TOKEN + outLine);
      log.debug("Files listed are: " + outLine);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Handle store request from client STORE <filename> <size>
   *
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
      log.warn(
          file
              + ": Insufficient dstore. "
              + storePoolSize
              + " available, "
              + replicFactor
              + " required");
      return;
    }

    // update file index
    synchronized (fileIndexLock) {
      fileIndex.putIfAbsent(file, "Storing 0 " + replicFactor);
      // <FileName, "Storing i j"> where i is the No. of dstore acked, j is the number of acks
      // needed
    }
    synchronized (fileClientLock) {
      fileClientIndex.putIfAbsent(file, client);
    }

    sortStoreFileCount();
    ArrayList<String> portsArray;
    synchronized (storeFileCountLock) {
      portsArray = new ArrayList<>(storeFileCount.keySet());
    }

    StringBuilder ports = new StringBuilder();
    for (int i = 0; i < replicFactor; i++) {
      ports.append(portsArray.get(i)).append(" ");
    }
    if (ports.length() > 0) ports = new StringBuilder(ports.substring(0, ports.length() - 1));
    try {
      PrintWriter out = new PrintWriter(client.getOutputStream(), true);
      out.println(Protocol.STORE_TO_TOKEN + " " + ports);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // timeout for store ack
    CountDownLatch latch = new CountDownLatch(replicFactor);
    boolean isComplete = false;
    synchronized (fileCountdownLock) {
      fileCountdown.put(file, latch);
    }
    try {
      isComplete = latch.await(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.error("Latch interrupted while waiting for dstore acks.");
    }
    if (isComplete) {
      log.debug("All dstore acks received for " + file);
      registerFile(ports.toString(), file, fileSize);
    } else {
      log.warn("Timeout while waiting for dstore acks of " + file);
      synchronized (fileIndexLock) {
        fileIndex.remove(file);
      }
    }
  }

  private void registerFile(String ports, String fileName, String fileSize) {
    synchronized (fileStoreLock) {
      fileStoreLookup.put(fileName, fileSize + " " + ports);
    }
    synchronized (fileClientLock) {
      try {
        Socket clientSocket = fileClientIndex.get(fileName);

        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println(Protocol.STORE_COMPLETE_TOKEN);

      } catch (IOException e) {
        e.printStackTrace();
      }
      fileClientIndex.remove(fileName);
    }
  }

  private void sortStoreFileCount() {
    synchronized (storeFileCountLock) {
      storeFileCount =
          storeFileCount.entrySet().stream()
              .sorted(Map.Entry.comparingByValue())
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      Map.Entry::getValue,
                      (oldVal, newVal) -> oldVal,
                      LinkedHashMap::new));
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
    boolean stored = false;
    boolean doCountdown = false;
    synchronized (fileIndexLock) {
      if (fileIndex.containsKey(fileName) && fileIndex.get(fileName).startsWith("Storing")) {
        doCountdown = true;
        String[] status = fileIndex.get(fileName).split(" ");
        int i = Integer.parseInt(status[1]);
        int r = Integer.parseInt(status[2]);
        i++;
        if (i == r) {
          fileIndex.replace(fileName, "Stored");
          stored = true;
          log.debug(fileName + ": is now stored");
        } else {
          fileIndex.replace(fileName, "Storing " + i + " " + r);
          log.info(fileName + ": " + i + "/" + r + " ACKs received");
        }
      } else {
        log.error(fileName + ": Non-pending file received ACK");
      }
    }

    if (doCountdown) {
      synchronized (fileCountdownLock) {
        fileCountdown.get(fileName).countDown();
      }
    }

  }

  private void clientLoad(String fileName, Socket client, boolean isFresh) {
    if (isFresh) {
      log.info("LOAD request received for " + fileName);
      synchronized (fileLoadLock) {
        fileLoadLookup.remove(fileName);
      }
    } else {
      log.info("RELOAD request received for " + fileName);
    }

    synchronized (fileIndexLock) {
      if (!fileIndex.containsKey(fileName)) {
        try {
          PrintWriter out = new PrintWriter(client.getOutputStream(), true);
          out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } catch (IOException e) {
          e.printStackTrace();
        }
        log.error(fileName + ": File does not exist");
        return;
      }
    }

    int i = 0;
    int r = numDstoresHasFile(fileName);
    int fileSize;
    List<String> dstores = dstoresHasFile(fileName);

    synchronized (fileStoreLock) {
      fileSize = Integer.parseInt(fileStoreLookup.get(fileName).split(" ")[0]);
    }

    boolean isStored = false;
    synchronized (fileIndexLock) {
      if (fileIndex.get(fileName).startsWith("Stored")) {
        isStored = true;
      } else {
        try {
          PrintWriter out = new PrintWriter(client.getOutputStream(), true);
          out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);

        } catch (IOException e) {
          e.printStackTrace();
        }
        log.error(fileName + ": Invalid state for loading. Sent file not found exception");
        return;
      }
    }

    if (isStored) {
      synchronized (fileLoadLock) {
        if (fileLoadLookup.containsKey(fileName)) {
          i = Integer.parseInt(fileLoadLookup.get(fileName).split(" ")[1]);
          i++;
        } else {
          fileLoadLookup.put(fileName, "Loading 0 " + r);
        }

        if (i == r) { // check if there's more dstore to try
          // no more dstore to try

          if (isFresh) { // this is a fresh serve
            fileLoadLookup.replace(fileName, "Loading 1 " + r);
            try {
              PrintWriter out = new PrintWriter(client.getOutputStream(), true);
              out.println(Protocol.LOAD_FROM_TOKEN + " " + dstores.get(0) + " " + fileSize);
            } catch (IOException e) {
              e.printStackTrace();
            }
            log.info(fileName + ": " + Protocol.LOAD_FROM_TOKEN + " token sent back");
            log.debug("LOAD FROM token served. Note: A fresh serve with Loading record uncleared. Is this a concurrency situation?");
          } else {
            // genuinely no more dstore to try
            try {
              PrintWriter out = new PrintWriter(client.getOutputStream(), true);
              out.println(Protocol.ERROR_LOAD_TOKEN);
            } catch (IOException e) {
              e.printStackTrace();
            }
            fileLoadLookup.remove(fileName);
            log.error(fileName + ": Load failed. No more dstore to try");
          }
        } else {
          fileLoadLookup.replace(fileName, "Loading " + i + " " + r);
          try {
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            out.println(Protocol.LOAD_FROM_TOKEN + " " + dstores.get(i) + " " + fileSize);
          } catch (IOException e) {
            e.printStackTrace();
          }
          log.info(fileName + ": " + Protocol.LOAD_FROM_TOKEN + " token sent back");
          log.debug(
              "whole token is " + Protocol.LOAD_FROM_TOKEN + " " + dstores.get(i) + " " + fileSize);
        }
      }
    }
  }

  /**
   * Remove file from dstore
   *
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

    int dstores = numDstoresHasFile(fileName);
    List<String> dstorePorts = dstoresHasFile(fileName);

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
    CountDownLatch latch = new CountDownLatch(dstores);
    boolean isComplete = false;
    synchronized (removeCountdownLock) {
        removeCountdown.put(fileName, latch);
    }
    try {
      isComplete = latch.await(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.error("Latch interrupted while waiting for REMOVE ack");
    }
    if (!isComplete) {
      log.error(fileName + ": REMOVE timed out");
      synchronized (fileClientIndex) {
        fileClientIndex.remove(fileName);
      }
    }
  }

  private int numDstoresHasFile(String fileName) {
    int dstores;
    synchronized (fileStoreLock) {
      dstores = fileStoreLookup.get(fileName).split(" ").length - 1;
    }
    return dstores;
  }

  private List<String> dstoresHasFile(String fileName) {
    List<String> dstorePorts;
    synchronized (fileStoreLock) {
      dstorePorts =
          new LinkedList<>(Arrays.stream(fileStoreLookup.get(fileName).split(" ")).toList());
    }
    dstorePorts.remove(0);
    return dstorePorts;
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
      synchronized (removeCountdownLock) {
        removeCountdown.get(fileName).countDown();
      }
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
      synchronized (removeCountdownLock) {
        removeCountdown.get(fileName).countDown();
      }
      synchronized (fileIndexLock) {
        fileIndex.replace(fileName, "Removing " + i + " " + r);
      }
      log.info(fileName + ": " + i + "/" + r + " ACKs received");
    }
  }

  public void start() {
    while (true) {
      try {
        Socket client = ss.accept();
        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));

        Thread t =
            new Thread(
                () -> {
                  try {
                    String line;
                    while ((line = in.readLine()) != null) {
                      String[] args = line.split(" ");

                      int dstores;
                      synchronized (storeIndexLock) {
                        dstores = storeIndex.size();
                      }
                      if (dstores == 0) {
                        log.error("No dstore joined");
                        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        return;
                      }

                      switch (args[0]) {
                        case Protocol.JOIN_TOKEN -> storeJoin(args[1], client);
                        case Protocol.LIST_TOKEN -> listFiles(client);
                        case Protocol.STORE_TOKEN -> clientStore(args[1], args[2], client);
                        case Protocol.STORE_ACK_TOKEN -> storeAck(args[1]);
                        case Protocol.LOAD_TOKEN -> clientLoad(args[1], client, true);
                        case Protocol.RELOAD_TOKEN -> clientLoad(args[1], client, false);
                        case Protocol.REMOVE_TOKEN -> clientRemove(args[1], client);
                        case Protocol.REMOVE_ACK_TOKEN -> dstoreRmAck(args[1]);
                        default -> {
                          log.error("Invalid Token");
                          log.error(line);
                        }
                      }
                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                });
        t.start();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    int port = Integer.parseInt(args[0]);
    int replicFactor = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    int rebalance = Integer.parseInt(args[3]);
    Controller ctler = new Controller(replicFactor, timeout, rebalance, port);
    ctler.start();
  }
}
