import java.io.*;
import java.net.*;

public class Dstore {
  private final int port;
  private final int timeout;
  private final String folder;
  private ServerSocket ss;
  private Socket controller;
  private ScreenLogger log = new ScreenLogger("DStore");

  public Dstore(int port, int cport, int timeout, String folder) {
    this.port = port;
    this.timeout = timeout;
    this.folder = folder;
    try {
      this.ss = new ServerSocket(port);
      InetAddress address = InetAddress.getLocalHost();
      this.controller = new Socket(address, cport);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void start() {
    log.info("Starting DStore");
    try {
      PrintWriter out = new PrintWriter(controller.getOutputStream(), true);
      out.println(Protocol.JOIN_TOKEN + " " + port);
      // Thread listening to controller
      Thread c =
          new Thread(
              () -> {
                try {
                  BufferedReader in =
                      new BufferedReader(new InputStreamReader(controller.getInputStream()));
                  String line;
                  while ((line = in.readLine()) != null) {
                    String[] args = line.split(" ");
                    switch (args[0]) {
                      case Protocol.REMOVE_TOKEN -> removeFile(args[1], controller);
                      default -> {
                        log.error("Invalid command from controller");
                        log.error(line);
                      }
                    }
                  }
                } catch (IOException e) {
                  e.printStackTrace();
                }
              });
      c.start();

      // Listen for clients
      while (true) {
        Socket client = ss.accept();
        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));

        Thread t =
            new Thread(
                () -> {
                  String line;
                  try {
                    while ((line = in.readLine()) != null) {
                      String[] args = line.split(" ");
                      switch (args[0]) {
                        case Protocol.STORE_TOKEN -> storeFile(args[1], args[2], client);
                        case Protocol.LOAD_DATA_TOKEN -> loadFile(args[1], client);
                        default -> {
                          log.error("Invalid command from client");
                          log.error(line);
                        }
                      }
                    }
                  } catch (SocketTimeoutException e) {
                    log.error("Client r/w timed out on port: " + client.getPort());
                  } catch (SocketException e) {
                    if (e.getMessage().equals("Socket closed")) {
                      log.info("Client disconnected");
                    } else {
                      e.printStackTrace();
                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                });
        t.start();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void removeFile(String fileName, Socket client) {
    log.info("Remove request received for " + fileName);
    File file = new File(folder + "/" + fileName);
    if (!file.exists()) {
      log.error("File does not exist");
      try {
        PrintWriter out = new PrintWriter(controller.getOutputStream(), true);
        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return;
    }
    if (file.delete()) {
      log.info(fileName + " deleted.");
      try {
        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
        out.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      log.error(fileName + "delete failed.");
    }
  }

  private void loadFile(String fileName, Socket client) {
    log.info("Load request received for " + fileName);
    try {
      client.setSoTimeout(timeout);
      File file = new File(folder + "/" + fileName);
      if (!file.exists()) {
        log.warn(fileName + ": file not found");
        client.close();
        return;
      }
      byte[] buffer = new byte[(int) file.length()];
      FileInputStream fIn = new FileInputStream(file);
      fIn.read(buffer);
      fIn.close();
      client.getOutputStream().write(buffer);
      log.info(fileName + ": sent to client");
    } catch (SocketTimeoutException e) {
        log.error("Client r/w timed out during load operation");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void storeFile(String fileName, String fileSize, Socket client) {
    log.info("Store request received for " + fileName);
    try {
      PrintWriter out = new PrintWriter(client.getOutputStream(), true);
      out.println(Protocol.ACK_TOKEN);
      log.info("Ready for file " + fileName);
      byte[] buffer = client.getInputStream().readNBytes(Integer.parseInt(fileSize));
      client.setSoTimeout(timeout);
      log.debug(buffer.length + " bytes received");
      File outFile = new File(folder + "/" + fileName);
      FileOutputStream fOut = new FileOutputStream(outFile);
      fOut.write(buffer);
      fOut.close();

      PrintWriter outS = new PrintWriter(controller.getOutputStream(), true);
      outS.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
      log.info(fileName + ": stored. Notify controller");
    } catch (SocketTimeoutException e) {
      log.error("Client r/w timed out during store operation");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    int port = Integer.parseInt(args[0]);
    int cport = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    String folder = args[3];
    File dir = new File(folder);
    dir.mkdir();
    Dstore dstore = new Dstore(port, cport, timeout, folder);
    dstore.start();
    ScreenLogger.log("DStore started");
  }
}
