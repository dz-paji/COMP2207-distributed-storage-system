import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class DStore {
    private final int port;
    private final int cport;
    private final int timeout;
    private final String folder;
    private ServerSocket ss;
    private Socket controller;
    private ScreenLogger log = new ScreenLogger("DStore");

  public DStore(int port, int cport, int timeout, String folder) {
    this.port = port;
    this.cport = cport;
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
        try {
            PrintWriter out = new PrintWriter(controller.getOutputStream(), true);
            out.println(Protocol.JOIN_TOKEN + " " + port);
            out.close();

            while (true) {
                Socket client = ss.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String line;
                while ((line = in.readLine() ) != null) {
                    String[] args = line.split(" ");
                    switch (args[0]) {
                        case Protocol.STORE_TOKEN -> storeFile(args[1], args[2], client);
                        case Protocol.LOAD_DATA_TOKEN -> loadFile(args[1], client);
                        case Protocol.REMOVE_TOKEN -> removeFile(args[1], client);
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

    private void removeFile(String fileName, Socket client) {
      log.info("Remove request received for " + fileName);
        File file = new File(folder + "/" + fileName);
        if (!file.exists()) {
            log.error("File does not exist");
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
        try {
            File file = new File(folder + "/" + fileName);
            if (!file.exists()) {
                client.close();
                return;
            }
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            byte[] buffer = new byte[(int) file.length()];
            FileInputStream fIn = new FileInputStream(file);
            fIn.read(buffer);
            fIn.close();
            client.getOutputStream().write(buffer);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void storeFile(String fileName, String fileSize, Socket client) {
        try {
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            out.println(Protocol.ACK_TOKEN);
            byte[] buffer = client.getInputStream().readNBytes(Integer.parseInt(fileSize));
            File outFile = new File(folder + "/" + fileName);
            FileOutputStream fOut = new FileOutputStream(outFile);
            fOut.write(buffer);
            fOut.close();
            out.close();

            PrintWriter outS = new PrintWriter(controller.getOutputStream(), true);
            outS.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args){
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String folder = args[3];
        DStore dstore = new DStore(port, cport, timeout, folder);
        dstore.start();
    }
}
