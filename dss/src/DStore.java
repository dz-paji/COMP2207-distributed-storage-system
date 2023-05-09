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
    private ScreenLogger log = new ScreenLogger("DStore");

    public DStore(int port, int cport, int timeout, String folder){
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.folder = folder;
    }

    public void start() {
        Socket socket = null;
        try {
            InetAddress address = InetAddress.getLocalHost();
            socket = new Socket(address, cport);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(Protocol.JOIN_TOKEN + " " + port);

            while (true) {
                Socket client = ss.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String line;
                while ((line = in.readLine() ) != null) {
                    String[] args = line.split(" ");
                    switch (args[0]) {
                        case Protocol.STORE_TOKEN -> storeFile(args[1], args[2], client);
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
