import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Represents a remote node (end-server or another proxy) in the network topology.
 */
class RemoteServer {
    private final String ip;
    private final int port;
    public boolean isProxy = false;
    public boolean isTcp = true;

    public RemoteServer(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() { return ip; }
    public int getPort() { return port; }
}

/**
 * Multithreaded TCP/UDP proxy server.
 *
 * <p>On startup the proxy connects to every configured backend node, identifies
 * whether it is a plain server or another proxy (via the TYPE command), collects
 * the keys it owns (GET NAMES) and builds a static key→server routing map.
 * Subsequent GET VALUE / SET requests from clients are forwarded transparently
 * to the owning node over TCP or UDP, matching the protocol used during discovery.
 *
 * <p>Usage:
 * <pre>
 *   java Proxy -port &lt;listenPort&gt; -server &lt;ip&gt; &lt;port&gt; [-server &lt;ip&gt; &lt;port&gt; ...]
 * </pre>
 */
public class Proxy {

    private static final int CONNECT_TIMEOUT_MS = 500;
    private static final int FORWARD_TIMEOUT_MS = 2000;

    static List<RemoteServer> serverList = new ArrayList<>();
    static ConcurrentHashMap<String, RemoteServer> keyMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        int port = parseArgs(args);
        if (port == 0 || serverList.isEmpty()) {
            System.err.println("Usage: java Proxy -port <port> -server <ip> <port> [...]");
            return;
        }

        discoverNodes();
        startListeners(port);
    }

    // Argument parsing

    private static int parseArgs(String[] args) {
        int port = 0;
        for (int i = 0; i < args.length; ) {
            switch (args[i]) {
                case "-port":
                    port = Integer.parseInt(args[i + 1]);
                    if (port == 0) throw new IllegalArgumentException("Port cannot be 0.");
                    i += 2;
                    break;
                case "-server":
                    String ip = args[i + 1];
                    int serverPort = Integer.parseInt(args[i + 2]);
                    serverList.add(new RemoteServer(ip, serverPort));
                    i += 3;
                    break;
                default:
                    System.err.println("Unknown parameter: " + args[i]);
                    i++;
            }
        }
        return port;
    }

    // Node discovery — build the key→server map

    private static void discoverNodes() {
        for (RemoteServer server : serverList) {
            System.out.println("Discovering node: " + server.getIp() + ":" + server.getPort());
            String response = null;

            try {
                try (Socket s = new Socket(server.getIp(), server.getPort())) {
                    s.setSoTimeout(CONNECT_TIMEOUT_MS);
                    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
                    BufferedReader in  = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    out.write("TYPE\n");
                    out.flush();
                    String typeResponse = in.readLine();
                    if ("PROXY".equals(typeResponse)) {
                        server.isProxy = true;
                        System.out.println("  -> identified as PROXY");
                    } else {
                        System.out.println("  -> identified as server (TYPE=" + typeResponse + ")");
                    }
                }

                // Collect keys
                try (Socket s = new Socket(server.getIp(), server.getPort())) {
                    s.setSoTimeout(CONNECT_TIMEOUT_MS);
                    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
                    BufferedReader in  = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    out.write("GET NAMES\n");
                    out.flush();
                    response = in.readLine();
                }

                server.isTcp = true;
                System.out.println("  -> connected via TCP");

            } catch (IOException tcpEx) {
                // Fall back to UDP
                server.isTcp = false;
                try {
                    try (DatagramSocket udp = new DatagramSocket()) {
                        udp.setSoTimeout(CONNECT_TIMEOUT_MS);
                        byte[] buf = "GET NAMES \n".getBytes();
                        InetAddress addr = InetAddress.getByName(server.getIp());
                        udp.send(new DatagramPacket(buf, buf.length, addr, server.getPort()));
                        byte[] recvBuf = new byte[1024];
                        DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
                        udp.receive(recvPacket);
                        response = new String(recvPacket.getData(), 0, recvPacket.getLength()).trim();
                    }
                    System.out.println("  -> connected via UDP");
                } catch (IOException udpEx) {
                    System.out.println("  -> unreachable (TCP and UDP both failed)");
                }
            }

            // Parse "OK <count> <key1> <key2> ..." and populate the map
            if (response != null && response.startsWith("OK")) {
                String[] parts = response.split(" ");
                for (int k = 2; k < parts.length; k++) {
                    keyMap.put(parts[k], server);
                    System.out.println("  -> mapped key: " + parts[k]);
                }
            }
        }
    }

    // Start TCP and UDP listeners

    private static void startListeners(int port) {
        ExecutorService udpWorkers = Executors.newCachedThreadPool();

        // UDP listener (dedicated thread)
        new Thread(() -> {
            try (DatagramSocket udpSocket = new DatagramSocket(port)) {
                System.out.println("UDP listener ready on port " + port);
                while (true) {
                    byte[] buffer = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    udpSocket.receive(packet);

                    // Capture immutable packet data before handing off to thread pool
                    final byte[] data   = Arrays.copyOf(packet.getData(), packet.getLength());
                    final InetAddress clientAddr = packet.getAddress();
                    final int clientPort         = packet.getPort();

                    udpWorkers.execute(() -> {
                        String command  = new String(data).trim();
                        String response = processCommand(command);
                        byte[] sendBuf  = response.getBytes();
                        try {
                            udpSocket.send(new DatagramPacket(sendBuf, sendBuf.length, clientAddr, clientPort));
                        } catch (IOException e) {
                            System.err.println("Error sending UDP response: " + e.getMessage());
                        }
                    });
                }
            } catch (IOException e) {
                System.err.println("UDP listener error: " + e.getMessage());
            }
        }, "udp-listener").start();

        // TCP listener (main thread)
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("TCP listener ready on port " + port);
            ExecutorService tcpWorkers = Executors.newCachedThreadPool();
            while (true) {
                Socket clientSocket = serverSocket.accept();
                tcpWorkers.execute(() -> {
                    try (Socket cs = clientSocket;
                         BufferedReader in  = new BufferedReader(new InputStreamReader(cs.getInputStream()));
                         PrintWriter    out = new PrintWriter(cs.getOutputStream(), true)) {
                        String command  = in.readLine();
                        String response = processCommand(command);
                        out.println(response);
                    } catch (IOException e) {
                        System.err.println("TCP client error: " + e.getMessage());
                    }
                });
            }
        } catch (IOException e) {
            System.err.println("TCP listener error: " + e.getMessage());
        }
    }

    // Protocol handler

    private static String processCommand(String command) {
        if (command == null || command.isEmpty()) return "NA";

        String[] parts = command.split(" ");
        String verb = parts[0];
        String key  = null;

        switch (verb) {
            case "TYPE":
                return "PROXY";

            case "GET":
                if (parts.length >= 2 && "NAMES".equals(parts[1])) {
                    return "OK " + keyMap.size() + " " + String.join(" ", keyMap.keySet());
                }
                if (parts.length >= 3 && "VALUE".equals(parts[1])) {
                    key = parts[2];
                }
                break;

            case "SET":
                if (parts.length >= 2) key = parts[1];
                break;

            case "QUIT":
                propagateQuit();
                System.exit(0);
                break;

            default:
                System.err.println("Unknown command: " + verb);
                return "NA";
        }

        if (key == null) return "NA";

        RemoteServer target = keyMap.get(key);
        if (target == null) return "NA";

        return forwardToNode(command, target);
    }

    // Forward a command to a backend node

    private static String forwardToNode(String command, RemoteServer node) {
        if (node.isTcp) {
            try (Socket s = new Socket(node.getIp(), node.getPort())) {
                s.setSoTimeout(FORWARD_TIMEOUT_MS);
                PrintWriter  out = new PrintWriter(s.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                out.println(command);
                String response = in.readLine();
                return response != null ? response : "NA";
            } catch (IOException e) {
                return "NA";
            }
        } else {
            try (DatagramSocket udp = new DatagramSocket()) {
                udp.setSoTimeout(FORWARD_TIMEOUT_MS);
                byte[] buf = (command + " \n").getBytes();
                InetAddress addr = InetAddress.getByName(node.getIp());
                udp.send(new DatagramPacket(buf, buf.length, addr, node.getPort()));
                byte[] recvBuf = new byte[1024];
                DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
                udp.receive(recvPacket);
                return new String(recvPacket.getData(), 0, recvPacket.getLength()).trim();
            } catch (IOException e) {
                return "NA";
            }
        }
    }

    // Propagate QUIT to all downstream nodes

    private static void propagateQuit() {
        System.out.println("Propagating QUIT to all downstream nodes...");
        for (RemoteServer s : serverList) {
            if (s.isTcp) {
                try (Socket sock = new Socket(s.getIp(), s.getPort())) {
                    new PrintWriter(sock.getOutputStream(), true).println("QUIT");
                } catch (IOException e) {
                    System.err.println("Could not send QUIT to " + s.getIp() + " via TCP: " + e.getMessage());
                }
            } else {
                try (DatagramSocket udp = new DatagramSocket()) {
                    byte[] buf = "QUIT \n".getBytes();
                    DatagramPacket p = new DatagramPacket(buf, buf.length,
                            InetAddress.getByName(s.getIp()), s.getPort());
                    udp.send(p);
                } catch (IOException e) {
                    System.err.println("Could not send QUIT to " + s.getIp() + " via UDP: " + e.getMessage());
                }
            }
        }
    }
}
