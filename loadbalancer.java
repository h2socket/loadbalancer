
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class loadbalancer {
    private final List<InetSocketAddress> backends;
    private final int port;
    private final AtomicInteger index = new AtomicInteger();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public loadbalancer(List<InetSocketAddress> backends, int port) {
        this.backends = Collections.unmodifiableList(backends);
        this.port = port;
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        while (true) {
            Socket clientSocket = serverSocket.accept();
            executor.submit(() -> handleClient(clientSocket));
        }
    }

    private void handleClient(Socket clientSocket) {
        Socket backendSocket = null;
        try {
            InetSocketAddress backend = selectBackend();
            backendSocket = new Socket(backend.getHostName(), backend.getPort());

            Socket finalBackendSocket = backendSocket;
            executor.submit(() -> pipe(clientSocket, finalBackendSocket));
            executor.submit(() -> pipe(finalBackendSocket, clientSocket));
        } catch (IOException e) {
            try {
                clientSocket.close();
            } catch (IOException ignored) {}
            if (backendSocket != null) {
                try {
                    backendSocket.close();
                } catch (IOException ignored) {}
            }
        }
    }

    private void pipe(Socket inSocket, Socket outSocket) {
        try (InputStream in = inSocket.getInputStream(); OutputStream out = outSocket.getOutputStream()) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                out.flush();
            }
        } catch (IOException ignored) {}
        try {
            inSocket.close();
            outSocket.close();
        } catch (IOException ignored) {}
    }

    private InetSocketAddress selectBackend() {
        int i = Math.abs(index.getAndIncrement() % backends.size());
        return backends.get(i);
    }

    public static void main(String[] args) throws IOException {
        List<InetSocketAddress> backends = Arrays.asList(
            new InetSocketAddress("localhost", 9001),
            new InetSocketAddress("localhost", 9002),
            new InetSocketAddress("localhost", 9003)
        );
        new loadbalancer(backends, 8080).start();
    }
}
