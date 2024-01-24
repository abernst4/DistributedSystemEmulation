package edu.yu.cs.com3800.stage5.demo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;

public class Client implements LoggingServer{
        
    public static void main(String[] args){
        int serverPort = Integer.parseInt(args[0]); 
        boolean wait = Boolean.parseBoolean(args[1]); 

        URI checkLeaderUri = null;
        URI PostRequestUri = null; 
        try {
            checkLeaderUri = new URI("http://127.0.0.1:" + serverPort + "/checkleader");
            PostRequestUri = new URI("http://127.0.0.1:" + serverPort + "/compileandrun"); 
        } catch (URISyntaxException e) {} 

        HttpRequest request = HttpRequest.newBuilder()
            .uri(checkLeaderUri)
            .GET()
            .build(); 

        HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build(); 

        if(wait){

            HttpResponse<String> response = null; 
            try {
                response = client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException e) {} 
            
            while(response.body().charAt(0) == 'T'){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}

                try {
                    response = client.send(request, HttpResponse.BodyHandlers.ofString());
                } catch (IOException | InterruptedException e) {} 
            }


            System.out.println(response.body()); 
        }

        
        //let's send 9 client requests
        String validClass = "package edu.yu.cs.com3800.stage3;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        ExecutorService exec = Executors.newCachedThreadPool(); 
        for(int i = 0; i < 9; i++){
            int job_number = i; 
            Runnable task = ()->{
                String msg = validClass.replace("world!", "world! request # " + job_number); 
                URI uri = null; 
                try {
                    uri = new URI("http://127.0.0.1:" + serverPort + "/compileandrun");
                } catch (URISyntaxException e) {}

                HttpRequest req = HttpRequest.newBuilder()
                    .header("content-type", "text/x-java-source")
                    .uri(uri)
                    .POST(HttpRequest.BodyPublishers.ofString(msg))
                    .build(); 
                HttpResponse<String> response = null; 
                try {
                    response = client.send(req, HttpResponse.BodyHandlers.ofString());
                } catch (IOException | InterruptedException e) {} 
                System.out.println(response.body()); 
            };
            exec.submit(task); 
        }

        try{exec.shutdown();}catch(Exception e){}
        try {
            exec.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {}
    }    
}
