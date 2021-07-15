package com.pubsub.outbound.controller;

import com.pubsub.outbound.model.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

@Controller
public class OutboundCtrl {


	@RequestMapping("/")
	public String homeMethod()
	{
		//System.out.println("Home page Called");
		return "index";		
	}

	// outbound converts the message pojo to pubsub and publish it to topic
	@RequestMapping(value="/samplePublish")
	public String samplePublish(@RequestParam(value="message" , required = false) String msg1, Model m) throws IOException, InterruptedException 	{
		
		GetSet a = new GetSet();
		a.setMessage(msg1);
		
		System.out.println(a.getMessage());
		
		 String projectId = "pubsub-project-318209";
		 String topicId = "topic-workout";
			
		
		TopicName topicName = TopicName.of(projectId, topicId);
	    Publisher publisher = null;
	     
	    String msg= a.getMessage();
	    
	    try {
	      // Create a publisher instance with default settings bound to the topic
	      publisher = Publisher.newBuilder(topicName).build();
	      
	      
	      List<String> messages = Arrays.asList(msg);

	        for (final String message : messages) {
	        	
	        ByteString data = ByteString.copyFromUtf8(message);
	        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build(); 
	        // here where the message is published and consumed by subscription

	        // Once published, returns a server-assigned message id (unique within the topic)
	        ApiFuture<String> future = publisher.publish(pubsubMessage);

	        // Add an asynchronous callback to handle success / failure
	        ApiFutures.addCallback(
	            future,
	            new ApiFutureCallback<String>() {

	              @Override
	              public void onFailure(Throwable throwable) {
	                if (throwable instanceof ApiException) {
	                  ApiException apiException = ((ApiException) throwable);
	                  // details on the API exception
	                  System.out.println(apiException.getStatusCode().getCode());
	                  System.out.println(apiException.isRetryable());
	                }
	                System.out.println("Error publishing message : " + message);
	              }

	              @Override
	              public void onSuccess(String messageId) {
	                // Once published, returns server-assigned message ids (unique within the topic)
	                System.out.println("Published message ID: " + messageId);
	              }
	            },
	            MoreExecutors.directExecutor());
	      }
	    } finally {
	      if (publisher != null) {
	        // When finished with the publisher, shutdown to free up resources.
	        publisher.shutdown();
	        publisher.awaitTermination(1, TimeUnit.MINUTES);
	       
	      }
	    }
		
		
		m.addAttribute("result","Message has been set");
		
		
		return "result";
	}
}
