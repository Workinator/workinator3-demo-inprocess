package com.allardworks.workinator3.demo.inprocess;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.allardworks.workinator3")
public class Workinator3DemoInprocessApplication {
	public static void main(String[] args) {
		SpringApplication.run(Workinator3DemoInprocessApplication.class, args);
	}
}
