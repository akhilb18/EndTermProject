# EndTermProject
This Repository is for End Term Project of Cloud

Requirements :
Maven
Java8
Docker
Docker Compose

1. Build Package of Java Project
	mvn clean package

2. Building Docker Image
	docker build -t end-term .

3. Start Docker Containers for Streaming
	docker-compose up -d

4. Stop Docker Containers
	docker-compose down