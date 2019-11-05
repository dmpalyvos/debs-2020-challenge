# DEBS 2019 Grand Challenge HTTP-Client Example Kit

This repository contains example HTTP-client that connects you to DEBS 2020 Grand Challenge Benchmark System.

Please use this repository as a template for your work. The final Benchmark System will mostly the same to one you will test against here.

We use Docker Compose to help you reduce the complexity of integration with the Benchmark System.
Please read the instructions below, to get insight how you can get started.

## About this repository

This repository contains project structure for your implementation.
`dataset` folder should contain your training datasets `in.csv` and `out.csv`.
`solution_app` folder for your implementation.
`docker-compose.yml` - defines the services that run together(HTTP-client against our Benchmarking system).
`Dockerfile.solution` - defines the steps needed to build container with your prediction system (if you decided to use another language than Python, you will need to redefine this file appropriately).

## Before you start

Make sure you have Docker Engine and Docker Compose installed.
You may use the official link below for downloading:
[Download Docker Engine](https://docs.docker.com/get-started/#prepare-your-docker-environment
https://docs.docker.com/compose/install/#install-compose)

Check your installation:
```
  $ docker --version
  $ docker-compose --version
```

## How to get started

You need to implement your solution as HTTP-client. Example solution, written in Python, is already provided in `/solution_app` folder.
You may use this code as it is, we already implemented all basic functionality that you may need. Just plug in your prediction system and you are ready to submit your solution.
However you are free to use any language that suits your needs.

1. Clone this repository
2. Use the project structure provided. Place your `out.csv` and `in.csv` files in `/dataset` folder for the Benchmark System Container to be able to test your prediction accuracy and latency.
3. Implement your HTTP-client as REST web service, that may reach the server via GET and POST requests (you may see an example implementation in `solution_app.py`).

    >Basically this means, that your solution should request data via GET method, and submit your answer via POST method.

    >Use `/data/` path for your requests.

    >For each GET request you will receive a new chunk of data containing various number of tuples.

    >You need to submit your answer for this chunk via POST request.

    >After getting all chunks, your solution should stop upon seeing `404` status code. Now you ready for the next step.

4. Both the final, and Benchmark Systems you will test against, in their environments, will contain `BENCHMARK_SYSTEM_URL`, so make sure you read it in your solution program to be able to connect to to our system.
4. Add the dependencies that your solution program uses to `Dockerfile.solution`.
5. To start the evaluation of your solution run:
      ```
      $ docker-compose up
      ```
6. Check the logs of `'benchmark-grader'` Docker container to see details of your run.
    Use this command:
      ```
      $ docker logs benchmark-system
      ```
7. Make changes to your system if needed.
After any change to your prediction system or HTTP-client, please run these commands:
      ```
      $ docker-compose down
      ```
    This will stop the previous run of benchmark system. Then run:
      ```
      $ docker-compose up --build
      ```
    To rebuild with changes you made.

`Note!`: If you willing to use another language for your development you need to change contents of `Dockerfile.solution` to support language of your choice.
