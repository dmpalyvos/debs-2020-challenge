# DEBS 2020 Grand Challenge HTTP-Client Example Kit

This repository contains an example HTTP-client that connects you to the DEBS 2020 Grand Challenge Benchmark System.

Please use this repository as a template for your work. The final Benchmark System will be mostly the same to one you will test against here.

We use Docker Compose to help you reduce the complexity of integration with the Benchmark System.
Please read the instructions below to get an insight about how you can get started.

## About this repository

This repository contains the project structure for your implementation.
`dataset` folder should contain your training datasets `in.csv` and `out.csv`. You can find the links in the call webpage.
`solution_app` is the folder for your implementation.
`docker-compose.yml` - defines the services that run together (HTTP-client against our Benchmarking system).
`Dockerfile.solution` - defines the steps needed to build the container with your solution (if you decided to use another language than Python, you will need to redefine this file appropriately).

## Before you start

Make sure you have Docker Engine and Docker Compose installed.
You may use the official link below for downloading:

[Download Docker Engine](https://docs.docker.com/get-started/#prepare-your-docker-environment)

[Download Docker Compose](https://docs.docker.com/compose/install/#install-compose)

Check your installation:

```bash
  docker --version
  docker-compose --version
```

## How to get started

You need to implement your solution as an HTTP-client. A sample solution, written in Python, is already provided in the `/solution_app` folder.
However you are free to use any language that suits your needs.

1. Clone this repository.
1. Use the project structure provided. Place your `out.csv` and `in.csv` files in `/dataset` folder for the Benchmark System Container to be able to evaluate your solution.
1. Implement your HTTP-client as REST web service, that may reach the server via GET and POST requests (you may see an example implementation in `solution_app.py`).

    - This means that your solution should request data via a GET method, and submit your answer via a POST method.

    - Use the `/data/` path for your requests.

    - For each GET request you will receive a new chunk of data containing various number of tuples.

    - You need to submit your answer for this chunk via POST request.

    - After getting all chunks, your solution should stop upon seeing `404` status code. Now you ready for the next step.

1. Both the final, and Benchmark Systems you will test against, in their environments, will contain `BENCHMARK_SYSTEM_URL`, so make sure you read it in your solution program to be able to connect to to our system.
1. Add the dependencies that your solution program uses to `Dockerfile.solution`.
1. To start the evaluation of your solution run:

      ```bash
      docker-compose up
      ```

1. Check the logs of `'benchmark-grader'` Docker container to see details of your run.
    Use this command:

      ```bash
      docker logs benchmark-system
      ```

1. Make changes to your system if needed.
After any change to your prediction system or HTTP-client, please run these commands:

      ```bash
      docker-compose down
      ```

    This will stop the previous run of benchmark system. Then run:

      ```bash
      docker-compose up --build
      ```

    To rebuild with changes you made.

`Note!`: If you want to use another language for your development, you need to change the content of `Dockerfile.solution` to support language of your choice.


## Standalone testing

If you want to test your solution outside docker (e.g., to speed up development in the initial stages) you can do so as follows.

Build the grader container by running:

```bash
docker build -f Dockerfile.grader -t grader .
```

Start the grader container and forward port 80:

```bash
docker run -p 8080:80 grader
```

After that, your solution should be able to access the grader exactly as it does when running in a container. Note that you will need to restart the grader container between consequent invocations of your solution application.
