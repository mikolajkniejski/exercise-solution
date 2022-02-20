# How to run the solution

The solution is added to the docker-compose.yml file. Running the solution is as simply as running `docker-compose up -d`. Docker compose will download an image and run it automatically. 

You can also build the image locally by typing  

`docker build -t <name> .`
`docker run --rm --network host  <name>`

in the solution folder.

You can also run the code without docker. Instructions are porvided in `solutions` folder.  

The solutions container is linked with an image from docker hub. It won't create an image based on the local version. I did that because that is how other containers in this repo are set up.

# Challenge the coder




## Programming exercise for software, robotics and machine vision engineer candidates at Aivero AS

In this challenge you will continue to compute the running maximum in a moving window of 100 numbers length over a stream of 10000 random numbers.

```
# Overview of the desired message flow
our_random_number_generator --> your_running_maximum_program --> our_validator
```

We provide you with 
- a random number generator
- a validator that checks your solution
- a rabbitmq message broker that transports messages between the 3 programs

You are to listen (rabbitmq subscribe) to messages containing a single random number each from our number generator, on every incoming message determine the largest number you have encountered within the last 100 consecutive numbers and send (rabbitmq publish) your result to our validator.


### Task:
Write a RabbitMQ pubsub (publisher/subscriber) program that:
- Connects to the rabbitmq broker spun up by our docker-compose containers on `amqp://0.0.0.0:5672`
- Listens on the rabbitmq topic `rand` to an incoming stream of signed 64-bit integers being published by the `random_number_generator` docker container
- On [every incoming message on the `rand` topic](#rand) computes the running maximum over a moving window of the last 100 consecutive samples.
- Constructs a json as shown in [the solution section](#solution). It should contain your solution (`running_max` value) as well as content of the `rand` message.
- Publishes your message on the `solution` topic.


Think about how this can be made memory and compute efficient.

Add documentation and further adjustments as you see fit.

A rough guide to getting started can be found below.

Have fun, don't stress and good success :)

#### Requirements

* A docker and docker-compose installation. Please see the [docker-compose installation instructions](https://docs.docker.com/compose/install/) for more information.
* A programming language of your choice with a RabbitMQ client (and preferably a JSON library as well). Please refer to the [RabbitMQ Client Libraries and Developer Tools](https://www.rabbitmq.com/devtools.html).

#### Steps:

1. Familiarise yourself with the [rabbitmq topics (data channels) and message format](#topics-and-message-format) you're expected to use.
2. Implement your solution using a programming language of your choice.
3. When you're ready to test your solution, spin up the `docker-compose`. Please see more instructions in [Testing your solution](#testing-your-solution).
4. Open the CSV file placed under *results/results.csv* and check that all fields in the rightmost column are `true`.
5. Send your solution and the .csv to the maintainers.

## Topics and message format

The `docker-compose` in this challenge hosts a RabbitMQ broker, a random number generator and a validator script. These
components use two different topics, that you're expected to consume from and publish to. They are, respectively:

### rand

`rand` - The random number generator publishes messages to this topic. **You are expected to subscribe to and consume
these messages in your program.** The messages are JSON formatted and contain a monotonically increasing serial: `sequence_number` and a random number: `rand` field, like
so:

```json
{
  "sequence_number": 0,
  "rand": 25
}
```

### solution

`solution` - The validator consumes messages from this topic. **You are expected to publish the your results on this
topic.** The messages published to this topic must be JSON formatted and contain a `sequence_number`, `rand` and
`running_max` field. **You should not change the value of `sequence_number` and `rand` in your program.** 
Example message:

```json
{
  "sequence_number": 2373,
  "rand": 25,
  "running_max": 12947712
}
```

## Testing you solution

When you're ready to test your solution, you should use the `docker-compose` provided in the repository. Please run:

```bash
docker-compose up -d # starts the docker-compose services and begins producing messages
# wait a second or two
docker-compose ps # ensure that the rabbitmq and validator services are "up". The number generator will terminate once it has produced its allocated number of messages.
docker-compose logs -f # Check if the rabbitmq broker is up and the other services have `CONNECTED`

# run your own solution - you'll know better what to do here (hopefully)
```

It is a good idea to stop the `docker-compose` when you have tested your script, as it will restart the containers on every boot of your machine.
Unless you specify a `NUMBER_OF_MESSAGES` environment variable in the docker-compose it will produce messages for ever.

Please do so by running:

```bash
docker-compose down
```

### Starting only RabbitMQ

In some cases it can be beneficial to only run RabbitMQ to test your connection. You may do so by running:

```bash
docker-compose up -d rabbitmq
```

## License:
Copyright (C) 2015 General Interfaces GmbH

Copyright (C) 2021 Aivero AS

Maintainer: Raphael Dürscheid

Program: *Challenge the coder*

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
