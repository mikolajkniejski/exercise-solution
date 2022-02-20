const amqp = require("amqplib");
var JSONbig = require("json-bigint");

function sleep(milliseconds) {
  const date = Date.now();
  let currentDate = null;
  do {
    currentDate = Date.now();
  } while (currentDate - date < milliseconds);
}

class Deque {
  constructor() {
    this.queue = [];
    this.window = 100;
  }

  push(num) {
    this.queue.push(num);
  }

  lookup() {
    return this.queue[this.queue.length - 1];
  }
  pop() {
    this.queue.pop();
  }
  shift() {
    this.queue.shift();
  }

  high() {
    return this.queue[0].rand;
  }
}

dq = new Deque();

async function connect() {
  async function connection_handler() {
    while (true) {
      try {
        return await amqp.connect("amqp://0.0.0.0:5672");
      } catch (err) {
        console.log("Couldn't connect, retrying in 5 s.");
        sleep(5000);
      }
    }
  }

  const connection = await connection_handler();
  console.log("Connected!");

  try {
    const channel = await connection.createChannel();
    console.log("Created Channel!");

    channel.consume("rand", (message) => {
      let { rand, sequence_number } = JSON.parse(message.content.toString());

      const bigRand = BigInt(rand);

      // Javascript can not handle numbers bigger than 2^53 - 1 and we have to use BigInt type.
      // If we didn't do that, JS would approximate each number and reaplce three
      // or four last digits with zeros and some comparisons might fail.

      while (bigRand > dq.lookup()?.rand) {
        dq.pop();
      }
      dq.push({ rand: bigRand, sequence_number });

      if (dq.queue[0].sequence_number <= sequence_number - dq.window) {
        dq.shift();
      }

      let msg = {
        sequence_number,
        rand: bigRand,
        running_max: dq.high(),
      };

      msg = JSONbig.stringify(msg);
      // Standard JSON module can not stringify BigInts and so it's necessary to use a different one.

      channel.sendToQueue("solution", Buffer.from(msg));
      channel.ack(message);
    });
  } catch (err) {
    console.log(err);
  }
}

connect();
