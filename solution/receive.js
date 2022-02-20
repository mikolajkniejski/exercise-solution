const amqp = require("amqplib");
var JSONbig = require("json-bigint");

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

console.log("working");

async function connect() {
  try {
    const connection = await amqp.connect("amqp://0.0.0.0:5672");
    const channel = await connection.createChannel();

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
    });
  } catch (err) {
    console.error(err);
  }
}

connect();
