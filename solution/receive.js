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

// After each error, connect() will wait 5 sec and restart. It's a poor approach
// and could be improved by setting a different flows for errors coming from
// not ready connection and errors coming from operations on objects.

(async function connect() {
  amqp
    .connect("amqp://0.0.0.0:5672")
    .then((con) => con.createChannel())
    .then((channel) => {
      channel.assertQueue("rand", { durable: false });
      console.log("Connected!");
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
    })
    .catch((err) => {
      console.log("Error connecting, trying again in 5 sec");
      console.log(err);
      setTimeout(() => {
        connect();
      }, 1000);
    });
})();
