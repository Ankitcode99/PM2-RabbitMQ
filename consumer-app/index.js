// consumer.js
const amqp = require('amqplib');


let workstats = {};

setInterval(() => {
    console.log('workstats - ', workstats);
}, 10000)

async function startConsumer() {
  try {
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue(process.env.QUEUE_NAME, { durable: true });
    channel.prefetch(1);

    console.log(`Consumer ${process.pid} is waiting for messages`);

    channel.consume(process.env.QUEUE_NAME, async (msg) => {
      const content = JSON.parse(msg.content.toString());
      console.log(new Date(),`  Consumer ${process.pid} received:`, content);
      if(workstats[process.pid]) {
        workstats[process.pid] = workstats[process.pid] + 1;
      } else {
        workstats[process.pid] = 0;
      }

      // Process the message based on its type
      let result;
      switch (content.randomElement) {
        case 'PRIME_CHECK':
          result = isPrime(content.randomNumber);
          break;
        case 'FIBONACCI_CHECK':
          result = isFibonacci(content.randomNumber);
          break;
        case 'ARMSTRONG_CHECK':
          result = isArmstrong(content.randomNumber);
          break;
      }

      console.log(new Date(),`  Consumer ${process.pid} processed:`, result);

      channel.ack(msg);
    }, { noAck: false });
  } catch (error) {
    console.error('Error in consumer:', error);
    process.exit(1);
  }
}

// Helper functions
function isPrime(num) {
  for (let i = 2; i <= Math.sqrt(num); i++) {
    if (num % i === 0) return false;
  }
  return num > 1;
}

function isFibonacci(n) {
    if (n < 0) return false;  

    let a = 0, b = 1;  
    while (b < n) {  
  
     let temp = a;  
  
     a = b;  
  
     b = temp + b;  
  
    }  
    return b === n; 
}

function isArmstrong(num) {
  const digits = num.toString().split('').map(Number);
  const power = digits.length;
  const sum = digits.reduce((acc, digit) => acc + Math.pow(digit, power), 0);
  return sum === num;
}

startConsumer();

// 18:31:59.079 - 18:31:51.486
// 18:35:56.929 - 18:35:55.171