const amqp = require('amqplib');
const dotenv = require('dotenv');
dotenv.config();
let channel;

// Connect to RabbitMQ
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertQueue(process.env.QUEUE_NAME, { durable: true });
  } catch (error) {
    fastify.log.error('Error connecting to RabbitMQ:', error);
    process.exit(1);
  }
}

connectRabbitMQ().then(()=>{
    console.log(`Connected to RabbitMQ`)
    let operations = ['PRIME_CHECK', 'FIBONACCI_CHECK', 'ARMSTRONG_CHECK'];
    setInterval(()=>{
        for(let i=0;i<50;i++) {
            const randomNumber = Math.floor(Math.random() * 101) + 100; // generates a random number between 100-200  
    
            const randomElement = operations[Math.floor(Math.random() * operations.length)];
    
            const message = JSON.stringify({ randomNumber, randomElement });
            console.log(new Date(),`   Sending message: ${message}`);
            channel.sendToQueue(process.env.QUEUE_NAME, Buffer.from(message), { persistent: true });
        }
        console.log('\n','-'.repeat(20), '\n\n');
    }, 2000)
    
})