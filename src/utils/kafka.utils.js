const kafka = require("kafka-node");

class Kafka {
  constructor(kafkaHost, topics = []) {
    try {
      const { Consumer, Producer } = kafka;
      this.Client = new kafka.KafkaClient({ kafkaHost });
      this.Producer = new Producer(this.Client);
      this.Consumer = new Consumer(
        this.Client,
        topics.map((v, i) => ({ topic: v, partition: 0 }))
      );
      this.Topics = [];
    } catch (ex) {
      console.error("Error Constructor Kafka", ex);
    }
  }

  async init() {
    try {
      return new Promise((resolve, reject) => {
        this.Client.on("ready", () => {
          console.log("Client is ready");
          return resolve();
        });
        this.Client.on("error", (err) => {
          console.error("Error on open Client", err);
          throw reject(err);
        });
      });
    } catch (ex) {
      console.error("Error on Init Kafka", ex);
      throw ex;
    }
  }

  async pub(topic, messages) {
    try {
      console.log(`Sending to the ${topic}`, messages);
      return await new Promise((resolve, reject) => {
        console.log("Producing is starting");
        console.log("Producer ready and sending");
        this.Producer.send([{ topic, messages }], (err, data) => {
          if (err) throw reject(err);

          return resolve(data);
        });
        this.Producer.on("error", (err) => {
          throw reject(err);
        });
      });
    } catch (ex) {
      console.error("Error on pub", ex);
      throw ex;
    }
  }

  sub(cb) {
    this.Consumer.on("message", (message) => cb(message));
    this.Consumer.on("error", (err) => console.error(err));
  }

  includeTopic(topic) {
    try {
      return new Promise((resolve, reject) => {
        this.Producer.createTopics([topic], (err) => {
          if (err) throw reject(err);
          return resolve();
        });
      });
    } catch (ex) {
      console.error("error on include topic", ex);
      throw ex;
    }
  }
}

module.exports = (kafkaHost, topics) => new Kafka(kafkaHost, topics);
