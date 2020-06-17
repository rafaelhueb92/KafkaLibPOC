require("dotenv/config");
const kafka = require("../utils/kafka.utils")(process.env.kafkaHost,["test"]);

console.log("Listening on Kafka Queue");

kafka.sub((data) => console.log("Message received", data));
