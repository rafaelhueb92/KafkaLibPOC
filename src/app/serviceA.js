require("dotenv/config");
const express = require("express");
const app = express();
const kafka = require("../utils/kafka.utils")(process.env.kafkaHost);
const { PORT } = process.env;

app.use(express.json());
app.use(require("helmet")());

app.post("/send", async (req, res) => {
  try {
    const { body } = req;
    await kafka.pub("test", JSON.stringify(body));
    return res.send({ message: "Message has sent" });
  } catch (ex) {
    console.error("Error on send", ex);
    return res.status(501).send({ message: "Error on send Message" });
  }
});

app.get("/create/:topic", async (req, res) => {
  try {
    const { topic } = req.params;
    await kafka.includeTopic(topic);
    return res.send({ message: "Topic Has Created" });
  } catch (ex) {
    console.error("Error on send", ex);
    return res.status(501).send({ message: "Error on send Message" });
  }
});

app.listen(PORT, async () => {
  try {
    console.log("Initializing Client Kafka");
    await kafka.init();
    console.log("Client Kafka it's initiated");
    console.log(`Server Running on http://localhost:${PORT}`);
  } catch (ex) {
    console.error("Error on init server", ex);
  }
});
