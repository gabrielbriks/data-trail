import fastify from "fastify";
import { processNewFile_v2 } from "./routes";

const app = fastify();

//Route test
app.get("/", () => "Is active!");

app.register(async (fastify) => {
  fastify.get("/new-process", await processNewFile_v2);
});

app
  .listen({
    port: 3333,
    // host: "0.0.0.0",
  })
  .then(() => console.log("HTTP Server Running"))
  .catch(() => console.log("ERROR: Not Server Running"));
