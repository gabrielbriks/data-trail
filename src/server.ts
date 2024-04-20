import { app } from "./app";

app
  .listen({
    port: 3333,
    host: "0.0.0.0",
  })
  .then(() => console.log("HTTP Server Running"))
  .catch(() => console.log("ERROR: Not Server Running"));
