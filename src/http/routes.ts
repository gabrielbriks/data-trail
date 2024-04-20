import { FastifyInstance } from "fastify";
import { processNewFileController } from "./controllers/process-new-file-controller";

export async function appRoutes(app: FastifyInstance) {
  app.get("/new-process", await processNewFileController);
}
