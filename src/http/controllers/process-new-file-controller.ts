import { parse } from "csv-parse";
import { formatISO } from "date-fns";
import { FastifyReply, FastifyRequest } from "fastify";
import { createReadStream, createWriteStream } from "fs";
import path from "path";
import { Transform, Writable } from "stream";
import { ColunsType } from "../../types/coluns-type";

export async function processNewFileController(
  req: FastifyRequest,
  reply: FastifyReply
) {
  let startTime: Date;
  let endTime: Date;
  let duration: number | null = null;
  let streamClosed = false;

  try {
    const csvFilePath = path.resolve(
      __dirname,
      "../../../data-files/TaxiTrips2_2024.csv"
    );

    const today = new Date();
    const outputFileJson = path.resolve(
      __dirname,
      `../../../data-files/output_${formatISO(today)}.json`
    );

    startTime = new Date();
    console.log("Processando ...");

    const readableStream = createReadStream(csvFilePath);
    const newFileJsonWritableStream = createWriteStream(outputFileJson, {});

    newFileJsonWritableStream.write("[\n");

    const transformStreamToObject = parse({
      delimiter: ",",
      columns: true,
      groupColumnsByName: true,
      autoParseDate: true,
    });

    const transformStreamToString = new Transform({
      objectMode: true,
      transform(chunk, econding, callback) {
        const dataString = JSON.stringify(chunk);

        callback(null, dataString);
      },
    });

    const writableStream = new Writable({
      write(chunk, econding, callback) {
        const data: ColunsType = JSON.parse(chunk);
        const output = JSON.stringify(
          {
            Trip_ID: data.Trip_ID,
            Taxi_ID: data.Taxi_ID,
            Trip_Start_Timestamp: data.Trip_Start_Timestamp,
            Trip_End_Timestamp: data.Trip_End_Timestamp,
            Trip_Miles: data.Trip_Miles,
            Trip_Seconds: data.Trip_Seconds,
            Fare: data.Fare,
            Payment_Type: data.Payment_Type,
            Trip_Total: data.Trip_Total,
            Company: data.Company,
          },
          null,
          2
        ).concat(",");

        newFileJsonWritableStream.write(output);
        callback(null);
      },
    });

    readableStream
      .pipe(transformStreamToObject)
      .pipe(transformStreamToString)
      .pipe(writableStream);

    readableStream.on("close", async () => {
      await newFileJsonWritableStream.write("\n]");
      await newFileJsonWritableStream.end();

      endTime = new Date();
      duration = Number(endTime.getTime() - startTime.getTime());
      console.log("Finalizado: ", `${duration} ms`);
      streamClosed = true;
    });

    // Aguardar até que readableStream seja fechado
    const waitForStreamClose = () => {
      return new Promise((resolve) => {
        const check = () => {
          if (streamClosed) {
            resolve(streamClosed);
          } else {
            setTimeout(check, 100);
          }
        };
        check();
      });
    };

    await waitForStreamClose();

    reply.send({
      message: `The file was processed successfully. \n Total Time of Processing: ${
        Number(duration) || undefined
      } ms `,
    });
  } catch (error) {
    console.log(error);
    return reply.send(JSON.stringify(error));
  }
}
