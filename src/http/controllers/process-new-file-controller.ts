import { parse } from "csv-parse";
import { formatISO } from "date-fns";
import { FastifyReply, FastifyRequest } from "fastify";
import { createReadStream, createWriteStream } from "fs";
import path from "path";
import { Transform, Writable } from "stream";
import { saveFileData } from "../../business/save-file";
import { saveListRecords } from "../../business/save-records";

export async function processNewFileController(
  req: FastifyRequest,
  reply: FastifyReply
) {
  let startTime: Date;
  let endTime: Date;
  let durationProccesFile: number | null = null;
  let duration: number | null = null;
  let streamClosed = false;
  let filePath = process.env.DIR_FILE_CSV;

  try {
    if (!filePath) {
      throw new Error("File path not found!");
    }

    const csvFilePath = path.resolve(__dirname, filePath);

    const today = new Date();
    const nameOutputFileJson = `output_${formatISO(today)}.json`;
    const outputFileJson = path.resolve(
      __dirname,
      `../../../data-files/${nameOutputFileJson}`
    );

    startTime = new Date();
    console.log("Processando ...");

    const readableStream = createReadStream(csvFilePath);
    const newFileJsonWritableStream = createWriteStream(outputFileJson, {});

    const transformStreamToObject = parse({
      delimiter: ",",
      columns: true,
      groupColumnsByName: true,
      autoParseDate: true,
    });

    const recordsForSave: any[] = [];

    const transformStreamToString = new Transform({
      objectMode: true,
      transform(chunk, econding, callback) {
        const dataString = JSON.stringify(chunk);

        callback(null, dataString);
      },
    });

    const writableStream = new Writable({
      async write(chunk, econding, callback) {
        try {
          const data = JSON.parse(chunk);
          const output = JSON.stringify(
            {
              trip_id: data.Trip_ID,
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
          );

          recordsForSave.push(JSON.parse(output));

          newFileJsonWritableStream.write(output);

          !newFileJsonWritableStream.writableFinished &&
            newFileJsonWritableStream.write(",\n");

          callback(null);
        } catch (error: any) {
          callback(error);
        }
      },
      final(this, callback) {
        //Close array in json
        newFileJsonWritableStream.write("\n]");

        newFileJsonWritableStream.end();
        callback(null);
      },
    });

    readableStream
      .pipe(transformStreamToObject)
      .pipe(transformStreamToString)
      .pipe(writableStream);

    readableStream.on("ready", async () => {
      //open array in json
      newFileJsonWritableStream.write("[\n");
    });

    readableStream.on("close", async () => {
      durationProccesFile = new Date().getTime() - startTime.getTime();
      console.log(
        `Salvando registros no banco... Tempo processamento arquivos: `,
        `${durationProccesFile} ms`
      );

      // Inserir os registros no banco de dados em lotes
      const batchSize =
        recordsForSave.length > 5000
          ? 1000
          : recordsForSave.length < 1000
          ? recordsForSave.length
          : 500;

      const fileResultId = await saveFileData(nameOutputFileJson);

      if (!fileResultId) {
        throw new Error("Registro não existe em [Files]! ");
      }

      for (let i = 0; i < recordsForSave.length; i += batchSize) {
        const batch = recordsForSave.slice(i, i + batchSize);
        await saveListRecords(batch, fileResultId);
      }

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
      message: `
        The file was processed successfully. 
        Time Processing file: ${durationProccesFile || undefined} ms
        Total time process: ${Number(duration) || undefined} ms  `,
    });
  } catch (error) {
    console.log(error);
    return reply.status(400).send(error);
  }
}
