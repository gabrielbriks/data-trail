import { randomUUID } from "crypto";
import z, { ZodError } from "zod";
import { sql } from "../lib/postgres";
import { ColunsType } from "../types/coluns-type";

export async function saveRecord(data: ColunsType, idFile: string) {
  try {
    const result = await sql/*sql*/ `
        INSERT INTO records (
          id,
          Trip_ID, 
          Taxi_ID, 
          Trip_Start_Timestamp, 
          Trip_End_Timestamp, 
          Trip_Seconds, 
          Trip_Miles, 
          Fare, 
          Trip_Total, 
          Payment_Type, 
          Company,
          file_id
        ) VALUES (
          ${randomUUID()},
          ${data.trip_id}, 
          ${data.Taxi_ID}, 
          ${data.Trip_Start_Timestamp}, 
          ${data.Trip_End_Timestamp}, 
          ${data.Trip_Seconds}, 
          ${data.Trip_Miles}, 
          ${data.Fare}, 
          ${data.Trip_Total}, 
          ${data.Payment_Type}, 
          ${data.Company}
          ${idFile}
        )
      `;

    console.log("SaveRecords_Result", result);

    return result;
  } catch (error) {
    throw new Error(JSON.stringify(error));
  }
}

const recordSchema = z.array(
  z.object({
    id: z.string().uuid(),
    trip_id: z.string(),
    taxi_id: z.string(),
    trip_start_timestamp: z.string(),
    trip_end_timestamp: z.string(),
    trip_seconds: z.coerce.number(),
    trip_miles: z.coerce.number(),
    fare: z.coerce.number(),
    trip_total: z.coerce.number(),
    payment_type: z.string(),
    company: z.string(),
    created_at: z.string(),
    file_id: z.string().uuid(),
  })
);

export async function saveListRecords(
  listRecords: ColunsType[],
  idFile: string
) {
  try {
    if (!idFile) {
      throw new Error("Not found id_file.");
    }

    const dataList = listRecords.map((item) => {
      const result = {
        id: randomUUID(),
        trip_id: item.trip_id,
        taxi_id: item.Taxi_ID,
        trip_start_timestamp: item.Trip_Start_Timestamp,
        trip_end_timestamp: item.Trip_End_Timestamp,
        trip_seconds: item.Trip_Seconds,
        trip_miles: item.Trip_Miles,
        fare: item.Fare,
        trip_total: item.Trip_Total,
        payment_type: item.Payment_Type,
        company: item.Company,
        created_at: new Date().toISOString(),
        file_id: idFile,
      };
      return result;
    });

    const records = await recordSchema
      .parseAsync(dataList)
      .catch((e: ZodError) => {
        console.error(e);
        return e;
      })
      .then((result) => result);

    if (records instanceof ZodError) {
      throw new Error("ZodErro Parser");
    }

    const result: Array<any> = await sql/*sql*/ `
    INSERT INTO records ${sql(records)} RETURNING id`;

    console.log("SaveRecords_Result \n", result.length);

    return result;
  } catch (error) {
    throw error;
  }
}
