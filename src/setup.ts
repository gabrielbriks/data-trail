import { sql } from "./lib/postgres";

async function setup() {
  await sql/*sql*/ `
    CREATE TABLE IF NOT EXISTS files (
      id VARCHAR PRIMARY KEY,
      name_file VARCHAR(50),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `;

  await sql/*sql*/ `
    CREATE TABLE IF NOT EXISTS records (
      id VARCHAR PRIMARY KEY,
      Trip_ID VARCHAR NULL,
      Taxi_ID VARCHAR NULL,
      Trip_Start_Timestamp VARCHAR NULL,
      Trip_End_Timestamp VARCHAR NULL,
      Trip_Seconds NUMERIC NULL,
      Trip_Miles DECIMAL NULL,
      Fare DECIMAL NULL,
      Trip_Total DECIMAL NULL,
      Payment_Type VARCHAR NULL,
      Company VARCHAR NULL,
      Created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      file_id VARCHAR REFERENCES files(id)
    )
  `;

  // Create indexes
  await sql/*sql*/ `
   CREATE INDEX idx_trip_id ON records (Trip_ID)
 `;

  await sql/*sql*/ `
   CREATE INDEX idx_taxi_id ON records (Taxi_ID)
 `;

  //Close connection with db
  await sql.end();

  console.log("Setup successfully completed");
}

setup();
