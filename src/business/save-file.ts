import { randomUUID } from "crypto";
import { sql } from "../lib/postgres";

export async function saveFileData(nameFile: string) {
  try {
    const id = randomUUID().toString();
    const today = new Date();

    const result = await sql/*sql*/ `
      INSERT INTO files (id, name_file, created_at)
      VALUES (
        ${id},
        ${nameFile},
        ${today}
      )
      RETURNING id
    `;

    const file = result[0];

    if (!file || !file?.id) {
      throw new Error(
        'Error when trying to save a new record in the "files" table '
      );
    }

    // console.log("-------------------- File: ", file);

    return file.id;
  } catch (error) {
    throw error;
  }
}
