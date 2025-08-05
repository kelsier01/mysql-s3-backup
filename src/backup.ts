import { PutObjectCommand, S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { createReadStream, unlink, statSync, writeFileSync } from "fs";
import { createConnection } from "mysql2/promise";
import { createGzip } from "zlib";
import { promisify } from "util";
import { pipeline } from "stream";
import { Readable } from "stream";
import { env } from "./env";

const pipelineAsync = promisify(pipeline);

const isDebug = () => {
  return env.DEBUG && env.DEBUG === '1';
};

const uploadToS3 = async (file: { name: string, path: string }): Promise<void> => {
  const bucket = env.AWS_S3_BUCKET;
  const clientOptions: S3ClientConfig = {
    region: env.AWS_S3_REGION,
    forcePathStyle: true,
  };

  console.log(`Uploading backup to S3 at ${bucket}/${file.name}...`);

  if (env.AWS_S3_ENDPOINT) {
    console.log(`Using custom endpoint: ${env.AWS_S3_ENDPOINT}`);

    clientOptions['endpoint'] = env.AWS_S3_ENDPOINT;
  }

  const client = new S3Client(clientOptions);

  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: file.name,
      Body: createReadStream(file.path),
    })
  );
}

const getDatabaseList = async (connection: any): Promise<string[]> => {
  const databasesToExclude = ['mysql', 'sys', 'performance_schema', 'information_schema', 'innodb'];

  if (env.BACKUP_DATABASE_NAME) {
    return [env.BACKUP_DATABASE_NAME];
  }

  const [rows] = await connection.execute('SHOW DATABASES');
  return (rows as any[])
    .map(row => row.Database)
    .filter(db => !databasesToExclude.includes(db));
};

const dumpDatabase = async (connection: any, database: string): Promise<string> => {
  let dump = `-- Database: ${database}\n`;
  dump += `CREATE DATABASE IF NOT EXISTS \`${database}\`;\n`;
  dump += `USE \`${database}\`;\n\n`;

  // Get all tables
  const [tables] = await connection.execute(`SHOW TABLES FROM \`${database}\``);

  for (const tableRow of tables as any[]) {
    const tableName = Object.values(tableRow)[0] as string;

    // Get table structure
    const [createTable] = await connection.execute(`SHOW CREATE TABLE \`${database}\`.\`${tableName}\``);
    const createTableSQL = (createTable as any[])[0]['Create Table'];

    dump += `-- Table structure for \`${tableName}\`\n`;
    dump += `DROP TABLE IF EXISTS \`${tableName}\`;\n`;
    dump += `${createTableSQL};\n\n`;

    // Get table data
    const [rows] = await connection.execute(`SELECT * FROM \`${database}\`.\`${tableName}\``);

    if ((rows as any[]).length > 0) {
      dump += `-- Data for table \`${tableName}\`\n`;
      dump += `LOCK TABLES \`${tableName}\` WRITE;\n`;

      const columns = Object.keys((rows as any[])[0]);
      const columnNames = columns.map(col => `\`${col}\``).join(',');

      for (const row of rows as any[]) {
        const values = columns.map(col => {
          const value = row[col];
          if (value === null) return 'NULL';
          if (typeof value === 'string') return `'${value.replace(/'/g, "\\'")}'`;
          if (value instanceof Date) return `'${value.toISOString().slice(0, 19).replace('T', ' ')}'`;
          return value;
        }).join(',');

        dump += `INSERT INTO \`${tableName}\` (${columnNames}) VALUES (${values});\n`;
      }

      dump += `UNLOCK TABLES;\n\n`;
    }
  }

  return dump;
};

const dumpToFile = async (path: string): Promise<void> => {
  console.log(`Creating dump at ${path}...`);
  console.log(`Connecting to database at ${env.BACKUP_DATABASE_HOST}:${env.BACKUP_DATABASE_PORT} as user ${env.BACKUP_DATABASE_USER}`);

  let connection;

  try {
    // Create MySQL connection
    connection = await createConnection({
      host: env.BACKUP_DATABASE_HOST,
      port: parseInt(env.BACKUP_DATABASE_PORT),
      user: env.BACKUP_DATABASE_USER,
      password: env.BACKUP_DATABASE_PASSWORD
    });

    console.log('Database connection successful');

    // Get databases to backup
    const databases = await getDatabaseList(connection);
    console.log(`Found databases to backup: ${databases.join(', ')}`);

    let fullDump = `-- MySQL dump created by Node.js backup script\n`;
    fullDump += `-- Date: ${new Date().toISOString()}\n\n`;
    fullDump += `SET FOREIGN_KEY_CHECKS=0;\n\n`;

    // Dump each database
    for (const database of databases) {
      console.log(`Dumping database: ${database}`);
      const dbDump = await dumpDatabase(connection, database);
      fullDump += dbDump;
    }

    fullDump += `SET FOREIGN_KEY_CHECKS=1;\n`;

    // Create gzipped file
    const readable = Readable.from([fullDump]);
    const gzip = createGzip();
    const writeStream = require('fs').createWriteStream(path);

    await pipelineAsync(readable, gzip, writeStream);

    console.log('Database dump completed successfully');

  } catch (error) {
    console.error(`Database connection failed: ${error}`);
    throw error;
  } finally {
    if (connection) {
      await connection.end();
    }
  }
};

const deleteFile = async (path: string): Promise<void> => {
  console.log(`Deleting local dump file at ${path}...`);

  await new Promise((resolve, reject) => {
    unlink(path, (error) => {
      if (error) {
        reject({ error: JSON.stringify(error) });

        if (isDebug()) {
          console.log(`Debug: could not remove local dump file. ${error}`);
        }
        return;
      }

      resolve(undefined);
    });
  });
}

export const backup = async (): Promise<void> => {
  const timestamp = new Date().toISOString().replace(/[:.]+/g, '-');
  const filename = `backup-${timestamp}.sql.gz`;
  const filepath = `/tmp/${filename}`;

  await dumpToFile(filepath);

  // Verify the backup file was created and has content
  try {
    const stats = statSync(filepath);
    console.log(`Backup file created: ${filepath} (${stats.size} bytes)`);

    if (stats.size === 0) {
      throw new Error('Backup file is empty');
    }

    // Check if file is suspiciously small (less than 100 bytes likely means error)
    if (stats.size < 100) {
      console.warn(`Warning: Backup file is very small (${stats.size} bytes). This might indicate an issue with the dump.`);
    }
  } catch (error) {
    console.error(`Error verifying backup file: ${error}`);
    throw error;
  }

  await uploadToS3({ name: filename, path: filepath });
  await deleteFile(filepath);

  console.log("Backup successfully created.");
}
