import { exec } from "child_process";
import { PutObjectCommand, S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { createReadStream, unlink, statSync } from "fs";
import { env } from "./env";

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

const dumpToFile = async (path: string): Promise<void> => {
  console.log(`Creating dump at ${path}...`);
  console.log(`Connecting to database at ${env.BACKUP_DATABASE_HOST}:${env.BACKUP_DATABASE_PORT} as user ${env.BACKUP_DATABASE_USER}`);

  await new Promise((resolve, reject) => {
    const host = `--host='${env.BACKUP_DATABASE_HOST}'`;
    const port = `--port='${env.BACKUP_DATABASE_PORT}'`;
    const user = `--user='${env.BACKUP_DATABASE_USER}'`;
    const password = `--password='${env.BACKUP_DATABASE_PASSWORD}'`;
    const databasesToExclude = ['mysql', 'sys', 'performance_schema', 'information_schema', 'innodb'].join('|');

    const command = env.BACKUP_DATABASE_NAME
      ? `mysqldump ${host} ${port} ${user} ${password} --single-transaction --routines --triggers ${env.BACKUP_DATABASE_NAME} | gzip > ${path}`
      : `mysql ${host} ${port} ${user} ${password} -e "show databases;" | grep -Ev "Database|${databasesToExclude}" | xargs mysqldump ${host} ${port} ${user} ${password} --single-transaction --routines --triggers --databases | gzip > ${path}`

    if (isDebug()) {
      console.log(`Debug: SQL command: ${command}`);
    }

    exec(command, (error, stdout, stderr) => {
      if (error) {
        console.log(`Database connection failed: ${error.message}`);
        console.log(`stderr: ${stderr}`);
        reject({ error: JSON.stringify(error), stderr });

        if (isDebug()) {
          console.log(`Debug: could not create local dump file. ${error}`);
        }

        return;
      }

      if (stderr && stderr.trim() !== '') {
        console.log(`Warning during dump: ${stderr}`);
      }

      console.log(`Database connection successful, dump created`);
      resolve(undefined);
    });
  });
}

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
  } catch (error) {
    console.error(`Error verifying backup file: ${error}`);
    throw error;
  }

  await uploadToS3({ name: filename, path: filepath });
  await deleteFile(filepath);

  console.log("Backup successfully created.");
}
