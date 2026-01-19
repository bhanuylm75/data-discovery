const { Client } = require("pg");
const crypto = require("crypto");

const sourceConfig = {
  host: "localhost",
  port: 5431,
  user: "postgres",
  password: "admin",
  database: "discovery_db",
};

const metaConfig = {
  host: "localhost",
  port: 5431,
  user: "postgres",
  password: "admin",
  database: "metadata_db",
};

function makeSchemaHash(columns) {
  // create fingerprint from column_name + data_type + nullable
  const str = columns
    .map((c) => `${c.column_name}:${c.data_type}:${c.is_nullable}`)
    .join("|");

  return crypto.createHash("sha256").update(str).digest("hex");
}

async function scanPostgres() {
  const sourceClient = new Client(sourceConfig);
  const metaClient = new Client(metaConfig);

  await sourceClient.connect();
  await metaClient.connect();

  console.log("Connected to source + metadata DB");

  // 1) Get tables from source
  const tablesRes = await sourceClient.query(`
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
      AND table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY table_schema, table_name;
  `);

  for (const t of tablesRes.rows) {
    const { table_schema, table_name } = t;

    // 2) Get columns for this table (from source)
    const colsRes = await sourceClient.query(
      `
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_schema=$1 AND table_name=$2
      ORDER BY ordinal_position;
      `,
      [table_schema, table_name]
    );

    const schemaHash = makeSchemaHash(colsRes.rows);

    // 3) Check existing asset hash in metadata DB
    const existing = await metaClient.query(
      `
      SELECT id, schema_hash
      FROM assets
      WHERE source_db=$1 AND schema_name=$2 AND table_name=$3
      `,
      ["discovery_db", table_schema, table_name]
    );

    if (existing.rows.length > 0) {
      const oldHash = existing.rows[0].schema_hash;

      // If schema hash same, skip updating columns
      if (oldHash && oldHash === schemaHash) {
        console.log(`No change: ${table_schema}.${table_name} (skipping)`);
        continue;
      }
    }

    // 4) Insert/update asset with schema_hash
    const assetRes = await metaClient.query(
      `
      INSERT INTO assets (source_db, schema_name, table_name, schema_hash)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (source_db, schema_name, table_name)
      DO UPDATE SET
        discovered_at = now(),
        schema_hash = EXCLUDED.schema_hash
      RETURNING id;
      `,
      ["discovery_db", table_schema, table_name, schemaHash]
    );

    const assetId = assetRes.rows[0].id;

    // 5) Refresh columns only when changed
    await metaClient.query(`DELETE FROM columns WHERE asset_id=$1`, [assetId]);

    for (const c of colsRes.rows) {
      await metaClient.query(
        `
        INSERT INTO columns (asset_id, column_name, data_type, is_nullable)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (asset_id, column_name)
        DO UPDATE SET
          data_type = EXCLUDED.data_type,
          is_nullable = EXCLUDED.is_nullable,
          discovered_at = now();
        `,
        [assetId, c.column_name, c.data_type, c.is_nullable]
      );
    }

    console.log(`Updated: ${table_schema}.${table_name}`);
  }

  await sourceClient.end();
  await metaClient.end();
  console.log("Scan completed ✅");
}

scanPostgres().catch((err) => console.error("Error:", err));
