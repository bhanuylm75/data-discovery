const { Pool } = require("pg");

const pool = new Pool({
  host: "127.0.0.1",     // IMPORTANT (not localhost)
  port: 5431,
  user: "postgres",
  password: "admin",
  database: "postgres",  // start with postgres DB first
  ssl: false,            // IMPORTANT
});

async function main() {
  try {
    const res = await pool.query("SELECT current_database() as db, current_user as usr");
    console.log("Connected ✅", res.rows);

    // Optional: list all databases
    const dbs = await pool.query("SELECT datname FROM pg_database WHERE datistemplate = false;");
    console.log("Databases:", dbs.rows);

  } catch (err) {
    console.error("Connection failed ❌");
    console.error(err.message);
  } finally {
    await pool.end();
  }
}

main();
