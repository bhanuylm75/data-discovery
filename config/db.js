const { Pool } = require('pg');

const sourceDb = new Pool({
  host: '127.0.0.1',
  user: 'postgres',
  password: 'admin',
  database: 'discovery_db',
  port: 5432,
  ssl: false
});

const metadataDb = new Pool({
  host: '127.0.0.1',
  user: 'postgres',
  password: 'admin',
  database: 'metadata_db',
  port: 5432,
  ssl: false
});

module.exports = { sourceDb, metadataDb };
