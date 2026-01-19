class PostgresConnector {
  constructor(pool) {
    this.pool = pool;
  }

  async fetchTables() {
    const query = `
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_schema IN ('sales', 'finance')
        AND table_type = 'BASE TABLE';
    `;
    const { rows } = await this.pool.query(query);
    return rows;
  }

  async fetchColumns() {
    const query = `
      SELECT table_schema, table_name, column_name, data_type, ordinal_position
      FROM information_schema.columns
      WHERE table_schema IN ('sales', 'finance')
      ORDER BY table_schema, table_name, ordinal_position;
    `;
    const { rows } = await this.pool.query(query);
    return rows;
  }
}

module.exports = PostgresConnector;
