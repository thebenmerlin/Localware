import postgres from "postgres";

const url =
  process.env.DATABASE_URL ??
  `postgresql://${process.env.USER || "postgres"}@localhost:5432/localware_fund`;

declare global {
  // eslint-disable-next-line no-var
  var __sql: ReturnType<typeof postgres> | undefined;
}

export const sql =
  globalThis.__sql ??
  postgres(url, {
    max: 10,
    idle_timeout: 20,
    connect_timeout: 5,
    transform: { undefined: null },
  });

if (process.env.NODE_ENV !== "production") globalThis.__sql = sql;
