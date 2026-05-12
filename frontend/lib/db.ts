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
    max: 1,
    idle_timeout: 5,
    max_lifetime: 60,
    connect_timeout: 15,
    transform: { undefined: null },
  });

if (process.env.NODE_ENV !== "production") globalThis.__sql = sql;
