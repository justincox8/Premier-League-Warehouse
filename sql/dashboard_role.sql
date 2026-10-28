DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dashboard') THEN
    CREATE ROLE dashboard LOGIN;
  END IF;
END $$;

ALTER ROLE dashboard PASSWORD :'pw';

GRANT CONNECT ON DATABASE pl_warehouse TO dashboard;
GRANT USAGE ON SCHEMA serving TO dashboard;
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO dashboard;

-- Future tables created by "pl" in this schema get SELECT automatically.
ALTER DEFAULT PRIVILEGES IN SCHEMA serving GRANT SELECT ON TABLES TO dashboard;
