DROP TABLE IF EXISTS public.load_audit CASCADE;

CREATE TABLE IF NOT EXISTS public.load_audit (
  table_name TEXT PRIMARY KEY,
  last_loaded TIMESTAMP WITH TIME ZONE NOT NULL,
  data_quality TEXT
);
