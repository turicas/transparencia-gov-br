CREATE OR REPLACE FUNCTION clean_text(value TEXT)
RETURNS TEXT AS $$
BEGIN
  RETURN CASE
    WHEN value ~* '^sem informa[^ ]+o$' THEN NULL
    ELSE TRIM(value)
  END;
END; $$ LANGUAGE 'plpgsql' IMMUTABLE;
