CREATE OR REPLACE FUNCTION clean_text(value TEXT)
RETURNS TEXT AS $$
BEGIN
  RETURN CASE
    WHEN value = 'Sem informação' THEN NULL
    ELSE value
  END;
END; $$ LANGUAGE 'plpgsql' IMMUTABLE;
