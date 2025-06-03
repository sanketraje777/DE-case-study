INSERT INTO public.load_audit (table_name, last_loaded, data_quality)
VALUES
    (%s, NOW(), %s)
ON CONFLICT (table_name) DO UPDATE
SET 
    last_loaded = EXCLUDED.last_loaded,
    data_quality = EXCLUDED.data_quality
WHERE (
    public.load_audit.last_loaded,
    public.load_audit.data_quality) 
IS DISTINCT FROM (
    EXCLUDED.last_loaded,
    EXCLUDED.data_quality);
