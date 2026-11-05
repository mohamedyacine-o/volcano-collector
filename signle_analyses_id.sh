AID="bb7561ce-b0c0-42b6-b081-47f660b0c8f0"
OUT_DIR="/var/log/volcano_artifacts/incoming"

PGPASSWORD="YOUR_POSTGRES_PASSWORD_HERE" psql -h 10.250.3.15 -p 5432 -U logstash_ro -d volcano -At -c "
  SELECT json_build_object(
    'analysis_id', '$AID',
    'source_table', table_name,
    'artifact', artifact_data::json
  )::text
  FROM artifacts.get_all_analyses_artifacts(CAST('$AID' AS uuid));
" > "${OUT_DIR}/${AID}.ndjson.tmp" && \
mv "${OUT_DIR}/${AID}.ndjson.tmp" "${OUT_DIR}/${AID}.ndjson" && \
chown logstash:logstash-svc "${OUT_DIR}/${AID}.ndjson" && \
chmod 644 "${OUT_DIR}/${AID}.ndjson" && \
echo "[✓] Successfully staged $AID for Logstash"
