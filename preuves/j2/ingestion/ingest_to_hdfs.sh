#!/bin/bash

set -u

SOURCE_ROOT="${HOME}/Downloads/retail_project"
HDFS_ROOT="/retail/bronze"
LOG_DIR="logs/ingestion"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/ingestion_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $1" | tee -a "$LOG_FILE"
}

ingest() {
    local SOURCE="$1"
    local DESTINATION="$2"

    log "Début ingestion : ${SOURCE}"

    if [ ! -d "$SOURCE_ROOT/$SOURCE" ]; then
        log "ERREUR : source absente : $SOURCE_ROOT/$SOURCE"
        return 1
    fi

    if ! docker exec retail-namenode hdfs dfs -test -d "$DESTINATION"; then
        log "ERREUR : destination HDFS absente : $DESTINATION"
        return 1
    fi

    if docker exec retail-namenode hdfs dfs -test -e "$DESTINATION/$SOURCE"; then
        log "IGNORÉ : destination déjà présente : $DESTINATION/$SOURCE"
        return 0
    fi

    if ! docker cp "$SOURCE_ROOT/$SOURCE" retail-namenode:/tmp/; then
        log "ERREUR : copie Docker échouée : $SOURCE"
        return 1
    fi

    if ! docker exec retail-namenode hdfs dfs -put "/tmp/$SOURCE" "$DESTINATION/"; then
        log "ERREUR : transfert HDFS échoué : $SOURCE"
        return 1
    fi

    log "SUCCÈS : $SOURCE importé dans $DESTINATION"
}

ERRORS=0

ingest "data" "$HDFS_ROOT" || ERRORS=$((ERRORS + 1))
ingest "data_refresh" "$HDFS_ROOT" || ERRORS=$((ERRORS + 1))

if [ "$ERRORS" -eq 0 ]; then
    log "BILAN : ingestion terminée sans erreur"
else
    log "BILAN : $ERRORS erreur(s) détectée(s)"
fi

exit "$ERRORS"
