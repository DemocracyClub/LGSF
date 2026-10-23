#!/usr/bin/env bash
#
# Backfill minutes into the migrated S3 store, then publish decisions and
# minutes to the council repositories on GitHub. Made to run in tmux.
#
#   scripts/weekend-backfill.sh scrape     # unattended: S3 only, no commits
#   scripts/weekend-backfill.sh publish    # at the keyboard: commits to GitHub
#
# Run from the repo root on master, with these merged: #529 (migration
# --to-prefix), #532 (GitHub reads), #533 (shared documents), the minutes
# --since PR, and the PR adding scripts/backfill_github.py. Run it in the
# directory you ran the migration from, so it reuses documents-migration/.
#
# scrape:
#   1. Brings merged-data/ up to date with data/ (the documents migration
#      with --to-prefix; it copies only what changed since its last run).
#   2. Scrapes minutes for every council, records and documents to
#      merged-data/ on S3. Each council starts where Poteris's data ends,
#      and at least a year back (scripts/poteris_since.py, written to
#      logs/), unless $SINCE gives one date for all. A document decisions already
#      stored is reused, not downloaded (#533). Minutes checkpoint every
#      50 meetings, so an interruption keeps what was done.
#   It repeats the minutes pass, skipping councils that finished in the
#   last $REFRESH_HOURS hours, until a pass ends with credentials still
#   valid, up to $MAX_PASSES passes. When AWS credentials have expired it
#   waits, polling, for you to log in again in another window.
#   Makes no git commits, so needs no signing.
#
# publish:
#   Copies decisions and minutes records from merged-data/ into each
#   council's repository, one commit per type per council, and pushes.
#   The commits are signed through 1Password, so be there to approve.
#   Rerunnable: a council already up to date makes no commit.
#   Run a dry run first:  DRY_RUN=1 scripts/weekend-backfill.sh publish
#
# Settings, by environment variable:
#   SINCE          one start date for every council    (default: per council, from Poteris)
#   WORKERS        councils scraped at once            (default 8)
#   REFRESH_HOURS  skip councils finished this recently (default 72)
#   MAX_PASSES     minutes passes before giving up     (default 4)
#   COUNCILS       comma-separated codes, default all
#   DRY_RUN        set to 1 for publish --dry-run

set -uo pipefail

STAGE="${1:-}"
SINCE="${SINCE:-}"
WORKERS="${WORKERS:-8}"
REFRESH_HOURS="${REFRESH_HOURS:-72}"
MAX_PASSES="${MAX_PASSES:-4}"
COUNCILS="${COUNCILS:-}"

export LGSF_S3_BUCKET="${LGSF_S3_BUCKET:-lgsf-data-archive}"
MERGED_PREFIX="merged-data"

mkdir -p logs
LOG="logs/weekend-backfill-${STAGE:-none}-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

say() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

aws_ok() { aws sts get-caller-identity >/dev/null 2>&1; }

wait_for_aws() {
    if aws_ok; then return; fi
    say "AWS credentials have expired. Log in again in another window (aws sso login); polling every 5 minutes."
    until aws_ok; do sleep 300; done
    say "AWS credentials are back."
}

which_councils() {
    if [[ -n "$COUNCILS" ]]; then echo "--council $COUNCILS"; else echo "--all-councils"; fi
}

scrape() {
    wait_for_aws

    say "1/2: bringing s3://$LGSF_S3_BUCKET/$MERGED_PREFIX up to date with data/"
    if [[ -n "$COUNCILS" ]]; then council_args=(--council "$COUNCILS"); else council_args=(); fi
    LGSF_S3_PREFIX=data uv run python -m scripts.migrate_documents \
        --to-prefix "$MERGED_PREFIX" --redo-done --councils-at-once 8 "${council_args[@]}" \
        || say "The migration reported problems; see above. Carrying on with minutes."

    local since_args
    if [[ -n "$SINCE" ]]; then
        since_args=(--since "$SINCE")
        say "2/2: minutes since $SINCE into s3://$LGSF_S3_BUCKET/$MERGED_PREFIX"
    else
        local since_file="logs/minutes-since-$(date +%Y%m%d).csv"
        if [[ ! -s "$since_file" ]]; then
            say "Working out each council's start from Poteris's data"
            uv run python -m scripts.poteris_since --out "$since_file" \
                || { say "Couldn't reach Poteris; set SINCE to run with one date."; exit 1; }
        fi
        since_args=(--since-file "$since_file")
        say "2/2: minutes from each council's date in $since_file into s3://$LGSF_S3_BUCKET/$MERGED_PREFIX"
    fi
    local pass=1
    while (( pass <= MAX_PASSES )); do
        wait_for_aws
        say "Minutes pass $pass of $MAX_PASSES (skipping councils finished in the last ${REFRESH_HOURS}h)"
        # shellcheck disable=SC2046
        LGSF_STORAGE_BACKEND=s3 LGSF_S3_PREFIX="$MERGED_PREFIX" LGSF_DOCUMENT_STORAGE_BACKEND=s3 \
            uv run python manage.py minutes $(which_councils) \
                "${since_args[@]}" --refresh "$REFRESH_HOURS" --workers "$WORKERS" --unpretty
        if aws_ok; then
            say "Pass $pass finished with credentials still valid."
            break
        fi
        say "Credentials expired during pass $pass; another pass picks up where it stopped."
        (( pass++ ))
    done

    say "Each pass ends with a table of the councils that failed; see $LOG."
    say "Scrape done. Next, at the keyboard: DRY_RUN=1 $0 publish, then $0 publish"
}

publish() {
    wait_for_aws
    local dry=()
    [[ "${DRY_RUN:-}" == "1" ]] && dry=(--dry-run)
    say "Publishing decisions and minutes from s3://$LGSF_S3_BUCKET/$MERGED_PREFIX to GitHub ${dry[*]:-}"
    if [[ -z "${DRY_RUN:-}" ]]; then
        say "Commits are signed through 1Password: keep it unlocked to approve."
    fi
    # shellcheck disable=SC2046
    uv run python -m scripts.backfill_github \
        --types decisions,minutes $(which_councils) \
        --prefix "$MERGED_PREFIX" "${dry[@]}" \
        --report "logs/github-backfill-$(date +%Y%m%d-%H%M%S).csv"
}

case "$STAGE" in
    scrape) scrape ;;
    publish) publish ;;
    *) echo "usage: $0 scrape|publish   (see the top of this file)"; exit 2 ;;
esac
say "Log: $LOG"
