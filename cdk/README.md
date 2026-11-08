# LGSF CDK deployment

AWS CDK v2 code for the Local Government Scraper Framework's daily run. One
stack, `cdk/stacks/lgsf_stack.py`, with the document pipeline's resources in
`cdk/stacks/documents.py`. CircleCI deploys it on every push to
`master` (see `.circleci/config.yml`), so a merge is a deploy.

## What runs

```
EventBridge (every 24h), input {"all_councils": true}
  -> Step Function "scraper-orchestration"
       1. EnumerateCouncils        Lambda: each current council with the scraper
                                   types it has, e.g. {"council": "KIR",
                                   "scraper_types": ["councillors"]}
       2. ParallelScrapers         Distributed Map, 10 at a time, over those councils
            RunCouncilScraperTypes Map, one at a time, over that council's types
              RunScraper           Lambda (Docker image, with git): scrapes one
                                   council for one type and pushes at most one
                                   commit to LGSF-Data/<COUNCIL> on GitHub
                                   The Map's results go to map-results/ in the
                                   reports bucket, not into the execution's state
       3. Aggregation              Lambda: reads them, writes
                                   run-reports/<timestamp>.json, deletes them
       4. NotificationReport       Lambda: emails a summary via SNS
       5. PostProcessing           Lambda: logs a summary
```

Which types run comes from `lgsf/scraper_types.py`, read by the enumerator
at run time: the scheduled run does the types marked `daily`, in the order
listed. So adding a type needs no change here. A council's types run one
after another, never at once, so two scrapes never push to the same
repository together.

Each council is one Map item, and each of its types is caught separately,
so one failing never stops the others (`tolerated_failure_percentage=100`);
failures show in the run report. The worker is told the Step Functions
execution name, which ends up in the trailers of every commit it makes and
names the run report, so a commit can be traced to the report for the run
that made it.

Storage: the worker writes records to GitHub (`LGSF_STORAGE_BACKEND=github`)
with a GitHub App's credentials, and the documents those records link to
(minutes, decision papers) to the data bucket (`LGSF_DOCUMENT_STORAGE_BACKEND=s3`),
which it can list, read and write but not delete from. Run reports and
per-council run logs for failures go to the reports bucket. The dashboard at
https://democracyclub.github.io/lgsf-dashboard/ reads those reports.

Logs: each scraper type has a log group, `/aws/lambda/lgsf-scrapers/<type>`,
with one-month retention, and each council's run is a stream in it. The
worker is told the prefix (`SCRAPER_LOG_GROUP_PREFIX`) so the two can't
disagree.

New commits: after each push to a council's `main` the worker publishes
one message to the `data-commits` SNS topic, `{"repository": "STO",
"after": "<sha main now points at>", "scraper_type": ..., "run_id": ...}`
(`lgsf/storage/doorbell.py`). It's a doorbell: it carries no data, and a
subscriber fetches the repository itself, so a lost message only delays it
until the next push. A run that pushes nothing publishes nothing, and a
failed publish never fails the run. Subscribers (reps-wiki's queue, say)
subscribe from their own stacks; this one has none. The topic's ARN is in
SSM at `/lgsf/<env>/sns/data_commits_topic_arn`, where
`scripts/backfill_github.py` finds it so its pushes ring too.

The scraper run itself uses no SQS queue; the document pipeline below has
three.

## The document pipeline

What turns stored documents into text, chunks and embeddings
(`docs/document-pipeline.md`), made by `cdk/stacks/documents.py`. The
handlers are `lgsf/embeddings/aws.py`.

```
data-commits topic (a push to LGSF-Data)  ─┐
Step Function ended (any end state)        ├─> lambda-documents-trigger ─> queue-documents
EventBridge, every hour                    ─┘       │
                                                    └─> Batch job, when queue-extract
                                                        has messages
queue-documents ─> lambda-documents-dispatch ─┬─> queue-extract ─> Batch lgsf-<env>-documents
                                              │                     (Docling) ─> queue-embed
                                              └─> queue-embed ─> lambda-documents-embed
                                                                  (Bedrock) ─> topic-file-ready
```

**Trigger** (`lgsf-<env>-lambda-documents-trigger`, 512 MB, 15 minutes)
finds files to look at and puts one message a file on the documents
queue: only files stored after `DOCUMENTS_SINCE`, the instant in SSM at
`/lgsf/<env>/documents/since`; older files are the backfill command's.
It runs three ways: subscribed to the data-commits topic, so a
council's new documents are looked at minutes after they're scraped; from
an EventBridge rule on the state machine's execution ending, however it
ended (SUCCEEDED, FAILED, TIMED_OUT or ABORTED), as a sweep for anything
a doorbell missed; and every hour, when it also submits a Batch job if
the extract queue has messages and no job is already on the queue. A
sweep invokes the function again once a council, so it may invoke
itself. The rules send EventBridge's own events; the handler tells them
apart by `source`.

**Dispatch** (`lgsf-<env>-lambda-documents-dispatch`, 1024 MB, 2 GB of
`/tmp` for the documents it downloads, 5 minutes, at most 10 at once:
the event source's maximum and the reserved concurrency) takes the
documents queue in batches of ten and settles each file without Docling: a restricted page, an empty file
or something that isn't a document gets its status; a file that only
needs embedding goes to the embed queue; one that needs converting goes
to the extract queue.

**Batch** (`lgsf-<env>-documents`, a job queue and a job definition of
the same name) is where Docling runs: with torch it's several times the
Lambda layer's limit. The image is `Dockerfile.documents`, built from
`python:3.12-slim` with the `embeddings` dependency group and Docling's
models downloaded into it, and pushed as a CDK image asset when the stack
is deployed. The compute environment is Spot `c7i.4xlarge` or
`c7a.4xlarge` instances in the default VPC, none running between jobs
and at most two (32 vCPUs) at once, with a 100 GB root volume for the
image. A job asks for 16 vCPUs and 30 GiB and runs
`manage.py embeddings --drain-queue <extract queue> --source s3 --workers 4 --threads 4`
until the queue is empty, extending each message's visibility while its
file converts and sending each converted file to the embed queue. It may
run for 12 hours and is retried, three times, only when its Spot
instance is taken back; anything else is left for a person. Its logs are
in `/aws/batch/lgsf-<env>-documents`, kept a month. GPU instances are a
context switch away, `-c documents-instances=gpu`, but need a dependency
group with the CUDA build of torch that `pyproject.toml` doesn't have.

**Embed** (`lgsf-<env>-lambda-documents-embed`, 1024 MB, 15 minutes)
takes the embed queue in batches of ten and calls Amazon Titan Text
Embeddings V2 on Bedrock, and may call nothing else there. A file whose
chunks file has gone goes back to the extract queue. Titan's quota
is 300,000 tokens a minute for the account, so the function's reserved
concurrency and the event source's maximum are both 4; more would be
throttled rather than faster.

**Queues**: `lgsf-<env>-queue-documents`, `-extract` and `-embed`, each
with a dead-letter queue (`-dlq`) that a message goes to after five
tries. Every queue keeps a message for 14 days, SQS's longest, so a file
waits out a stopped Batch job rather than expiring. A Lambda's queue hides a message for six
times the function's timeout; the extract queue for two hours, since the
Batch worker extends it while converting.

**Topic**: `lgsf-<env>-topic-file-ready` gets one message each time a
file's status is written, from whichever function or job wrote it. Like
data-commits, it's a doorbell: the subscriber reads the status file
itself. Its ARN is in SSM at `/lgsf/<env>/sns/file_ready_topic_arn` and
the stack output `FileReadyTopicArn`.

**Alarms**, to the scraper-notifications topic: each dead-letter queue
having a message, and the extract queue's oldest message being over six
hours old, which means the Batch job isn't running or isn't keeping up.
An EventBridge rule (`lgsf-<env>-rule-documents-batch-failed`) sends the
same topic a line naming a Batch job on our queue that ends FAILED, with
its status reason; a Spot reclaim is retried first, so that's a job that
ran out of attempts, timed out or stopped on an error.

**Permissions**: each function has its own role with only what it does.
The trigger lists the bucket and reads `merged-data/*`, sends to the
documents queue, reads the extract queue's depth, submits to the Batch
queue and lists its jobs, and invokes itself. Dispatch, embed and the
Batch job read and write `merged-data/*` (never delete), publish to
file-ready, and send to the queue after theirs; the embed function also
sends to extract, for a file whose chunks have gone, and alone has
`bedrock:InvokeModel`, on Titan V2 only.

## Configuration

Everything environment-specific is read from SSM Parameter Store under
`/lgsf/<DC_ENVIRONMENT>/`:

| Parameter | Used for |
| --- | --- |
| `github/app_id`, `github/installation_id`, `github/app_private_key` | The GitHub App the worker commits as |
| `github/organization` | The organisation holding one repository per council |
| `s3/reports_bucket` | Run reports and run logs |
| `s3/data_bucket` | Scraped documents, under `merged-data/<COUNCIL>/documents/`: the content-keyed store made by `scripts/migrate_documents.py --to-prefix merged-data` |
| `notification/emails` | Comma-separated recipients of the summary |
| `proxy_url` | Optional outbound proxy for scrapers |
| `vpc/id` | The default VPC, where the document pipeline's Batch instances run |
| `vpc/public_subnet_ids` | Its three subnets, one an availability zone, as a `StringList` (`subnet-…,subnet-…,subnet-…`) |
| `documents/since` | The trigger and sweep only queue files stored after this instant (ISO 8601, UTC); older files are queued by the backfill command. Made by the stack, from `DOCUMENTS_SINCE` in `documents.py`: change it there, as a deploy puts it back |

The VPC is named here rather than looked up by the CDK because a lookup
needs AWS credentials when the stack is synthesised, and CI synthesises
without any. To fill the two parameters in:

```bash
aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId'
aws ec2 describe-subnets --filters Name=default-for-az,Values=true --query 'Subnets[].SubnetId'
```

The stack writes `sns/data_commits_topic_arn` and `sns/file_ready_topic_arn`
itself, for subscribers outside it.

`DC_ENVIRONMENT` is `development` (default) or `production`.

## Running it yourself

Prerequisites: `uv sync --group cdk`, `npm install`, and AWS credentials for
the account.

```bash
export AWS_PROFILE=<profile> DC_ENVIRONMENT=development
make lambda-layers/DependenciesLayer/requirements.txt   # layer requirements from uv.lock
npx cdk bootstrap        # first time in an account only
npx cdk synth            # render the template
npx cdk diff             # compare with what is deployed
npx cdk deploy
```

To start a run from the CLI instead of waiting for the schedule:

```bash
python manage.py councillors --aws --council KIR [--wait]
python manage.py councillors --aws --all-councils
```

Every scraper type's command has `--aws`, and runs only its own type:
`python manage.py minutes --aws --council KIR` runs minutes for Kirklees,
whether or not minutes are in the daily run yet. A type must be registered
in `lgsf/scraper_types.py` for the enumerator to accept it.

This refuses to start while a running execution already covers any of the
same councils, whatever types it is running,, since the two would race to push the same repository and
the later one would fail. `--allow-overlap` starts anyway.

## Stack outputs

`StepFunctionArn`, the ARN of each Lambda, the ARNs of the three SNS
topics (run reports, `ScraperNotificationTopicArn`; new commits,
`DataCommitsTopicArn`; documents finished, `FileReadyTopicArn`), and the
execution role ARN. `npx cdk deploy` prints them.

## Troubleshooting

- **"Need to perform AWS CDK bootstrap"**: run `npx cdk bootstrap` once for the account and region.
- **Lambda layer build fails on a missing requirements file**: run `make lambda-layers/DependenciesLayer/requirements.txt`.
- **"Unable to locate credentials"**: check `AWS_PROFILE`.
- **A council's run**: its log stream in `/aws/lambda/lgsf-scrapers/councillors`, and for a failure the run log the report points to in S3.
