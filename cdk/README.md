# LGSF CDK deployment

AWS CDK v2 code for the Local Government Scraper Framework's daily run. One
stack, `cdk/stacks/lgsf_stack.py`. CircleCI deploys it on every push to
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
       3. Aggregation              Lambda: writes run-reports/<timestamp>.json to S3
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

Storage: the worker writes to GitHub (`LGSF_STORAGE_BACKEND=github`) with a
GitHub App's credentials. Run reports and per-council run logs for failures
go to the S3 bucket named in SSM. The dashboard at
https://democracyclub.github.io/lgsf-dashboard/ reads those reports.

Logs: each scraper type has a log group, `/aws/lambda/lgsf-scrapers/<type>`,
with one-month retention, and each council's run is a stream in it. The
worker is told the prefix (`SCRAPER_LOG_GROUP_PREFIX`) so the two can't
disagree.

There is no SQS queue. An earlier design used one; the Step Function
replaced it.

## Configuration

Everything environment-specific is read from SSM Parameter Store under
`/lgsf/<DC_ENVIRONMENT>/`:

| Parameter | Used for |
| --- | --- |
| `github/app_id`, `github/installation_id`, `github/app_private_key` | The GitHub App the worker commits as |
| `github/organization` | The organisation holding one repository per council |
| `s3/reports_bucket` | Run reports and run logs |
| `notification/emails` | Comma-separated recipients of the summary |
| `proxy_url` | Optional outbound proxy for scrapers |

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

`StepFunctionArn`, the ARN of each Lambda, the SNS topic ARN, and the
execution role ARN. `npx cdk deploy` prints them.

## Troubleshooting

- **"Need to perform AWS CDK bootstrap"**: run `npx cdk bootstrap` once for the account and region.
- **Lambda layer build fails on a missing requirements file**: run `make lambda-layers/DependenciesLayer/requirements.txt`.
- **"Unable to locate credentials"**: check `AWS_PROFILE`.
- **A council's run**: its log stream in `/aws/lambda/lgsf-scrapers/councillors`, and for a failure the run log the report points to in S3.
