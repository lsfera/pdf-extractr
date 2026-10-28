# pdf-extract-infra

Pulumi (Rust SDK, experimental) program for the PDF extraction pipeline:
S3 raw bucket -> SQS (+ DLQ) -> Lambda (`../lambda`, Rust, arm64) -> S3 output bucket,
with a log group, least-privilege IAM role, CloudWatch alarms and, on real AWS, an SNS topic they notify. 26 resources on LocalStack, 27 on AWS.

Architecture diagram: `../lambda/docs/pdf-extraction-pipeline.drawio`.

## Prerequisites

- Pulumi CLI, Go >= 1.25 (to build the language plugin), Rust >= 1.85.
- A checkout of <https://github.com/pulumi-labs/pulumi-rust> beside this directory (`../../pulumi-rust`),
  with the plugin built: `(cd ../../pulumi-rust/pulumi-language-rust && go build .)` and that directory on `PATH`.
- The generated AWS SDK (not committed, see `.gitignore`):
  `pulumi package gen-sdk aws@7.41.0 --language rust --out ./sdks/aws`, then point
  `sdks/aws/rust/Cargo.toml`'s `pulumi` dependency at `../../../../../pulumi-rust/sdk/rust/pulumi`.
- `lambda-package/bootstrap`: the static arm64 build of the Lambda (also not committed).

## Build the Lambda package

```sh
# In the devcontainer, bind mounts to the Docker host do not work, so build through a docker build
# context and export the binary. The Dockerfile copies ../../pdf_oxide and ../lambda and runs
#   cargo build --release --features lambda --bin lambda --target aarch64-unknown-linux-musl
# in clux/muslrust:stable, then `COPY --from=build .../lambda /bootstrap` into a scratch stage.
docker build --target out --output type=local,dest=./out .
cp out/bootstrap lambda-package/bootstrap
```

## Validate against LocalStack

`localstack/localstack:latest` (2026.10) refuses to start without `LOCALSTACK_AUTH_TOKEN`.
`4.14.0` is the community image used here.

```sh
docker run -d --name pdf-localstack --network devcontainer-rust_network \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e SERVICES=s3,sqs,lambda,iam,logs,cloudwatch,sts,xray \
  -e LAMBDA_DOCKER_NETWORK=devcontainer-rust_network localstack/localstack:4.14.0

export PULUMI_CONFIG_PASSPHRASE="" AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
       AWS_ENDPOINT_URL=http://pdf-localstack:4566
pulumi login file://$PWD/.pulumi-state && pulumi stack init local
pulumi config set aws:region us-east-1
pulumi config set aws:skipCredentialsValidation true
pulumi config set aws:skipRequestingAccountId true
pulumi config set aws:skipMetadataApiCheck true
pulumi config set aws:s3UsePathStyle true
pulumi config set forceDestroy true
pulumi config set endpointUrl http://pdf-localstack:4566   # points the Lambda's SDK at LocalStack
pulumi up
# upload raw/<name>.pdf to the raw bucket (stack output rawBucket); extracted/raw/<name>.pdf.{json,md}
# appear in outputBucket.
pulumi destroy
```

Notes:

- If a build is killed (exit 137), keep the `[profile.dev]` in `Cargo.toml`; compiling the AWS SDK crate
  with full debuginfo needs far more memory. Setting `CARGO_TARGET_DIR` outside `/workspaces` also avoids
  permission errors on the bind mount.
- Real AWS: use a stack without `endpointUrl`, the `aws:skip*` options or `AWS_ENDPOINT_URL`.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `codeDir` | `lambda-package` | Directory holding `bootstrap` |
| `maxConcurrency` | `5` | Event source mapping `MaximumConcurrency` |
| `forceDestroy` | `false` | Allow deleting non-empty buckets |
| `rawRetentionDays` | unset | Expire raw PDFs after this many days (unset keeps them; old versions always expire after 30 days) |
| `outputRetentionDays` | unset | Expire extraction results after this many days |
| `alarmEmail` | unset | Email address subscribed to the alarms topic (must confirm the subscription); ignored on LocalStack, where no topic is created |
| `endpointUrl` | unset | Emulator endpoint for the Lambda's AWS SDK (LocalStack only) |

Memory (1024 MB), timeout (120 s), visibility timeout (6x timeout) and `maxReceiveCount` (3) are constants in
`src/main.rs`.

## Tests and local stack (containers)

From the parent directory (`pdf-extractor/`), with Docker available:

```sh
make test   # lambda tests + LocalStack end-to-end test in containers; the compose stack is always torn down
make up     # LocalStack on :4566 with the pipeline deployed, for manual PDF uploads; Ctrl-C tears it down
make down   # manual teardown if `make up` was killed hard
```

The orchestration lives in `infra/Makefile` (`deploy`, `assert`, `e2e`, `dev`, `destroy`); there are no shell scripts.
To debug the Lambda handler against that LocalStack, use the "Debug PDF extract lambda (LocalStack)" launch
configuration in `pdf-extractor/.vscode/launch.json` (it asks for the bucket names `make up` prints and an uploaded object key).
