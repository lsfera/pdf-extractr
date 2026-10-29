//! PDF extraction pipeline: S3 raw bucket -> SQS -> Lambda -> S3 output bucket.
//!
//! The Lambda is the Rust binary from `../lambda`, packaged as
//! `bootstrap` in the directory named by the `codeDir` config value (default
//! `lambda-package`). The same program deploys to Floci or AWS; only the
//! stack's `aws:*` config differs.
//!
//! Generate the provider SDK first (see README.md):
//! `pulumi package gen-sdk aws@7.41.0 --language rust --out ./sdks/aws`

use pulumi::pv;
use pulumi_aws::{cloudwatch, iam, lambda, s3, sns, sqs, types};

const FUNCTION_NAME: &str = "pdf-extract";
/// Lambda timeout. The queue's visibility timeout is derived from it.
const LAMBDA_TIMEOUT_SECS: i32 = 120;
/// Starting point; size it from the `Max Memory Used` field of the REPORT log line.
const LAMBDA_MEMORY_MB: i32 = 1024;
/// SQS visibility timeout must comfortably exceed the function timeout, or a
/// slow PDF is delivered again while still being processed. AWS recommends 6x.
const VISIBILITY_TIMEOUT_SECS: i32 = 6 * LAMBDA_TIMEOUT_SECS;
const MAX_RECEIVE_COUNT: f64 = 3.0;
const MAX_PDF_BYTES: u64 = 100 << 20;
const OUTPUT_PREFIX: &str = "extracted/";

const LAMBDA_ASSUME_ROLE: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": "sts:AssumeRole", "Principal": { "Service": "lambda.amazonaws.com" } }
  ]
}"#;

type Pv = pulumi::Output<pulumi::PropertyValue>;

fn s(v: &str) -> Pv {
    pv::string(v)
}

fn obj(fields: Vec<(&str, Pv)>) -> Pv {
    pv::object(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

fn policy(statements: Vec<Pv>) -> Pv {
    pv::to_json(obj(vec![
        ("Version", s("2012-10-17")),
        ("Statement", pv::array(statements)),
    ]))
}

fn allow(actions: &[&str], resources: Vec<Pv>) -> Pv {
    obj(vec![
        ("Effect", s("Allow")),
        ("Action", pv::array(actions.iter().map(|a| s(a)).collect())),
        ("Resource", pv::array(resources)),
    ])
}

/// Encryption at rest, no public access, TLS only, and lifecycle rules for a bucket that holds financial documents.
/// `versioned` keeps overwritten and deleted objects recoverable (old versions expire after 30 days);
/// `retention_days` (optional) expires current objects after that many days.
fn harden_bucket(
    ctx: &pulumi::Context,
    id: &str,
    bucket: &s3::Bucket,
    versioned: bool,
    retention_days: Option<Pv>,
) {
    let none = pulumi::ResourceOptions::default;

    s3::BucketServerSideEncryptionConfiguration::new(
        ctx,
        &format!("{id}-sse"),
        s3::BucketServerSideEncryptionConfigurationArgs {
            bucket: Some(bucket.bucket()),
            rules: Some(vec![types::S3BucketServerSideEncryptionConfigurationRuleArgs {
                apply_server_side_encryption_by_default: Some(
                    types::S3BucketServerSideEncryptionConfigurationRuleApplyServerSideEncryptionByDefaultArgs {
                        sse_algorithm: Some(s("AES256").cast()),
                        ..Default::default()
                    },
                ),
                ..Default::default()
            }]),
            ..Default::default()
        },
        none(),
    );

    // The policy is created after the public access block: S3 rejects concurrent changes to the two.
    let access_block = s3::BucketPublicAccessBlock::new(
        ctx,
        &format!("{id}-public-access"),
        s3::BucketPublicAccessBlockArgs {
            bucket: Some(bucket.bucket()),
            block_public_acls: Some(pv::bool(true).cast()),
            block_public_policy: Some(pv::bool(true).cast()),
            ignore_public_acls: Some(pv::bool(true).cast()),
            restrict_public_buckets: Some(pv::bool(true).cast()),
            ..Default::default()
        },
        none(),
    );
    s3::BucketPolicy::new(
        ctx,
        &format!("{id}-tls-only"),
        s3::BucketPolicyArgs {
            bucket: Some(bucket.bucket()),
            policy: Some(
                policy(vec![obj(vec![
                    ("Sid", s("DenyInsecureTransport")),
                    ("Effect", s("Deny")),
                    ("Principal", s("*")),
                    ("Action", s("s3:*")),
                    (
                        "Resource",
                        pv::array(vec![
                            bucket.arn().cast(),
                            pv::concat(vec![bucket.arn().cast(), s("/*")]),
                        ]),
                    ),
                    (
                        "Condition",
                        obj(vec![(
                            "Bool",
                            obj(vec![("aws:SecureTransport", s("false"))]),
                        )]),
                    ),
                ])])
                .cast(),
            ),
            ..Default::default()
        },
        pulumi::ResourceOptions {
            depends_on: vec![access_block.pulumi_resource().clone()],
            ..Default::default()
        },
    );

    let versioning = versioned.then(|| {
        s3::BucketVersioning::new(
            ctx,
            &format!("{id}-versioning"),
            s3::BucketVersioningArgs {
                bucket: Some(bucket.bucket()),
                versioning_configuration: Some(
                    types::S3BucketVersioningVersioningConfigurationArgs {
                        status: Some(s("Enabled").cast()),
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
            none(),
        )
    });

    let rule = |rule_id: &str| types::S3BucketLifecycleConfigurationV2RuleArgs {
        id: Some(s(rule_id).cast()),
        status: Some(s("Enabled").cast()),
        filter: Some(types::S3BucketLifecycleConfigurationV2RuleFilterArgs::default()),
        ..Default::default()
    };
    let mut rules = vec![types::S3BucketLifecycleConfigurationV2RuleArgs {
        abort_incomplete_multipart_upload: Some(
            types::S3BucketLifecycleConfigurationV2RuleAbortIncompleteMultipartUploadArgs {
                days_after_initiation: Some(pv::number(7.0).cast()),
            },
        ),
        ..rule("abort-incomplete-multipart")
    }];
    if versioned {
        rules.push(types::S3BucketLifecycleConfigurationV2RuleArgs {
            noncurrent_version_expiration: Some(
                types::S3BucketLifecycleConfigurationV2RuleNoncurrentVersionExpirationArgs {
                    noncurrent_days: Some(pv::number(30.0).cast()),
                    ..Default::default()
                },
            ),
            ..rule("expire-noncurrent-versions")
        });
    }
    if let Some(days) = retention_days {
        rules.push(types::S3BucketLifecycleConfigurationV2RuleArgs {
            expiration: Some(types::S3BucketLifecycleConfigurationV2RuleExpirationArgs {
                days: Some(days.cast()),
                ..Default::default()
            }),
            ..rule("expire-current")
        });
    }
    s3::BucketLifecycleConfigurationV2::new(
        ctx,
        &format!("{id}-lifecycle"),
        s3::BucketLifecycleConfigurationV2Args {
            bucket: Some(bucket.bucket()),
            rules: Some(rules),
            ..Default::default()
        },
        pulumi::ResourceOptions {
            // Lifecycle rules for noncurrent versions need versioning enabled first.
            depends_on: versioning
                .iter()
                .map(|v| v.pulumi_resource().clone())
                .collect(),
            ..Default::default()
        },
    );
}

/// Input and output are separate buckets so writing results can never re-trigger the notification.
fn buckets(ctx: &pulumi::Context, force_destroy: Pv) -> (s3::Bucket, s3::Bucket) {
    let raw = s3::Bucket::new(
        ctx,
        "raw",
        s3::BucketArgs {
            force_destroy: Some(force_destroy.cast()),
            ..Default::default()
        },
        pulumi::ResourceOptions::default(),
    );
    let out = s3::Bucket::new(
        ctx,
        "extracted",
        s3::BucketArgs {
            force_destroy: Some(force_destroy.cast()),
            ..Default::default()
        },
        pulumi::ResourceOptions::default(),
    );

    // `rawRetentionDays` / `outputRetentionDays` (optional) expire objects; unset keeps them indefinitely.
    harden_bucket(
        ctx,
        "raw",
        &raw,
        true,
        ctx.config().get_int_opt("rawRetentionDays"),
    );
    harden_bucket(
        ctx,
        "extracted",
        &out,
        false,
        ctx.config().get_int_opt("outputRetentionDays"),
    );

    (raw, out)
}

/// Work queue plus dead-letter queue, and the bucket notification that feeds the work queue.
fn queues(ctx: &pulumi::Context, raw: &s3::Bucket) -> (sqs::Queue, sqs::Queue) {
    // Queues: PDFs that fail MAX_RECEIVE_COUNT times land in the DLQ.
    let dlq = sqs::Queue::new(
        ctx,
        "dlq",
        sqs::QueueArgs {
            message_retention_seconds: Some(pv::number(1_209_600.0).cast()),
            sqs_managed_sse_enabled: Some(pv::bool(true).cast()),
            ..Default::default()
        },
        pulumi::ResourceOptions::default(),
    );
    let queue = sqs::Queue::new(
        ctx,
        "queue",
        sqs::QueueArgs {
            visibility_timeout_seconds: Some(pv::number(f64::from(VISIBILITY_TIMEOUT_SECS)).cast()),
            sqs_managed_sse_enabled: Some(pv::bool(true).cast()),
            redrive_policy: Some(
                pv::to_json(obj(vec![
                    ("deadLetterTargetArn", dlq.arn().cast()),
                    ("maxReceiveCount", pv::number(MAX_RECEIVE_COUNT)),
                ]))
                .cast(),
            ),
            ..Default::default()
        },
        pulumi::ResourceOptions::default(),
    );

    // S3 may only send to the queue if the queue says so, and S3 checks
    // that when the notification is created, hence the explicit ordering.
    let queue_policy = sqs::QueuePolicy::new(
        ctx,
        "queue-policy",
        sqs::QueuePolicyArgs {
            queue_url: Some(queue.url().cast()),
            policy: Some(
                policy(vec![obj(vec![
                    ("Effect", s("Allow")),
                    ("Principal", obj(vec![("Service", s("s3.amazonaws.com"))])),
                    ("Action", s("sqs:SendMessage")),
                    ("Resource", queue.arn().cast()),
                    (
                        "Condition",
                        obj(vec![(
                            "ArnEquals",
                            obj(vec![("aws:SourceArn", raw.arn().cast())]),
                        )]),
                    ),
                ])])
                .cast(),
            ),
            ..Default::default()
        },
        pulumi::ResourceOptions::default(),
    );
    s3::BucketNotification::new(
        ctx,
        "raw-notification",
        s3::BucketNotificationArgs {
            bucket: Some(raw.bucket().cast()),
            queues: Some(vec![types::S3BucketNotificationQueueArgs {
                queue_arn: Some(queue.arn().cast()),
                events: Some(pv::array(vec![s("s3:ObjectCreated:*")]).cast()),
                filter_prefix: Some(s("raw/").cast()),
                filter_suffix: Some(s(".pdf").cast()),
                ..Default::default()
            }]),
            ..Default::default()
        },
        pulumi::ResourceOptions {
            depends_on: vec![queue_policy.pulumi_resource().clone()],
            ..Default::default()
        },
    );

    (queue, dlq)
}

/// Log group and a least-privilege execution role for exactly what the handler does.
fn execution_role(
    ctx: &pulumi::Context,
    raw: &s3::Bucket,
    out: &s3::Bucket,
    queue: &sqs::Queue,
) -> (iam::Role, iam::RolePolicy, cloudwatch::LogGroup) {
    // Execution role: least privilege for exactly what the handler does.
    let log_group = cloudwatch::LogGroup::new(
        ctx,
        "logs",
        cloudwatch::LogGroupArgs {
            name: Some(pv::concat(vec![s("/aws/lambda/"), s(FUNCTION_NAME)]).cast()),
            retention_in_days: Some(pv::number(30.0).cast()),
            ..Default::default()
        },
        pulumi::ResourceOptions::default(),
    );
    let role = iam::Role::new(
        ctx,
        "role",
        iam::RoleArgs {
            assume_role_policy: Some(s(LAMBDA_ASSUME_ROLE).cast()),
            ..Default::default()
        },
        pulumi::ResourceOptions::default(),
    );
    let role_policy = iam::RolePolicy::new(
        ctx,
        "role-policy",
        iam::RolePolicyArgs {
            role: Some(role.name().cast()),
            policy: Some(
                policy(vec![
                    allow(
                        &["s3:GetObject"],
                        vec![pv::concat(vec![raw.arn().cast(), s("/*")])],
                    ),
                    allow(
                        &["s3:PutObject"],
                        vec![pv::concat(vec![out.arn().cast(), s("/*")])],
                    ),
                    allow(
                        &[
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes",
                        ],
                        vec![queue.arn().cast()],
                    ),
                    allow(
                        &["logs:CreateLogStream", "logs:PutLogEvents"],
                        vec![pv::concat(vec![log_group.arn().cast(), s(":*")])],
                    ),
                    // X-Ray does not support resource-level permissions.
                    allow(
                        &["xray:PutTraceSegments", "xray:PutTelemetryRecords"],
                        vec![s("*")],
                    ),
                ])
                .cast(),
            ),
            ..Default::default()
        },
        pulumi::ResourceOptions::default(),
    );

    (role, role_policy, log_group)
}

fn function(
    ctx: &pulumi::Context,
    code_dir: Pv,
    out: &s3::Bucket,
    role: &iam::Role,
    role_policy: &iam::RolePolicy,
    log_group: &cloudwatch::LogGroup,
) -> lambda::Function {
    let mut env = vec![
        ("OUTPUT_BUCKET", out.bucket().cast()),
        ("OUTPUT_PREFIX", s(OUTPUT_PREFIX)),
        ("MAX_PDF_BYTES", s(&MAX_PDF_BYTES.to_string())),
    ];
    // Emulators only (Floci): point the function's AWS SDK at them.
    if let Some(endpoint) = ctx.config().get_string_opt("endpointUrl") {
        env.push(("AWS_ENDPOINT_URL", endpoint));
        env.push(("S3_FORCE_PATH_STYLE", s("1")));
    }

    lambda::Function::new(
        ctx,
        "extract",
        lambda::FunctionArgs {
            name: Some(s(FUNCTION_NAME).cast()),
            role: Some(role.arn().cast()),
            code: Some(pv::file_archive(code_dir).cast()),
            handler: Some(s("bootstrap").cast()),
            runtime: Some(s("provided.al2023").cast()),
            architectures: Some(pv::array(vec![s("arm64")]).cast()),
            memory_size: Some(pv::number(f64::from(LAMBDA_MEMORY_MB)).cast()),
            timeout: Some(pv::number(f64::from(LAMBDA_TIMEOUT_SECS)).cast()),
            tracing_config: Some(types::LambdaFunctionTracingConfigArgs {
                mode: Some(s("Active").cast()),
            }),
            logging_config: Some(types::LambdaFunctionLoggingConfigArgs {
                log_format: Some(s("JSON").cast()),
                log_group: Some(log_group.name().cast()),
                ..Default::default()
            }),
            environment: Some(types::LambdaFunctionEnvironmentArgs {
                variables: Some(obj(env).cast()),
            }),
            ..Default::default()
        },
        pulumi::ResourceOptions {
            depends_on: vec![role_policy.pulumi_resource().clone()],
            ..Default::default()
        },
    )
}

fn connect_queue(
    ctx: &pulumi::Context,
    queue: &sqs::Queue,
    function: &lambda::Function,
    role_policy: &iam::RolePolicy,
    max_concurrency: Pv,
) {
    // One PDF per invocation, with a cap on parallel invocations.
    lambda::EventSourceMapping::new(
        ctx,
        "queue-to-extract",
        lambda::EventSourceMappingArgs {
            event_source_arn: Some(queue.arn().cast()),
            function_name: Some(function.name().cast()),
            batch_size: Some(pv::number(1.0).cast()),
            // The handler returns `batchItemFailures`; without this SQS ignores them.
            function_response_types: Some(pv::array(vec![s("ReportBatchItemFailures")]).cast()),
            scaling_config: Some(types::LambdaEventSourceMappingScalingConfigArgs {
                maximum_concurrency: Some(max_concurrency.cast()),
            }),
            ..Default::default()
        },
        pulumi::ResourceOptions {
            depends_on: vec![role_policy.pulumi_resource().clone()],
            ..Default::default()
        },
    );
}

/// Alarm topic (not on an emulator) and every alarm. Returns the topic for export.
fn alarms(ctx: &pulumi::Context, dlq: &sqs::Queue) -> Option<sns::Topic> {
    // Alarms notify this topic; `alarmEmail` (optional) subscribes an address, which must confirm by email.
    // Skipped on an emulator (`endpointUrl` set): without an account id the provider cannot build the topic ARN,
    // and emulators may lack the S3 Control API needed when it does.
    let on_emulator = ctx.config().get_string_opt("endpointUrl").is_some();
    let alarms_topic = (!on_emulator).then(|| {
        let topic = sns::Topic::new(
            ctx,
            "alarms",
            sns::TopicArgs::default(),
            pulumi::ResourceOptions::default(),
        );
        if let Some(email) = ctx.config().get_string_opt("alarmEmail") {
            sns::TopicSubscription::new(
                ctx,
                "alarms-email",
                sns::TopicSubscriptionArgs {
                    topic: Some(topic.arn().cast()),
                    protocol: Some(s("email").cast()),
                    endpoint: Some(email.cast()),
                    ..Default::default()
                },
                pulumi::ResourceOptions::default(),
            );
        }
        topic
    });

    // Alarms on the DLQ, on the custom metrics the handler emits (EMF) and on Lambda's own error count.
    let alarm = |id: &str, description: &str, namespace: &str, metric: &str, dim: (&str, Pv)| {
        cloudwatch::MetricAlarm::new(
            ctx,
            id,
            cloudwatch::MetricAlarmArgs {
                alarm_description: Some(s(description).cast()),
                alarm_actions: alarms_topic
                    .as_ref()
                    .map(|t| pulumi::output::all(vec![t.arn().cast()])),
                namespace: Some(s(namespace).cast()),
                metric_name: Some(s(metric).cast()),
                dimensions: Some(obj(vec![dim]).cast()),
                statistic: Some(s("Sum").cast()),
                period: Some(pv::number(300.0).cast()),
                evaluation_periods: Some(pv::number(1.0).cast()),
                threshold: Some(pv::number(0.0).cast()),
                comparison_operator: Some(s("GreaterThanThreshold").cast()),
                treat_missing_data: Some(s("notBreaching").cast()),
                ..Default::default()
            },
            pulumi::ResourceOptions::default(),
        )
    };
    alarm(
        "alarm-dlq",
        "PDFs are in the dead-letter queue",
        "AWS/SQS",
        "ApproximateNumberOfMessagesVisible",
        ("QueueName", dlq.name().cast()),
    );
    // Timeouts, out-of-memory kills and crashes never reach the handler's own `Failures` metric.
    alarm(
        "alarm-lambda-errors",
        "Lambda invocations errored (timeout, out of memory, crash or retryable failure)",
        "AWS/Lambda",
        "Errors",
        ("FunctionName", s(FUNCTION_NAME)),
    );
    let fn_dim = || ("Function", s(FUNCTION_NAME));
    alarm(
        "alarm-failures",
        "Extraction invocations failed",
        "PdfExtract",
        "Failures",
        fn_dim(),
    );
    alarm(
        "alarm-pages-failed",
        "Pages failed inside otherwise successful documents",
        "PdfExtract",
        "PagesFailed",
        fn_dim(),
    );
    alarm(
        "alarm-pages-scanned",
        "Pages without a text layer need OCR",
        "PdfExtract",
        "PagesScanned",
        fn_dim(),
    );

    alarms_topic
}

fn main() {
    pulumi::run(|ctx| async move {
        let force_destroy = ctx
            .config()
            .get_bool_or("forceDestroy", pulumi::PropertyValue::Bool(false));
        let code_dir = ctx.config().get_string_or(
            "codeDir",
            pulumi::PropertyValue::String("lambda-package".into()),
        );
        let max_concurrency = ctx
            .config()
            .get_number_or("maxConcurrency", pulumi::PropertyValue::Number(5.0));

        let (raw, out) = buckets(&ctx, force_destroy);
        let (queue, dlq) = queues(&ctx, &raw);
        let (role, role_policy, log_group) = execution_role(&ctx, &raw, &out, &queue);
        let function = function(&ctx, code_dir, &out, &role, &role_policy, &log_group);
        connect_queue(&ctx, &queue, &function, &role_policy, max_concurrency);
        let alarms_topic = alarms(&ctx, &dlq);

        ctx.export("rawBucket", raw.bucket().cast::<pulumi::PropertyValue>());
        ctx.export("outputBucket", out.bucket().cast::<pulumi::PropertyValue>());
        ctx.export("queueUrl", queue.url().cast::<pulumi::PropertyValue>());
        ctx.export("dlqUrl", dlq.url().cast::<pulumi::PropertyValue>());
        if let Some(topic) = &alarms_topic {
            ctx.export(
                "alarmsTopicArn",
                topic.arn().cast::<pulumi::PropertyValue>(),
            );
        }
        ctx.export(
            "functionName",
            function.name().cast::<pulumi::PropertyValue>(),
        );

        Ok(())
    });
}
