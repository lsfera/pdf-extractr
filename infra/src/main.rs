//! PDF extraction pipeline: S3 raw bucket -> SQS -> Lambda -> S3 output bucket.
//!
//! The Lambda is the Rust binary from `../lambda`, packaged as
//! `bootstrap` in the directory named by the `codeDir` config value (default
//! `lambda-package`). The same program deploys to LocalStack or AWS; only the
//! stack's `aws:*` config differs.
//!
//! Generate the provider SDK first (see README.md):
//! `pulumi package gen-sdk aws@7.41.0 --language rust --out ./sdks/aws`

use pulumi::pv;
use pulumi_aws::{cloudwatch, iam, lambda, s3, sqs, types};

const FUNCTION_NAME: &str = "pdf-extract";
/// Lambda timeout. The queue's visibility timeout is derived from it.
const LAMBDA_TIMEOUT_SECS: i32 = 120;
/// Starting point; size it from the `Max Memory Used` field of the REPORT log line.
const LAMBDA_MEMORY_MB: i32 = 1024;
/// SQS visibility timeout must comfortably exceed the function timeout, or a
/// slow PDF is delivered again while still being processed. AWS recommends 6x.
const VISIBILITY_TIMEOUT_SECS: i32 = 6 * LAMBDA_TIMEOUT_SECS;
const MAX_RECEIVE_COUNT: f64 = 3.0;
const MAX_PDF_BYTES: &str = "104857600";
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
    pv::object(fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
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

fn main() {
    pulumi::run(|ctx| async move {
        let force_destroy = ctx
            .config()
            .get_bool_or("forceDestroy", pulumi::PropertyValue::Bool(false));
        let code_dir = ctx
            .config()
            .get_string_or("codeDir", pulumi::PropertyValue::String("lambda-package".into()));
        let max_concurrency = ctx
            .config()
            .get_number_or("maxConcurrency", pulumi::PropertyValue::Number(5.0));

        // Buckets. Input and output are separate so writing results can never
        // re-trigger the notification.
        let raw = s3::Bucket::new(
            &ctx,
            "raw",
            s3::BucketArgs {
                force_destroy: Some(force_destroy.cast()),
                ..Default::default()
            },
            pulumi::ResourceOptions::default(),
        );
        let out = s3::Bucket::new(
            &ctx,
            "extracted",
            s3::BucketArgs {
                force_destroy: Some(force_destroy.cast()),
                ..Default::default()
            },
            pulumi::ResourceOptions::default(),
        );

        // Queues: PDFs that fail MAX_RECEIVE_COUNT times land in the DLQ.
        let dlq = sqs::Queue::new(
            &ctx,
            "dlq",
            sqs::QueueArgs {
                message_retention_seconds: Some(pv::number(1_209_600.0).cast()),
                ..Default::default()
            },
            pulumi::ResourceOptions::default(),
        );
        let queue = sqs::Queue::new(
            &ctx,
            "queue",
            sqs::QueueArgs {
                visibility_timeout_seconds: Some(pv::number(f64::from(VISIBILITY_TIMEOUT_SECS)).cast()),
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
            &ctx,
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
            &ctx,
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

        // Execution role: least privilege for exactly what the handler does.
        let log_group = cloudwatch::LogGroup::new(
            &ctx,
            "logs",
            cloudwatch::LogGroupArgs {
                name: Some(pv::concat(vec![s("/aws/lambda/"), s(FUNCTION_NAME)]).cast()),
                retention_in_days: Some(pv::number(30.0).cast()),
                ..Default::default()
            },
            pulumi::ResourceOptions::default(),
        );
        let role = iam::Role::new(
            &ctx,
            "role",
            iam::RoleArgs {
                assume_role_policy: Some(s(LAMBDA_ASSUME_ROLE).cast()),
                ..Default::default()
            },
            pulumi::ResourceOptions::default(),
        );
        let role_policy = iam::RolePolicy::new(
            &ctx,
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
                            &["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
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

        let mut env = vec![
            ("OUTPUT_BUCKET", out.bucket().cast()),
            ("OUTPUT_PREFIX", s(OUTPUT_PREFIX)),
            ("MAX_PDF_BYTES", s(MAX_PDF_BYTES)),
        ];
        // Emulators only (LocalStack): point the function's AWS SDK at them.
        if let Some(endpoint) = ctx.config().get_string_opt("endpointUrl") {
            env.push(("AWS_ENDPOINT_URL", endpoint));
            env.push(("S3_FORCE_PATH_STYLE", s("1")));
        }

        let function = lambda::Function::new(
            &ctx,
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
        );

        // One PDF per invocation, with a cap on parallel invocations.
        lambda::EventSourceMapping::new(
            &ctx,
            "queue-to-extract",
            lambda::EventSourceMappingArgs {
                event_source_arn: Some(queue.arn().cast()),
                function_name: Some(function.name().cast()),
                batch_size: Some(pv::number(1.0).cast()),
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

        // Alarms on the DLQ and on the custom metrics the handler emits (EMF).
        let alarm = |id: &str, description: &str, namespace: &str, metric: &str, dim: (&str, Pv)| {
            cloudwatch::MetricAlarm::new(
                &ctx,
                id,
                cloudwatch::MetricAlarmArgs {
                    alarm_description: Some(s(description).cast()),
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
        let fn_dim = || ("Function", s(FUNCTION_NAME));
        alarm("alarm-failures", "Extraction invocations failed", "PdfExtract", "Failures", fn_dim());
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

        ctx.export("rawBucket", raw.bucket().cast::<pulumi::PropertyValue>());
        ctx.export("outputBucket", out.bucket().cast::<pulumi::PropertyValue>());
        ctx.export("queueUrl", queue.url().cast::<pulumi::PropertyValue>());
        ctx.export("dlqUrl", dlq.url().cast::<pulumi::PropertyValue>());
        ctx.export("functionName", function.name().cast::<pulumi::PropertyValue>());

        Ok(())
    });
}
