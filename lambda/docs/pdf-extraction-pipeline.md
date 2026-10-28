# PDF Extraction Pipeline

Companion guide to `pdf-extraction-pipeline.drawio` (open in draw.io / diagrams.net).
Target design: not yet deployed or validated.

## Flow

1. Upstream systems **PUT** a PDF into the S3 raw bucket (`raw/*.pdf`).
2. S3 raises an **ObjectCreated** notification into an SQS queue.
3. The Lambda polls the queue with **batch size 1**, so memory use follows a single PDF.
4. The Lambda calls **GetObject** on the raw bucket for the key in the event and reads the whole PDF into memory.
5. It extracts every page, then **PutObject**s two files to the separate S3 output bucket: `<key>.json` and `<key>.md`.
6. The output bucket's ObjectCreated event on the `.json` file notifies the next stage (EventBridge, SNS or SQS).
7. A message received 3 times without success moves to the **dead-letter queue**.

## Services

| Service | Purpose |
|---|---|
| S3 raw bucket | Input PDFs |
| SQS queue | Absorbs bursts, retries failures, lets concurrency be capped |
| SQS dead-letter queue | Holds PDFs that failed 3 times |
| Lambda (Rust, arm64, `provided.al2023`) | Per-document extraction to JSON and Markdown |
| S3 output bucket | Structured results, separate from input so writes cannot re-trigger the pipeline |
| CloudWatch Logs | JSON `tracing` logs with S3 key and per-page timings |
| CloudWatch Metrics | Embedded Metric Format lines, namespace `PdfExtract` |
| CloudWatch Alarms | DLQ depth, `Failures`, `PagesFailed`, `PagesScanned` |
| X-Ray | Active tracing of invocations |
| EventBridge / SNS | Hand-off to downstream stages (outside this stack) |

## Key decisions

- **SQS between S3 and Lambda** for retries, a DLQ and a concurrency cap (`MaximumConcurrency` on the event source mapping).
- **Visibility timeout of at least 6x the Lambda timeout**, so a slow PDF is not processed twice in parallel.
- **Separate output bucket** to prevent trigger loops.
- **Metrics as EMF log lines**, so no extension or API calls are needed on the Lambda.
- **`PagesScanned`** counts pages with no text layer, which need OCR downstream.

## Layout notes

- Dashed black edges are reads/async hand-offs; the red dashed edge is the failure path.
- The S3 raw bucket connects to the Lambda twice: the notification goes through SQS, while the file itself is fetched directly with GetObject.
