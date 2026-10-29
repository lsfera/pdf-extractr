# Deploys the pipeline (Pulumi) to a running Floci and exercises it. Runs inside the test image
# (see ../Dockerfile.test) but works anywhere pulumi, the rust language plugin, aws, jq and curl are on PATH.
#
#   deploy   create the stack            destroy  remove it
#   e2e      deploy, assert, destroy     dev      deploy and wait for uploads until SIGINT/SIGTERM
#            (no destroy: the compose stack and its Floci are discarded on exit)
#
# Floci must have the docker socket mounted and FLOCI_SERVICES_LAMBDA_DOCKER_NETWORK set so Lambda containers reach it
# at LS_INTERNAL_URL. lambda-package/bootstrap must exist.
SHELL := bash
.SHELLFLAGS := -euo pipefail -c
.ONESHELL:
.SILENT:
MAKEFLAGS += --no-print-directory
.PHONY: deploy destroy assert e2e dev info

LS_URL          ?= http://localhost:4566
LS_INTERNAL_URL ?= http://pdf-floci:4566
FIXTURE         ?= ../lambda/tests/fixtures/1.pdf
STACK           := ci

export AWS_ACCESS_KEY_ID = test
export AWS_SECRET_ACCESS_KEY = test
export AWS_REGION = us-east-1
export AWS_DEFAULT_REGION = us-east-1
export AWS_ENDPOINT_URL = $(LS_URL)
export PULUMI_CONFIG_PASSPHRASE =
export PULUMI_BACKEND_URL = file://$(CURDIR)/.pulumi-state

define PRELUDE
awsls() { aws --endpoint-url "$(LS_URL)" "$$@"; }
fail() { echo "FAIL: $$*" >&2; exit 1; }
retry() { local deadline=$$((SECONDS + $$1)); shift; until "$$@" >/dev/null 2>&1; do ((SECONDS < deadline)) || return 1; sleep 2; done; }
out() { pulumi stack output "$$1" --stack $(STACK); }
endef

deploy:
	[[ -f lambda-package/bootstrap ]] || { echo "lambda-package/bootstrap is missing" >&2; exit 1; }
	mkdir -p .pulumi-state
	pulumi stack select $(STACK) --create >/dev/null
	pulumi config set aws:region us-east-1
	pulumi config set aws:skipCredentialsValidation true
	pulumi config set aws:skipRequestingAccountId true
	pulumi config set aws:skipMetadataApiCheck true
	pulumi config set aws:s3UsePathStyle true
	pulumi config set forceDestroy true
	pulumi config set endpointUrl "$(LS_INTERNAL_URL)"
	echo "== pulumi up"
	pulumi up --yes --skip-preview --stack $(STACK)

destroy:
	pulumi destroy --yes --stack $(STACK) || echo "warning: pulumi destroy failed" >&2
	pulumi stack rm --yes --force --stack $(STACK) >/dev/null 2>&1 || true
	rm -rf .pulumi-state Pulumi.$(STACK).yaml

assert:
	$(PRELUDE)
	[[ -f $(FIXTURE) ]] || fail "fixture $(FIXTURE) is missing"
	RAW=$$(out rawBucket); OUT=$$(out outputBucket); QUEUE=$$(out queueUrl); DLQ=$$(out dlqUrl); FUNCTION=$$(out functionName)

	echo "== queue wiring"
	redrive=$$(awsls sqs get-queue-attributes --queue-url "$$QUEUE" --attribute-names RedrivePolicy \
	  --query Attributes.RedrivePolicy --output text)
	jq -e '.maxReceiveCount == 3' <<<"$$redrive" >/dev/null || fail "unexpected redrive policy: $$redrive"

	echo "== happy path (key contains a space)"
	KEY="raw/statement one.pdf"; OUT_KEY="extracted/$$KEY"
	awsls s3 cp "$(FIXTURE)" "s3://$$RAW/$$KEY" --only-show-errors
	retry 120 awsls s3api head-object --bucket "$$OUT" --key "$$OUT_KEY.json" || fail "no $$OUT_KEY.json after 120s"
	retry 30 awsls s3api head-object --bucket "$$OUT" --key "$$OUT_KEY.md" || fail "no $$OUT_KEY.md"
	awsls s3 cp "s3://$$OUT/$$OUT_KEY.json" - --only-show-errors | jq -e '
	  .page_count > 0
	  and (.pages | length) == .page_count
	  and all(.pages[]; (.error // null) == null)' >/dev/null || fail "extraction JSON failed validation"
	md_bytes=$$(awsls s3 cp "s3://$$OUT/$$OUT_KEY.md" - --only-show-errors | wc -c)
	((md_bytes > 0)) || fail "markdown output is empty"

	echo "== notification filters"
	awsls s3 cp "$(FIXTURE)" "s3://$$RAW/raw/ignored.txt" --only-show-errors
	awsls s3 cp "$(FIXTURE)" "s3://$$RAW/other/ignored.pdf" --only-show-errors
	sleep 15
	count=$$(awsls s3api list-objects-v2 --bucket "$$OUT" --query 'length(Contents)' --output text)
	[[ $$count == 2 ]] || fail "expected exactly 2 outputs, found $$count"

	echo "== dead-letter queue is empty"
	depth=$$(awsls sqs get-queue-attributes --queue-url "$$DLQ" --attribute-names ApproximateNumberOfMessages \
	  --query Attributes.ApproximateNumberOfMessages --output text)
	[[ $$depth == 0 ]] || fail "DLQ holds $$depth message(s)"

	echo "== metrics line in CloudWatch Logs"
	has_emf() { [[ $$(awsls logs filter-log-events --log-group-name "/aws/lambda/$$FUNCTION" --filter-pattern PdfExtract \
	  --query 'length(events)' --output text) -gt 0 ]]; }
	retry 60 has_emf || fail "no PdfExtract metrics line in /aws/lambda/$$FUNCTION"
	echo PASS

e2e:
	$(MAKE) deploy
	status=0
	$(MAKE) assert || status=$$?
	$(MAKE) destroy
	exit $$status

info:
	$(PRELUDE)
	cat <<MSG

	Pipeline is up. Floci is published on the Docker host at port 4566.
	  raw bucket    : $$(out rawBucket)   (upload PDFs under raw/)
	  output bucket : $$(out outputBucket)   (results appear under extracted/raw/<name>.pdf.json and .md)

	  export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
	  aws --endpoint-url http://localhost:4566 s3 cp my.pdf s3://$$(out rawBucket)/raw/my.pdf
	  aws --endpoint-url http://localhost:4566 s3 ls s3://$$(out outputBucket)/ --recursive

	Press Ctrl-C (or run 'make down') to tear everything down.
	MSG

dev:
	trap 'exit 0' INT TERM
	$(MAKE) deploy
	$(MAKE) info
	sleep infinity & wait
