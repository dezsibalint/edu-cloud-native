#!/bin/bash
# Run the observability test cases described in k8shw1.pdf.

set -u
set -o pipefail

TARGET="${1:-${PUBLIC_IP:-}}"
TEST_FILE="${TEST_FILE:-test.pdf}"
EXPECTED_PAGES="${EXPECTED_PAGES:-2}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-240}"
RUN_ID="$(date +%s)"

PASS_COUNT=0
FAIL_COUNT=0

if [ -z "$TARGET" ]; then
    echo "Usage: $0 <public-ip-or-url>"
    echo "Example: $0 34.228.25.69"
    echo "Optional env vars: TEST_FILE=test.pdf EXPECTED_PAGES=2 TIMEOUT_SECONDS=240"
    exit 2
fi

TARGET="${TARGET%/}"
TARGET="${TARGET#http://}"
TARGET="${TARGET#https://}"
APP_URL="http://${TARGET}:30080"
PROM_URL="http://${TARGET}:30900"

pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    echo "[PASS] $1"
}

fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    echo "[FAIL] $1"
    if [ $# -gt 1 ]; then
        echo "       $2"
    fi
}

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        fail "Missing command: $1"
        return 1
    fi
    return 0
}

prom_query_json() {
    curl -fsG --data-urlencode "query=$1" "${PROM_URL}/api/v1/query"
}

prom_value() {
    prom_query_json "$1" | python3 -c '
import json, sys
data = json.load(sys.stdin)
results = data.get("data", {}).get("result", [])
if not results:
    print("0")
else:
    print(results[0]["value"][1])
'
}

prom_series_count() {
    prom_query_json "$1" | python3 -c '
import json, sys
data = json.load(sys.stdin)
print(len(data.get("data", {}).get("result", [])))
'
}

float_gt() {
    python3 - "$1" "$2" <<'PY'
import sys
left = float(sys.argv[1])
right = float(sys.argv[2])
sys.exit(0 if left > right else 1)
PY
}

float_ge() {
    python3 - "$1" "$2" <<'PY'
import sys
left = float(sys.argv[1])
right = float(sys.argv[2])
sys.exit(0 if left >= right else 1)
PY
}

wait_value_gt() {
    query="$1"
    baseline="$2"
    timeout="${3:-$TIMEOUT_SECONDS}"
    start="$(date +%s)"

    while true; do
        value="$(prom_value "$query" 2>/dev/null || echo 0)"
        if float_gt "$value" "$baseline"; then
            echo "$value"
            return 0
        fi

        now="$(date +%s)"
        if [ $((now - start)) -ge "$timeout" ]; then
            echo "$value"
            return 1
        fi
        sleep 5
    done
}

wait_series() {
    query="$1"
    timeout="${2:-$TIMEOUT_SECONDS}"
    start="$(date +%s)"

    while true; do
        count="$(prom_series_count "$query" 2>/dev/null || echo 0)"
        if [ "$count" -gt 0 ]; then
            return 0
        fi

        now="$(date +%s)"
        if [ $((now - start)) -ge "$timeout" ]; then
            return 1
        fi
        sleep 5
    done
}

test_filegrab_metrics() {
    body="$(curl -fsS "${APP_URL}/metrics" 2>&1)"
    if echo "$body" | grep -q "component_execution_seconds"; then
        pass "TC1 FileGrab /metrics returns Prometheus metrics"
    else
        fail "TC1 FileGrab /metrics returns Prometheus metrics" "$body"
    fi
}

test_worker_metrics() {
    local all_ok=1
    for svc in pdf-to-image preprocessing ocr text-aggregation; do
        pod_name="obs-curl-${svc//-/}-${RUN_ID}"
        body="$(kubectl run "$pod_name" --quiet --rm -i --restart=Never --image=curlimages/curl:8.11.1 --command -- curl -fsS --max-time 10 "http://${svc}:8000/metrics" 2>&1 || true)"
        if echo "$body" | grep -q "component_execution_seconds"; then
            echo "       ${svc}: direct /metrics OK"
            continue
        fi

        up_value="$(prom_value "up{job=\"${svc}\"}" 2>/dev/null || echo 0)"
        if [ "$up_value" = "1" ]; then
            echo "       ${svc}: Prometheus scrape UP"
            continue
        fi

        echo "       ${svc}: metrics unavailable"
        echo "$body" | sed 's/^/       /'
        all_ok=0
    done

    if [ "$all_ok" -eq 1 ]; then
        pass "TC2 Worker /metrics endpoints are reachable"
    else
        fail "TC2 Worker /metrics endpoints are reachable"
    fi
}

upload_valid_file() {
    if [ ! -f "$TEST_FILE" ]; then
        fail "Valid test input exists" "Missing file: $TEST_FILE"
        return 1
    fi

    response="$(curl -fsS -X POST -F "file=@${TEST_FILE}" "${APP_URL}/upload" 2>&1)"
    job_id="$(printf '%s' "$response" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("job_id", ""))
except Exception:
    print("")
'
)"

    if [ -n "$job_id" ]; then
        echo "       Uploaded ${TEST_FILE}; job_id=${job_id}"
        return 0
    fi

    fail "Upload valid test file" "$response"
    return 1
}

test_upload_metrics_and_pipeline() {
    upload_requests_before="$(prom_value 'sum(upload_requests_total)' 2>/dev/null || echo 0)"
    upload_duration_before="$(prom_value 'sum(component_execution_seconds_count{job="filegrab"})' 2>/dev/null || echo 0)"
    page_count_before="$(prom_value 'sum(document_page_count_sum)' 2>/dev/null || echo 0)"
    elapsed_count_before="$(prom_value 'sum(document_upload_to_finish_seconds_count)' 2>/dev/null || echo 0)"
    aggregation_count_before="$(prom_value 'sum(document_upload_to_finish_seconds_count{job="text-aggregation"})' 2>/dev/null || echo 0)"
    total_work_before="$(prom_value 'sum(document_total_work_seconds_sum)' 2>/dev/null || echo 0)"

    if ! upload_valid_file; then
        return
    fi

    upload_requests_after="$(wait_value_gt 'sum(upload_requests_total)' "$upload_requests_before" 90 || true)"
    upload_duration_after="$(wait_value_gt 'sum(component_execution_seconds_count{job="filegrab"})' "$upload_duration_before" 90 || true)"
    if float_gt "$upload_requests_after" "$upload_requests_before" && float_gt "$upload_duration_after" "$upload_duration_before"; then
        pass "TC3 Upload request counter and upload duration are recorded"
    else
        fail "TC3 Upload request counter and upload duration are recorded" "upload_requests: ${upload_requests_before} -> ${upload_requests_after}, filegrab duration count: ${upload_duration_before} -> ${upload_duration_after}"
    fi

    page_count_after="$(wait_value_gt 'sum(document_page_count_sum)' "$page_count_before" "$TIMEOUT_SECONDS" || true)"
    page_delta="$(python3 - "$page_count_after" "$page_count_before" <<'PY'
import sys
print(float(sys.argv[1]) - float(sys.argv[2]))
PY
)"
    if float_ge "$page_delta" "$EXPECTED_PAGES"; then
        pass "TC4 Page count metric increases by at least expected page count (${EXPECTED_PAGES})"
    else
        fail "TC4 Page count metric increases by at least expected page count (${EXPECTED_PAGES})" "document_page_count_sum delta=${page_delta}"
    fi

    elapsed_count_after="$(wait_value_gt 'sum(document_upload_to_finish_seconds_count)' "$elapsed_count_before" "$TIMEOUT_SECONDS" || true)"
    elapsed_sum="$(prom_value 'sum(document_upload_to_finish_seconds_sum)' 2>/dev/null || echo 0)"
    if float_gt "$elapsed_count_after" "$elapsed_count_before" && float_gt "$elapsed_sum" "0"; then
        pass "TC5 Elapsed upload-to-finish metrics are positive"
    else
        fail "TC5 Elapsed upload-to-finish metrics are positive" "count: ${elapsed_count_before} -> ${elapsed_count_after}, sum=${elapsed_sum}"
    fi

    aggregation_count_after="$(wait_value_gt 'sum(document_upload_to_finish_seconds_count{job="text-aggregation"})' "$aggregation_count_before" "$TIMEOUT_SECONDS" || true)"
    aggregation_pages="$(prom_value 'sum(document_page_count_sum{job="text-aggregation"})' 2>/dev/null || echo 0)"
    if float_gt "$aggregation_count_after" "$aggregation_count_before" && float_ge "$aggregation_pages" "$EXPECTED_PAGES"; then
        pass "TC6 Text aggregation completion and page count metrics are recorded"
    else
        fail "TC6 Text aggregation completion and page count metrics are recorded" "aggregation count: ${aggregation_count_before} -> ${aggregation_count_after}, pages=${aggregation_pages}"
    fi

    total_work_after="$(wait_value_gt 'sum(document_total_work_seconds_sum)' "$total_work_before" "$TIMEOUT_SECONDS" || true)"
    end_to_end_sum="$(prom_value 'sum(document_upload_to_finish_seconds_sum{job="text-aggregation"})' 2>/dev/null || echo 0)"
    if float_gt "$total_work_after" "$total_work_before" && float_ge "$total_work_after" "$end_to_end_sum"; then
        pass "TC7 Total processing work time is recorded and is >= text aggregation end-to-end sum"
    else
        fail "TC7 Total processing work time is recorded and is >= text aggregation end-to-end sum" "document_total_work_seconds_sum=${total_work_after}, text_aggregation_elapsed_sum=${end_to_end_sum}"
    fi
}

test_resource_metrics() {
    missing=""
    for query in \
        'node_cpu_seconds_total' \
        'node_memory_MemAvailable_bytes' \
        'process_cpu_seconds_total' \
        'process_resident_memory_bytes' \
        'container_cpu_usage_seconds_total{namespace="default"}' \
        'container_memory_working_set_bytes{namespace="default"}'
    do
        if ! wait_series "$query" 60; then
            missing="${missing}${query}; "
        fi
    done

    if [ -z "$missing" ]; then
        pass "TC8 Node, process, and pod CPU/memory metrics are visible"
    else
        fail "TC8 Node, process, and pod CPU/memory metrics are visible" "Missing: ${missing}"
    fi
}

test_failure_metrics() {
    invalid_file="$(mktemp)"
    echo "not a supported upload type" > "$invalid_file"
    failures_before="$(prom_value 'sum(upload_failures_total)' 2>/dev/null || echo 0)"

    http_code="$(curl -sS -o /tmp/invalid-upload-response.txt -w "%{http_code}" -X POST -F "file=@${invalid_file};filename=invalid.txt" "${APP_URL}/upload" 2>/dev/null || echo 000)"
    rm -f "$invalid_file"

    failures_after="$(wait_value_gt 'sum(upload_failures_total)' "$failures_before" 90 || true)"
    if [ "$http_code" = "400" ] && float_gt "$failures_after" "$failures_before"; then
        pass "TC9 Invalid input increments failure counter"
    else
        fail "TC9 Invalid input increments failure counter" "HTTP=${http_code}, upload_failures_total: ${failures_before} -> ${failures_after}, response=$(cat /tmp/invalid-upload-response.txt 2>/dev/null)"
    fi
}

echo "Observability test runner"
echo "App:        ${APP_URL}"
echo "Prometheus: ${PROM_URL}"
echo "Test file:  ${TEST_FILE}"
echo ""

need_cmd curl
need_cmd python3
need_cmd kubectl

test_filegrab_metrics
test_worker_metrics
test_upload_metrics_and_pipeline
test_resource_metrics
test_failure_metrics

echo ""
echo "Summary: ${PASS_COUNT} passed, ${FAIL_COUNT} failed"

if [ "$FAIL_COUNT" -gt 0 ]; then
    exit 1
fi
