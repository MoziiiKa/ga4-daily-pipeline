# Title

## Purpose

## Scope

## Version



# GA4 Public Dataset – Incoming File Contract & Profiling Report

>- *Document owner*: **Mozaffar Kazemi** – Intern Candidate
>- *Last updated*: 05-05-2025

---

## 1 Purpose

This markdown file freezes the technical contract for the daily **`ga4_public_dataset.csv`** drops that arrive in **`gs://platform_assignment_bucket`** at 07:00 CET.  It captures the initial profiling results, establishes a schema baseline, and lists the quality rules that the automated ingestion pipeline will enforce.

## 2 File‑level profile (Day‑0 baseline)

| Check                 | Result                                           | Notes                                                        |
| --------------------- | ------------------------------------------------ | ------------------------------------------------------------ |
| **Row count**         | **416 097 data rows + 1 header = 416 098 total** | Output from `wc ‑l`.  Future runs ±5 % will raise a warning. |
| **Encoding**          | ASCII (valid subset of UTF‑8)                    | Verified with `file` utility.  No BOM detected.              |
| **Delimiter**         | Comma `,`                                        | Confirmed by manual inspection of first 10 lines.            |
| **Quote char**        | Double quote `"`                                 | Required because JSON blobs are embedded.                    |
| **Embedded newlines** | *Present* inside quoted fields                   | Loader flag **`--allow_quoted_newlines=true`** is mandatory. |
| **Compression**       | None                                             | gzip allowed in future versions.                             |

### 2.1 Row‑count sanity (how many records?)

```shell
gsutil cat gs://platform_assignment_bucket/ga4_public_dataset.csv | wc -l
```

Output:

```
416098
```

### 2.2 Sample head

```shell
gsutil cat gs://platform_assignment_bucket/ga4_public_dataset.csv | head -n 10
```

Output:

```
event_date,event_timestamp,event_name,event_params,event_previous_timestamp,event_value_in_usd,event_bundle_sequence_id,event_server_timestamp_offset,user_id,user_pseudo_id,privacy_info,user_first_touch_timestamp,user_ltv,device,geo,app_info,traffic_source,stream_id,platform,event_dimensions,ecommerce,items
20210127,1611789078248882,view_item,"{
  ""event_params"": [{
    ""key"": ""page_location"",
    ""value"": {
      ""string_value"": ""https://shop.googlemerchandisestore.com/Google+Redesign/Apparel/Kids"",
      ""int_value"": null,
      ""float_value"": null,
      ""double_value"": null
    }
```

### 2.3 File format

```shell
gsutil cat gs://platform_assignment_bucket/ga4_public_dataset.csv | head -c 16384 | file -
```

Output

```
/dev/stdin: CSV ASCII text
```

### 2.4 Outlier quote patterns (unquoted “{” that might break the loader)

```shell
# Looks for opening braces NOT immediately preceded by a double quote
gsutil cat gs://platform_assignment_bucket/ga4_public_dataset.csv \
  | grep -n --color=auto '{' | grep -v '\"{'
```

Output:

```
...
415985:  }, {
415987:    ""value"": {
415993:  }, {
415995:    ""value"": {
416001:  }, {
416003:    ""value"": {
416009:  }, {
416011:    ""value"": {
416017:  }, {
416019:    ""value"": {
416025:  }, {
416027:    ""value"": {
416035:  ""privacy_info"": {
416041:  ""user_ltv"": {
416046:  ""device"": {
416059:    ""web_info"": {
416065:  ""geo"": {
416076:  ""traffic_source"": {
416084:  ""ecommerce"": {
```
- Between the closing brace of one object and the opening brace of the next there is just a comma, a newline, and some spaces—no double‑quote right before the new `{`. So `grep -v '\"{'` flags every such continuation line.
- The matches do not mean the file is malformed; they simply confirm the field really contains multi‑line JSON. We should consider this item because the default BigQuery CSV loader (which disallows embedded newlines) will be failed.
- So, we should load with `--allow_quoted_newlines=true` (and keep the normal quote char `"`). It tells BigQuery that newlines inside a quoted field are legal; it will treat the whole block as one cell.


---

## 3 BigQuery schema baseline

The file was loaded into a scratch table, `mozaffar_kazemi_scratch`, with auto‑detection:

```bash
bq --location=europe-west4 load \
  --source_format=CSV \
  --allow_quoted_newlines=true \
  --autodetect \
  mozaffar_kazemi_scratch.ga4_csv \
  gs://platform_assignment_bucket/ga4_public_dataset.csv
```

The inferred schema was exported into `ga4_csv_schema.json`:

```bash
bq --location=europe-west4 show --schema --format=prettyjson \
    mozaffar_kazemi_scratch.ga4_csv > docs/ga4_csv_schema.json
```

> **Contract rule:** The following 22 columns, their types, and modes **must not change** without a version bump and PR review.

| #  | Column                           | Type    | Mode     |
| -- | -------------------------------- | ------- | -------- |
| 1  | event\_date                      | INTEGER | NULLABLE |
| 2  | event\_timestamp                 | INTEGER | NULLABLE |
| 3  | event\_name                      | STRING  | NULLABLE |
| 4  | event\_params                    | STRING  | NULLABLE |
| 5  | event\_previous\_timestamp       | STRING  | NULLABLE |
| 6  | event\_value\_in\_usd            | FLOAT   | NULLABLE |
| 7  | event\_bundle\_sequence\_id      | INTEGER | NULLABLE |
| 8  | event\_server\_timestamp\_offset | STRING  | NULLABLE |
| 9  | user\_id                         | STRING  | NULLABLE |
| 10 | user\_pseudo\_id                 | FLOAT   | NULLABLE |
| 11 | privacy\_info                    | STRING  | NULLABLE |
| 12 | user\_first\_touch\_timestamp    | INTEGER | NULLABLE |
| 13 | user\_ltv                        | STRING  | NULLABLE |
| 14 | device                           | STRING  | NULLABLE |
| 15 | geo                              | STRING  | NULLABLE |
| 16 | app\_info                        | STRING  | NULLABLE |
| 17 | traffic\_source                  | STRING  | NULLABLE |
| 18 | stream\_id                       | INTEGER | NULLABLE |
| 19 | platform                         | STRING  | NULLABLE |
| 20 | event\_dimensions                | STRING  | NULLABLE |
| 21 | ecommerce                        | STRING  | NULLABLE |
| 22 | items                            | STRING  | NULLABLE |

Full schema JSON is stored in **`docs/ga4_csv_schema.json`** for machine‑readable diffing.

---

## 4 Validation rules (enforced by ingestion Cloud Function)

1. **File path** must match regex `ga4_raw/\d{4}/\d{2}/\d{2}/ga4_public_dataset\.csv` (we create a date‑stamped folder each day because the filename is static, avoid overwrites by _partitioning the **path**_).
2. BigQuery load uses flags: `--allow_quoted_newlines=true`, `--source_format=CSV`, `--quote='"'`.
3. Autodetected schema must equal the 22‑column contract above; any extra/missing columns fail the ingest.
4. Load must complete < 5 min after file arrival, otherwise Workflows raises an alert.
5. **Row count delta** > ±5 % versus baseline raises a WARNING; > ±20 % fails the load.
   > **Volume‑based quality gate**
   > * **Warning** – Google’s managed data‑quality service lets you set a row‑level “passing threshold” and recommends ≥ 95% rows passing as a common starting point. A 95% pass‑rate translates to a 5 % variance window before the rule flips from OK to FAIL [1][2]. So, if total rows differ from the baseline by more than **± 5 %**, Workflows raises a Slack/email alert but the load continues.
   > * **Fail** – Google’s sample anomaly‑detection pipelines (BigQuery ML `ML.DETECT_ANOMALIES`) flag data points that deviate `~2σ`, which for normally distributed row counts is roughly a 20% swing in many GA‑like datasets [3]. So, if variance exceeds **± 20 %**, the Cloud Function aborts and marks the DAG run as FAILED (mirrors Dataplex < 80% pass threshold and BigQuery‑ML anomaly‑detection norms).

**References:**

1. [Auto data quality overview](https://cloud.google.com/dataplex/docs/auto-data-quality-overview)
2. [Use auto data quality](https://cloud.google.com/dataplex/docs/use-auto-data-quality)
3. [The ML.DETECT_ANOMALIES function](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-detect-anomalies)