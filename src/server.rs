use askama::Template;
use axum::extract::{Form, Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Sse};
use axum::Json;
use axum::routing::{get, post};
use axum::Router;
use crate::backup;
use clap::Parser;
use clap::ArgAction;
use chrono::TimeZone;
use reqwest::Certificate;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde::de::{self, Deserializer};
use libc::{kill, SIGTERM};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{ErrorKind, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex, Notify, RwLock};
use std::convert::Infallible;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};
use tokio_stream::StreamExt;
use tracing::{info, warn};
use walkdir::WalkDir;
use zip::write::FileOptions;

const DEFAULT_STAGE_NAME: &str = "copy";
const SPLIT_SUFFIX: &str = "split";
const MAX_TAIL_LINES: usize = 200;
const REDACTED_VALUE: &str = "***";

#[derive(Parser, Debug)]
#[command(name = "es-copy-indices-server", version)]
struct ServerArgs {
    #[arg(long, value_name = "PATH")]
    main_config: PathBuf,
    #[arg(long = "env-templates", alias = "templates", value_name = "DIR")]
    templates_dir: PathBuf,
    #[arg(long = "root-certificates", alias = "ca-path", value_name = "PATH")]
    root_certificates: Option<PathBuf>,
    #[arg(long)]
    insecure: bool,
    #[arg(long, default_value = "runs")]
    runs_dir: PathBuf,
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: String,
    #[arg(long = "es-copy-indices-path", default_value = "es-copy-indices")]
    es_copy_path: PathBuf,
    #[arg(long, default_value = "/")]
    base_path: String,
    #[arg(long, default_value_t = 5)]
    refresh_seconds: u64,
    #[arg(long, default_value_t = 5)]
    metrics_seconds: u64,
    #[arg(long, value_name = "COUNT", default_value_t = 0)]
    max_concurrent_jobs: usize,
    #[arg(long)]
    timestamp: Option<String>,
    #[arg(long = "from-index-name-suffix")]
    from_suffix: Option<String>,
    #[arg(long = "index-copy-suffix")]
    copy_suffix: Option<String>,
    #[arg(long = "alias-suffix")]
    alias_suffix: Option<String>,
    #[arg(long = "alias-remove-if-exists", default_value_t = false)]
    alias_remove_if_exists: bool,
    #[arg(long, default_value_t = false)]
    audit: bool,
    #[arg(long = "backup-dir", value_name = "PATH")]
    backup_dir: Option<PathBuf>,
    #[arg(long = "no-redact-logs", action = ArgAction::SetTrue)]
    no_redact_logs: bool,
}

#[derive(Clone)]
struct AppState {
    endpoints: Vec<EndpointConfig>,
    templates: Vec<TemplateConfig>,
    client: reqwest::Client,
    runs_dir: PathBuf,
    es_copy_path: PathBuf,
    refresh_seconds: u64,
    base_path: String,
    ca_path: Option<PathBuf>,
    insecure: bool,
    metrics: Arc<RwLock<MetricsState>>,
    metrics_tx: broadcast::Sender<MetricsSample>,
    timestamp: Option<String>,
    from_suffix: Option<String>,
    copy_suffix: Option<String>,
    alias_suffix: Option<String>,
    alias_remove_if_exists: bool,
    audit: bool,
    redact_logs: bool,
    backup_dir: Option<PathBuf>,
    runs: Arc<RwLock<RunStore>>,
    quarantined_runs: Arc<RwLock<Vec<String>>>,
    default_max_concurrent_jobs: Option<usize>,
    destination_queues: Arc<Mutex<HashMap<String, DestinationQueue>>>,
    queue_notify: Arc<Notify>,
}

#[derive(Default)]
struct RunStore {
    order: Vec<String>,
    runs: HashMap<String, RunState>,
}

fn build_destination_queues(
    endpoints: &[EndpointConfig],
    default_max: Option<usize>,
) -> HashMap<String, DestinationQueue> {
    let mut queues = HashMap::new();
    for endpoint in endpoints {
        queues.entry(endpoint.id.clone()).or_insert_with(|| DestinationQueue {
            max_concurrent_jobs: default_max,
            queue: VecDeque::new(),
        });
    }
    queues
}

async fn ensure_destination_queue(state: &Arc<AppState>, destination_id: &str) {
    let mut queues = state.destination_queues.lock().await;
    queues.entry(destination_id.to_string()).or_insert_with(|| DestinationQueue {
        max_concurrent_jobs: state.default_max_concurrent_jobs,
        queue: VecDeque::new(),
    });
}

#[derive(Clone, Debug)]
struct QueuedJob {
    run_id: String,
    job_id: String,
    destination_id: String,
}

#[derive(Clone, Debug)]
struct DestinationQueue {
    max_concurrent_jobs: Option<usize>,
    queue: VecDeque<QueuedJob>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RunPersist {
    id: String,
    created_at: String,
    #[serde(default)]
    template: TemplateSnapshot,
    #[serde(default)]
    src_endpoint: EndpointSnapshot,
    #[serde(default)]
    dst_endpoint: EndpointSnapshot,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    copy_suffix: Option<String>,
    #[serde(default)]
    alias_suffix: Option<String>,
    #[serde(default)]
    wizard: Option<WizardSnapshot>,
    #[serde(default)]
    run_mode: RunMode,
    stages: Vec<StagePersist>,
    jobs: Vec<JobPersist>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StagePersist {
    id: String,
    name: String,
    job_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JobPersist {
    id: String,
    name: String,
    stage_id: String,
    stage_name: String,
    index_name: String,
    status: JobStatus,
    exit_code: Option<i32>,
    started_at: Option<String>,
    finished_at: Option<String>,
    config_path: String,
    stdout_path: String,
    stderr_path: String,
    #[serde(default)]
    progress_percent: Option<f64>,
    #[serde(default)]
    completed_line: bool,
    #[serde(default)]
    split_field: Option<String>,
    #[serde(default)]
    split_from: Option<String>,
    #[serde(default)]
    split_to: Option<String>,
    #[serde(default)]
    split_leftover: bool,
    #[serde(default)]
    split_query: Option<String>,
    #[serde(default)]
    split_doc_count: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct WizardSnapshot {
    defaults: WizardDefaults,
    rename: WizardRenameRule,
    alias: WizardAliasRule,
    items: Vec<WizardItemSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct WizardDefaults {
    buffer_size: u64,
    copy_content: bool,
    copy_mapping: bool,
    delete_if_exists: bool,
    number_of_replicas: Option<u64>,
    number_of_shards: Option<u64>,
    alias_enabled: bool,
    alias_remove_if_exists: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct WizardRenameRule {
    #[serde(default)]
    pattern: String,
    #[serde(default)]
    replace: String,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    suffix: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct WizardAliasRule {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    pattern: String,
    #[serde(default)]
    replace: String,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    suffix: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct WizardOverrides {
    #[serde(default)]
    buffer_size: Option<u64>,
    #[serde(default)]
    copy_content: Option<bool>,
    #[serde(default)]
    copy_mapping: Option<bool>,
    #[serde(default)]
    delete_if_exists: Option<bool>,
    #[serde(default)]
    number_of_replicas: Option<u64>,
    #[serde(default)]
    number_of_shards: Option<u64>,
    #[serde(default)]
    alias_enabled: Option<bool>,
    #[serde(default)]
    alias_remove_if_exists: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct WizardItemSnapshot {
    source_name: String,
    dest_base_name: String,
    #[serde(default)]
    alias_base_name: Option<String>,
    #[serde(default)]
    overrides: Option<WizardOverrides>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum RunMode {
    Copy,
    Backup,
    Restore,
}

impl Default for RunMode {
    fn default() -> Self {
        RunMode::Copy
    }
}

impl RunMode {
    fn as_str(&self) -> &'static str {
        match self {
            RunMode::Copy => "copy",
            RunMode::Backup => "backup",
            RunMode::Restore => "restore",
        }
    }
}

#[derive(Clone, Debug)]
struct RunState {
    id: String,
    created_at: String,
    template: TemplateSnapshot,
    src_endpoint: EndpointSnapshot,
    dst_endpoint: EndpointSnapshot,
    dry_run: bool,
    copy_suffix: Option<String>,
    alias_suffix: Option<String>,
    wizard: Option<WizardSnapshot>,
    run_mode: RunMode,
    stages: Vec<StageState>,
    jobs: HashMap<String, JobState>,
}

#[derive(Clone, Debug)]
struct StageState {
    id: String,
    name: String,
    job_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum JobStatus {
    Pending,
    Running,
    Queued,
    Succeeded,
    Failed,
    Stopped,
}

impl JobStatus {
    fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Queued => "queued",
            JobStatus::Succeeded => "succeeded",
            JobStatus::Failed => "failed",
            JobStatus::Stopped => "stopped",
        }
    }
}

#[derive(Clone, Debug)]
struct JobState {
    id: String,
    name: String,
    stage_id: String,
    stage_name: String,
    index_name: String,
    status: JobStatus,
    exit_code: Option<i32>,
    started_at: Option<String>,
    finished_at: Option<String>,
    pid: Option<u32>,
    config_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    stdout_tail: VecDeque<String>,
    stderr_tail: VecDeque<String>,
    stdout_tx: broadcast::Sender<LogEvent>,
    stderr_tx: broadcast::Sender<LogEvent>,
    progress_percent: Option<f64>,
    completed_line: bool,
    split_field: Option<String>,
    split_from: Option<String>,
    split_to: Option<String>,
    split_leftover: bool,
    split_query: Option<String>,
    split_doc_count: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct LogEvent {
    stream: LogStream,
    line: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct EndpointAuth {
    username: String,
    #[serde(default)]
    password: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EndpointFile {
    name: String,
    url: String,
    prefix: String,
    #[serde(default)]
    number_of_replicas: u64,
    keep_alive: String,
    #[serde(default)]
    auth: Option<EndpointAuth>,
    #[serde(default)]
    backup_dir: Option<String>,
    #[serde(default)]
    tenants: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct MainGlobalConfig {
    #[serde(default)]
    backup_dir: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MainConfigFile {
    #[serde(default)]
    global: Option<MainGlobalConfig>,
    endpoints: Vec<EndpointFile>,
}

#[derive(Clone, Debug)]
struct EndpointConfig {
    id: String,
    name: String,
    url: String,
    prefix: String,
    number_of_replicas: u64,
    keep_alive: String,
    auth: Option<EndpointAuth>,
    backup_dir: Option<String>,
    tenants: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct EndpointSnapshot {
    id: String,
    name: String,
    url: String,
    prefix: String,
    number_of_replicas: u64,
    keep_alive: String,
    #[serde(default)]
    backup_dir: Option<String>,
    #[serde(default)]
    tenants: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct TemplateSnapshot {
    id: String,
    name: String,
    path: String,
    #[serde(default)]
    number_of_replicas: Option<u64>,
    #[serde(default)]
    tenants: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct TemplateGlobal {
    name: Option<String>,
    number_of_replicas: Option<u64>,
    #[serde(default)]
    tenants: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct TemplateFile {
    global: Option<TemplateGlobal>,
    indices: Vec<InputIndex>,
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

#[derive(Debug, Deserialize, Clone)]
struct InputIndex {
    name: String,
    buffer_size: u64,
    #[serde(default = "default_true")]
    copy_content: bool,
    #[serde(default = "default_true")]
    copy_mapping: bool,
    #[serde(default = "default_false")]
    delete_if_exists: bool,
    routing_field: Option<String>,
    number_of_shards: Option<u64>,
    number_of_replicas: Option<u64>,
    #[serde(default)]
    dest_name: Option<String>,
    #[serde(default)]
    alias_name: Option<String>,
    #[serde(default = "default_false")]
    use_dest_name_as_is: bool,
    #[serde(default = "default_false")]
    use_alias_name_as_is: bool,
    #[serde(default = "default_true")]
    alias_enabled: bool,
    #[serde(default)]
    alias_remove_if_exists: Option<bool>,
    #[serde(default = "default_true")]
    use_src_prefix: bool,
    #[serde(default = "default_true")]
    use_dst_prefix: bool,
    #[serde(default = "default_true")]
    use_from_suffix: bool,
    split: Option<SplitConfig>,
    custom: Option<InputCustom>,
}

#[derive(Debug, Deserialize, Clone)]
struct SplitConfig {
    field_name: String,
    number_of_parts: u64,
}

#[derive(Debug, Deserialize, Clone)]
struct InputCustom {
    mapping: Option<String>,
}

#[derive(Clone, Debug)]
struct TemplateConfig {
    id: String,
    name: String,
    path: PathBuf,
    number_of_replicas: Option<u64>,
    indices: Vec<InputIndex>,
    tenants: Vec<String>,
}

#[derive(Clone, Debug)]
struct StagePlan {
    name: String,
    jobs: Vec<JobPlan>,
}

#[derive(Clone, Debug)]
struct JobPlan {
    name: String,
    index: InputIndex,
    date_from: Option<String>,
    date_to: Option<String>,
    leftover: bool,
    split_doc_count: Option<u64>,
}

#[derive(Clone, Debug)]
struct SplitRange {
    from: Option<String>,
    to: Option<String>,
    doc_count: Option<u64>,
}

#[derive(Serialize)]
struct OutputConfig {
    endpoints: Vec<OutputEndpoint>,
    indices: Vec<OutputIndex>,
    audit: Option<OutputAudit>,
}

#[derive(Serialize)]
struct OutputEndpoint {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    root_certificates: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    insecure: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    basic_auth: Option<OutputBasicAuth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_dir: Option<String>,
}

#[derive(Serialize)]
struct OutputBasicAuth {
    username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
}

#[derive(Serialize)]
struct OutputAudit {
    enabled: bool,
    file_name: String,
}

#[derive(Serialize)]
struct OutputIndex {
    buffer_size: u64,
    copy_content: bool,
    copy_mapping: bool,
    delete_if_exists: bool,
    from: String,
    keep_alive: String,
    name: String,
    name_of_copy: String,
    number_of_replicas: u64,
    number_of_shards: u64,
    to: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_quantile_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    routing_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pre_create_doc_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    alias: Option<OutputAlias>,
    #[serde(skip_serializing_if = "Option::is_none")]
    custom: Option<OutputCustom>,
}

#[derive(Serialize)]
struct OutputAlias {
    name: String,
    remove_if_exists: bool,
}

#[derive(Serialize)]
struct OutputCustom {
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mapping: Option<String>,
}

#[derive(Template)]
#[template(path = "server/index.html")]
struct IndexTemplate {
    runs: Vec<RunSummary>,
    base_path: String,
    active_nav: String,
    endpoints: Vec<EndpointView>,
    templates: Vec<TemplateView>,
    endpoints_json: String,
    templates_json: String,
    metrics_samples_json: String,
    queue_limits: Vec<QueueLimitView>,
    running_total: usize,
    queued_total: usize,
    copy_suffix: Option<String>,
    alias_suffix: Option<String>,
    backup_root_display: String,
    backup_root_value: String,
}

#[derive(Template)]
#[template(path = "server/run.html")]
struct RunTemplate {
    run: RunView,
    base_path: String,
    active_nav: String,
}

#[derive(Template)]
#[template(path = "server/job.html")]
struct JobTemplate {
    run_id: String,
    job: JobView,
    base_path: String,
    active_nav: String,
}

#[derive(Template)]
#[template(path = "server/status.html")]
struct StatusTemplate {
    base_path: String,
    summary: MetricsSummary,
    active_nav: String,
}

#[derive(Template)]
#[template(path = "server/config.html")]
struct ConfigTemplate {
    base_path: String,
    endpoints: Vec<EndpointView>,
    templates: Vec<TemplateView>,
    active_nav: String,
}

#[derive(Template)]
#[template(path = "server/jobs.html")]
struct JobsTemplate {
    base_path: String,
    runs: Vec<RunOption>,
    runs_json: String,
    active_nav: String,
}

#[derive(Clone, Serialize)]
struct EndpointView {
    id: String,
    name: String,
    url: String,
    prefix: String,
    keep_alive: String,
    tenants: Vec<String>,
}

#[derive(Clone, Serialize)]
struct TemplateView {
    id: String,
    name: String,
    file_name: String,
    indices_count: usize,
    number_of_replicas: Option<u64>,
    indices: Vec<TemplateIndexView>,
    tenants: Vec<String>,
}

#[derive(Clone, Serialize)]
struct TemplateIndexView {
    name: String,
    has_split: bool,
}

#[derive(Clone, Serialize)]
struct RunOption {
    id: String,
    label: String,
}

#[derive(Clone, Serialize)]
struct RunSummary {
    id: String,
    created_at: String,
    jobs_total: usize,
    jobs_running: usize,
    jobs_queued: usize,
    jobs_failed: usize,
    jobs_succeeded: usize,
    src_name: String,
    dst_name: String,
    dst_id: String,
    template_name: String,
    dry_run: bool,
    copy_suffix: Option<String>,
    alias_suffix: Option<String>,
    wizard: bool,
    run_mode: RunMode,
}

#[derive(Clone, Serialize)]
struct QueueLimitView {
    endpoint_id: String,
    endpoint_name: String,
    running_jobs: usize,
    queued_jobs: usize,
    max_concurrent_jobs: Option<usize>,
}

#[derive(Clone, Serialize)]
struct RunView {
    id: String,
    created_at: String,
    stages: Vec<StageView>,
    src_endpoint: EndpointSnapshot,
    dst_endpoint: EndpointSnapshot,
    template: TemplateSnapshot,
    dry_run: bool,
    copy_suffix: Option<String>,
    alias_suffix: Option<String>,
    wizard: Option<WizardSnapshot>,
    run_mode: RunMode,
}

#[derive(Clone, Serialize)]
struct StageView {
    id: String,
    name: String,
    jobs: Vec<JobSummary>,
    split_details: Vec<SplitDetailView>,
}

#[derive(Clone, Serialize)]
struct SplitDetailView {
    job_name: String,
    field_name: String,
    date_from: Option<String>,
    date_to: Option<String>,
    leftover: bool,
    query: Option<String>,
    doc_count: Option<u64>,
}

#[derive(Clone, Serialize)]
struct JobSummary {
    id: String,
    name: String,
    status: JobStatus,
    progress_label: Option<String>,
    progress_width: Option<u8>,
    eta_label: Option<String>,
    completed_line: bool,
}

#[derive(Clone)]
struct JobView {
    id: String,
    name: String,
    stage_name: String,
    status: JobStatus,
    stdout_tail: Vec<String>,
    stderr_tail: Vec<String>,
    progress_label: Option<String>,
    eta_label: Option<String>,
}

#[derive(Clone, Serialize)]
struct JobListEntry {
    run_id: String,
    run_label: String,
    run_env_label: String,
    stage_name: String,
    job_name: String,
    status: JobStatus,
    progress_label: Option<String>,
    progress_width: Option<u8>,
    eta_label: Option<String>,
    logs_url: String,
    start_url: String,
    stop_url: String,
    can_start: bool,
    can_stop: bool,
}

#[derive(Clone, Debug, Default)]
struct MetricsState {
    samples: VecDeque<MetricsSample>,
}

#[derive(Clone, Debug, Serialize)]
struct MetricsSample {
    ts: i64,
    host_cpu: f32,
    host_mem_used_kb: u64,
    host_mem_total_kb: u64,
    proc_cpu: f32,
    proc_mem_kb: u64,
    children_cpu: f32,
    children_mem_kb: u64,
    total_cpu: f32,
    total_mem_kb: u64,
    running_jobs: usize,
    queued_jobs: usize,
    max_concurrent_jobs: Option<usize>,
    load1: f64,
    load5: f64,
    load15: f64,
}

#[derive(Clone, Debug, Default)]
struct MetricsSummary {
    host_cpu: f32,
    proc_cpu: f32,
    children_cpu: f32,
    total_cpu: f32,
    host_mem_used_mb: u64,
    host_mem_total_mb: u64,
    proc_mem_mb: u64,
    children_mem_mb: u64,
    total_mem_mb: u64,
    running_jobs: usize,
    queued_jobs: usize,
    load1: f64,
    load5: f64,
    load15: f64,
}

pub async fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_file(true)
        .with_line_number(true)
        .init();

    let args = ServerArgs::parse();
    if let Some(path) = &args.root_certificates {
        if !path.is_dir() {
            warn!(
                "--root-certificates {:?} is not a directory; HTTPS may fail.",
                path
            );
        }
    }
    if args.insecure && args.root_certificates.is_none() {
        warn!("--insecure disables TLS verification for percentile queries and generated jobs.");
    }
    let main_config = load_main_config(&args.main_config);
    let mut backup_dir = args
        .backup_dir
        .clone()
        .and_then(resolve_backup_dir_path)
        .or(main_config.backup_dir);
    if let Some(dir) = &backup_dir {
        if dir.as_os_str().is_empty() {
            backup_dir = None;
        }
    }
    let endpoints = main_config.endpoints;
    let templates = load_templates(&args.templates_dir);
    if endpoints.is_empty() {
        panic!("No endpoints found in {:?}", args.main_config);
    }
    if templates.is_empty() {
        panic!("No templates found in {:?}", args.templates_dir);
    }

    let client = build_reqwest_client(args.root_certificates.as_ref(), args.insecure)
        .unwrap_or_else(|e| panic!("Failed to build HTTP client: {e}"));

    let runs_dir = args.runs_dir.clone();
    if let Err(e) = tokio::fs::create_dir_all(&runs_dir).await {
        panic!("Failed to create runs dir {:?}: {:?}", runs_dir, e);
    }

    let metrics = Arc::new(RwLock::new(MetricsState::default()));
    let (metrics_tx, _) = broadcast::channel(200);
    let default_max_concurrent_jobs = if args.max_concurrent_jobs == 0 {
        None
    } else {
        Some(args.max_concurrent_jobs)
    };
    let destination_queues = build_destination_queues(&endpoints, default_max_concurrent_jobs);
    let state = Arc::new(AppState {
        endpoints,
        templates,
        client,
        runs_dir,
        es_copy_path: args.es_copy_path,
        refresh_seconds: args.refresh_seconds,
        base_path: normalize_base_path(&args.base_path),
        ca_path: args.root_certificates.clone(),
        insecure: args.insecure,
        metrics: Arc::clone(&metrics),
        metrics_tx,
        timestamp: args.timestamp,
        from_suffix: args.from_suffix,
        copy_suffix: args.copy_suffix,
        alias_suffix: args.alias_suffix,
        alias_remove_if_exists: args.alias_remove_if_exists,
        audit: args.audit,
        redact_logs: !args.no_redact_logs,
        backup_dir,
        runs: Arc::new(RwLock::new(RunStore::default())),
        quarantined_runs: Arc::new(RwLock::new(Vec::new())),
        default_max_concurrent_jobs,
        destination_queues: Arc::new(Mutex::new(destination_queues)),
        queue_notify: Arc::new(Notify::new()),
    });

    load_runs(&state).await;
    start_queue_worker(Arc::clone(&state));
    start_metrics_sampler(
        Arc::clone(&state.metrics),
        state.metrics_tx.clone(),
        Arc::clone(&state.runs),
        args.metrics_seconds,
    );

    let routes = Router::new()
        .route("/", get(index))
        .route("/dashboard", get(index))
        .route("/runs", get(index).post(create_run))
        .route("/runs/wizard", post(create_run_wizard))
        .route("/wizard/sources", get(wizard_sources))
        .route("/jobs", get(jobs_view))
        .route("/config", get(config_view))
        .route("/status", get(status_view))
        .route("/status/snapshot", get(status_snapshot))
        .route("/status/stream", get(status_stream))
        .route("/backups", get(backups_list))
        .route("/jobs/snapshot", get(jobs_snapshot))
        .route("/jobs/stream", get(jobs_stream))
        .route("/runs/snapshot", get(runs_snapshot))
        .route("/runs/stream", get(runs_stream))
        .route(
            "/settings/max-concurrent-jobs",
            post(update_max_concurrent_jobs),
        )
        .route(
            "/settings/max-concurrent-jobs/{endpoint_id}",
            post(update_max_concurrent_jobs_for_endpoint),
        )
        .route("/runs/{run_id}", get(run_view))
        .route("/runs/{run_id}/stream", get(run_stream))
        .route("/runs/{run_id}/delete", post(delete_run))
        .route("/runs/{run_id}/export", get(export_run))
        .route("/runs/{run_id}/retry-failed", post(retry_failed))
        .route("/runs/{run_id}/stop", post(stop_run))
        .route("/runs/{run_id}/stages/{stage_id}/start", post(start_stage))
        .route("/runs/{run_id}/stages/{stage_id}/stop", post(stop_stage))
        .route("/runs/{run_id}/jobs/{job_id}/start", post(start_job))
        .route("/runs/{run_id}/jobs/{job_id}/stop", post(stop_job))
        .route("/runs/{run_id}/jobs/{job_id}", get(job_view))
        .route("/runs/{run_id}/jobs/{job_id}/stream", get(job_stream))
        .route(
            "/runs/{run_id}/jobs/{job_id}/status",
            get(job_status_stream),
        );

    let app = if state.base_path == "/" {
        routes
    } else {
        let base_with_slash = format!("{}/", state.base_path);
        Router::new()
            .route("/", get(root_redirect))
            .route(&base_with_slash, get(base_trailing_redirect))
            .nest(&state.base_path, routes)
    }
    .with_state(Arc::clone(&state));

    let listener = tokio::net::TcpListener::bind(&args.bind)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind {}: {e}", &args.bind));
    info!("Server listening on {}", &args.bind);
    axum::serve(listener, app).await.unwrap();
}

async fn index(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let runs = build_run_summaries(&state).await;
    let running_total = runs.iter().map(|run| run.jobs_running).sum();
    let queued_total = runs.iter().map(|run| run.jobs_queued).sum();
    let queue_limits = build_queue_limits(&state).await;
    let backup_root_value = state
        .backup_dir
        .as_ref()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_default();
    let backup_root_display = if backup_root_value.is_empty() {
        "Not configured".to_string()
    } else {
        backup_root_value.clone()
    };
    let template = IndexTemplate {
        runs,
        base_path: state.base_path.clone(),
        active_nav: "dashboard".to_string(),
        endpoints: build_endpoint_views(&state),
        templates: build_template_views(&state),
        endpoints_json: endpoints_json(&state),
        templates_json: templates_json(&state),
        metrics_samples_json: metrics_samples_json(state.as_ref()).await,
        queue_limits,
        running_total,
        queued_total,
        copy_suffix: state.copy_suffix.clone(),
        alias_suffix: state.alias_suffix.clone(),
        backup_root_display,
        backup_root_value,
    };
    Html(render_template(&template)).into_response()
}

async fn backups_list(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let backup_root = match &state.backup_dir {
        Some(path) => path.clone(),
        None => {
            let response = BackupListResponse {
                backup_root: None,
                backups: Vec::new(),
            };
            return Json(response).into_response();
        }
    };
    let backup_root_str = backup_root.to_string_lossy().to_string();
    let read_dir = match std::fs::read_dir(&backup_root) {
        Ok(entries) => entries,
        Err(_) => {
            let response = BackupListResponse {
                backup_root: Some(backup_root_str),
                backups: Vec::new(),
            };
            return Json(response).into_response();
        }
    };
    let mut backups = Vec::new();
    for entry in read_dir.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let run_id = match path.file_name().and_then(|name| name.to_str()) {
            Some(name) if !name.is_empty() => name.to_string(),
            _ => continue,
        };
        let indices_path = path.join("indices.json");
        if !indices_path.is_file() {
            continue;
        }
        let catalog = backup::load_catalog(&indices_path);
        if catalog.indices.is_empty() {
            continue;
        }
        let index_count = catalog.indices.len();
        let docs_total = catalog
            .indices
            .iter()
            .filter_map(|item| item.docs_total)
            .sum::<u64>();
        let created_at = catalog.indices.first().map(|item| item.created_at.clone());
        let mut source = None;
        let mut tenants = Vec::new();
        let run_path = state.runs_dir.join(&run_id).join("run.json");
        if let Ok(content) = std::fs::read_to_string(run_path) {
            if let Ok(run) = serde_json::from_str::<RunPersist>(&content) {
                if !run.src_endpoint.name.is_empty() {
                    source = Some(run.src_endpoint.name);
                }
                if !run.template.tenants.is_empty() {
                    tenants = run.template.tenants;
                } else if !run.src_endpoint.tenants.is_empty() {
                    tenants = run.src_endpoint.tenants;
                } else if !run.dst_endpoint.tenants.is_empty() {
                    tenants = run.dst_endpoint.tenants;
                }
            }
        }
        let label = if let Some(src) = &source {
            format!("{} · {}", run_id, src)
        } else {
            format!("{} · {} indices", run_id, index_count)
        };
        backups.push(BackupRunSummary {
            id: run_id,
            label,
            source,
            created_at,
            index_count,
            docs_total,
            indices: catalog.indices,
            tenants,
        });
    }
    backups.sort_by(|a, b| b.id.cmp(&a.id));
    let response = BackupListResponse {
        backup_root: Some(backup_root_str),
        backups,
    };
    Json(response).into_response()
}

async fn config_view(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let template = ConfigTemplate {
        base_path: state.base_path.clone(),
        endpoints: build_endpoint_views(&state),
        templates: build_template_views(&state),
        active_nav: "config".to_string(),
    };
    Html(render_template(&template)).into_response()
}

async fn root_redirect(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let target = if state.base_path == "/" {
        "/".to_string()
    } else {
        format!("{}/", state.base_path)
    };
    Redirect::to(&target)
}

async fn base_trailing_redirect(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Redirect::to(&state.base_path)
}

#[derive(Deserialize)]
struct CreateRunForm {
    src_endpoint_id: String,
    dst_endpoint_id: String,
    template_id: String,
    dry_run: Option<String>,
    index_copy_suffix: Option<String>,
    alias_suffix: Option<String>,
    mode: Option<String>,
    backup_id: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    selected_indices: Vec<String>,
}

#[derive(Serialize)]
struct BackupRunSummary {
    id: String,
    label: String,
    source: Option<String>,
    created_at: Option<String>,
    index_count: usize,
    docs_total: u64,
    indices: Vec<backup::BackupIndexEntry>,
    tenants: Vec<String>,
}

#[derive(Serialize)]
struct BackupListResponse {
    backup_root: Option<String>,
    backups: Vec<BackupRunSummary>,
}

fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrVec;

    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or list of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(Vec::new());
            }
            if trimmed.contains('\n') {
                let values = trimmed
                    .lines()
                    .map(|line| line.trim())
                    .filter(|line| !line.is_empty())
                    .map(|line| line.to_string())
                    .collect::<Vec<_>>();
                return Ok(values);
            }
            if trimmed.contains(',') {
                let values = trimmed
                    .split(',')
                    .map(|item| item.trim())
                    .filter(|item| !item.is_empty())
                    .map(|item| item.to_string())
                    .collect::<Vec<_>>();
                return Ok(values);
            }
            Ok(vec![trimmed.to_string()])
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element::<String>()? {
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_any(StringOrVec)
}

#[derive(Deserialize)]
struct WizardRunRequest {
    src_endpoint_id: String,
    dst_endpoint_id: String,
    #[serde(default)]
    dry_run: bool,
    defaults: WizardDefaults,
    rename: WizardRenameRule,
    alias: WizardAliasRule,
    items: Vec<WizardItemSnapshot>,
}

#[derive(Deserialize)]
struct WizardSourcesQuery {
    src_endpoint_id: String,
    pattern: Option<String>,
}

#[derive(Serialize)]
struct WizardSourceItem {
    name: String,
    kind: String,
    docs: Option<u64>,
    size: Option<String>,
    indices: Vec<String>,
}

#[derive(Deserialize)]
struct MaxConcurrentForm {
    delta: Option<i32>,
    value: Option<usize>,
}

fn backup_endpoint_config(backup_dir: &str) -> EndpointConfig {
    EndpointConfig {
        id: "backup".to_string(),
        name: "Backup directory".to_string(),
        url: String::new(),
        prefix: String::new(),
        number_of_replicas: 0,
        keep_alive: "10m".to_string(),
        auth: None,
        backup_dir: Some(backup_dir.to_string()),
        tenants: Vec::new(),
    }
}

async fn create_run(
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateRunForm>,
) -> impl IntoResponse {
    let mode = form
        .mode
        .as_deref()
        .unwrap_or("copy")
        .trim()
        .to_lowercase();
    let template = match template_by_id(&state, &form.template_id) {
        Some(template) => template.clone(),
        None => return (StatusCode::BAD_REQUEST, "Unknown template").into_response(),
    };
    let dry_run = form.dry_run.is_some();
    let copy_suffix_override = form.index_copy_suffix.map(|value| value.trim().to_string());
    let alias_suffix_override = form.alias_suffix.map(|value| value.trim().to_string());
    let selected_indices = if form.selected_indices.is_empty() {
        None
    } else {
        let filtered: HashSet<String> = form
            .selected_indices
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        if filtered.is_empty() { None } else { Some(filtered) }
    };

    let (src_endpoint, dst_endpoint, run_mode) = match mode.as_str() {
        "backup" => {
            let src_endpoint = match endpoint_by_id(&state, &form.src_endpoint_id) {
                Some(endpoint) => endpoint.clone(),
                None => {
                    return (StatusCode::BAD_REQUEST, "Unknown source endpoint").into_response()
                }
            };
            let backup_root = match &state.backup_dir {
                Some(path) => path.to_string_lossy().to_string(),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        "backup_dir is not configured on the server",
                    )
                        .into_response()
                }
            };
            (src_endpoint, backup_endpoint_config(&backup_root), RunMode::Backup)
        }
        "restore" => {
            let dst_endpoint = match endpoint_by_id(&state, &form.dst_endpoint_id) {
                Some(endpoint) => endpoint.clone(),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        "Unknown destination endpoint",
                    )
                        .into_response()
                }
            };
            let backup_root = match &state.backup_dir {
                Some(path) => path.clone(),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        "backup_dir is not configured on the server",
                    )
                        .into_response()
                }
            };
            let backup_id = form
                .backup_id
                .as_deref()
                .unwrap_or("")
                .trim()
                .to_string();
            if backup_id.is_empty() {
                return (StatusCode::BAD_REQUEST, "backup_id is required").into_response();
            }
            let backup_dir = backup_root.join(&backup_id);
            if !backup_dir.is_dir() {
                return (
                    StatusCode::BAD_REQUEST,
                    "Selected backup directory does not exist",
                )
                    .into_response();
            }
            let backup_dir = backup_dir.to_string_lossy().to_string();
            (backup_endpoint_config(&backup_dir), dst_endpoint, RunMode::Restore)
        }
        _ => {
            let src_endpoint = match endpoint_by_id(&state, &form.src_endpoint_id) {
                Some(endpoint) => endpoint.clone(),
                None => return (StatusCode::BAD_REQUEST, "Unknown source endpoint").into_response(),
            };
            let dst_endpoint = match endpoint_by_id(&state, &form.dst_endpoint_id) {
                Some(endpoint) => endpoint.clone(),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        "Unknown destination endpoint",
                    )
                        .into_response()
                }
            };
            (src_endpoint, dst_endpoint, RunMode::Copy)
        }
    };

    let copy_suffix_override = if run_mode == RunMode::Backup {
        None
    } else {
        copy_suffix_override
    };
    let alias_suffix_override = if run_mode == RunMode::Backup {
        None
    } else {
        alias_suffix_override
    };

    match run_mode {
        RunMode::Copy => {
            if !tenants_overlap(&template, &src_endpoint)
                || !tenants_overlap(&template, &dst_endpoint)
            {
                return (
                    StatusCode::BAD_REQUEST,
                    "Template tenants do not match selected endpoints",
                )
                    .into_response();
            }
        }
        RunMode::Backup => {
            if !tenants_overlap(&template, &src_endpoint) {
                return (
                    StatusCode::BAD_REQUEST,
                    "Template tenants do not match selected endpoint",
                )
                    .into_response();
            }
        }
        RunMode::Restore => {
            if !tenants_overlap(&template, &dst_endpoint) {
                return (
                    StatusCode::BAD_REQUEST,
                    "Template tenants do not match selected endpoint",
                )
                    .into_response();
            }
        }
    }

    match create_run_state_with_filter(
        &state,
        &template,
        &src_endpoint,
        &dst_endpoint,
        dry_run,
        copy_suffix_override,
        alias_suffix_override,
        selected_indices.as_ref(),
        run_mode,
    )
    .await
    {
        Ok(run_id) => Redirect::to(&with_base(&state, &format!("/runs/{}", run_id)))
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create run: {}", err),
        )
            .into_response(),
    }
}

async fn create_run_wizard(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<WizardRunRequest>,
) -> impl IntoResponse {
    let src_endpoint = match endpoint_by_id(&state, &payload.src_endpoint_id) {
        Some(endpoint) => endpoint.clone(),
        None => return (StatusCode::BAD_REQUEST, "Unknown source endpoint").into_response(),
    };
    let dst_endpoint = match endpoint_by_id(&state, &payload.dst_endpoint_id) {
        Some(endpoint) => endpoint.clone(),
        None => return (StatusCode::BAD_REQUEST, "Unknown destination endpoint").into_response(),
    };
    if payload.items.is_empty() {
        return (StatusCode::BAD_REQUEST, "No indices selected").into_response();
    }

    let copy_suffix_override = None;
    let alias_suffix_override = None;
    let defaults = payload.defaults.clone();

    let mut jobs = Vec::new();
    for item in &payload.items {
        if item.source_name.trim().is_empty() || item.dest_base_name.trim().is_empty() {
            return (StatusCode::BAD_REQUEST, "Missing index name").into_response();
        }
        let overrides = item.overrides.clone().unwrap_or_default();
        let buffer_size = overrides.buffer_size.unwrap_or(defaults.buffer_size);
        if buffer_size == 0 {
            return (StatusCode::BAD_REQUEST, "buffer_size must be > 0").into_response();
        }
        let copy_content = overrides.copy_content.unwrap_or(defaults.copy_content);
        let copy_mapping = overrides.copy_mapping.unwrap_or(defaults.copy_mapping);
        let delete_if_exists = overrides.delete_if_exists.unwrap_or(defaults.delete_if_exists);
        let number_of_replicas = overrides
            .number_of_replicas
            .or(defaults.number_of_replicas);
        let number_of_shards = overrides
            .number_of_shards
            .or(defaults.number_of_shards);
        let alias_enabled = overrides
            .alias_enabled
            .unwrap_or(defaults.alias_enabled && payload.alias.enabled);
        let alias_remove_if_exists = overrides
            .alias_remove_if_exists
            .unwrap_or(defaults.alias_remove_if_exists);
        let alias_base = if alias_enabled {
            item.alias_base_name
                .clone()
                .or_else(|| Some(item.dest_base_name.clone()))
        } else {
            None
        };

        let index = InputIndex {
            name: item.source_name.clone(),
            buffer_size,
            copy_content,
            copy_mapping,
            delete_if_exists,
            routing_field: None,
            number_of_shards,
            number_of_replicas,
            dest_name: Some(item.dest_base_name.clone()),
            alias_name: alias_base,
            use_dest_name_as_is: true,
            use_alias_name_as_is: alias_enabled,
            alias_enabled,
            alias_remove_if_exists: Some(alias_remove_if_exists),
            use_src_prefix: false,
            use_dst_prefix: false,
            use_from_suffix: false,
            split: None,
            custom: None,
        };
        jobs.push(JobPlan {
            name: format!("{}-{}", DEFAULT_STAGE_NAME, item.source_name),
            index,
            date_from: None,
            date_to: None,
            leftover: false,
            split_doc_count: None,
        });
    }

    let stages = vec![StagePlan {
        name: DEFAULT_STAGE_NAME.to_string(),
        jobs,
    }];
    let mut wizard_tenants = src_endpoint.tenants.clone();
    for tenant in &dst_endpoint.tenants {
        if !wizard_tenants.iter().any(|value| value == tenant) {
            wizard_tenants.push(tenant.clone());
        }
    }
    let template_snapshot = TemplateSnapshot {
        id: "wizard".to_string(),
        name: "Wizard selection".to_string(),
        path: "".to_string(),
        number_of_replicas: defaults.number_of_replicas,
        tenants: wizard_tenants,
    };
    let wizard_snapshot = WizardSnapshot {
        defaults: defaults.clone(),
        rename: payload.rename.clone(),
        alias: payload.alias.clone(),
        items: payload.items.clone(),
    };

    match create_run_state_from_stages(
        &state,
        stages,
        template_snapshot,
        &src_endpoint,
        &dst_endpoint,
        payload.dry_run,
        copy_suffix_override,
        alias_suffix_override,
        Some(wizard_snapshot),
        RunMode::Copy,
    )
    .await
    {
        Ok(run_id) => Redirect::to(&with_base(&state, &format!("/runs/{}", run_id)))
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create run: {}", err),
        )
            .into_response(),
    }
}

async fn wizard_sources(
    State(state): State<Arc<AppState>>,
    Query(query): Query<WizardSourcesQuery>,
) -> impl IntoResponse {
    let src_endpoint = match endpoint_by_id(&state, &query.src_endpoint_id) {
        Some(endpoint) => endpoint.clone(),
        None => return (StatusCode::BAD_REQUEST, "Unknown source endpoint").into_response(),
    };
    let pattern = query.pattern.unwrap_or_else(|| "*".to_string());
    let pattern = if pattern.trim().is_empty() {
        "*".to_string()
    } else {
        pattern.trim().to_string()
    };

    let indices = match fetch_cat_indices(&state, &src_endpoint, &pattern).await {
        Ok(items) => items,
        Err(err) => {
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };
    let aliases = match fetch_cat_aliases(&state, &src_endpoint, &pattern).await {
        Ok(items) => items,
        Err(err) => {
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let mut items = Vec::new();
    items.extend(indices);
    items.extend(aliases);
    Json(items).into_response()
}

#[derive(Deserialize)]
struct CatIndexRow {
    index: String,
    #[serde(rename = "docs.count")]
    docs_count: Option<String>,
    #[serde(rename = "store.size")]
    store_size: Option<String>,
}

#[derive(Deserialize)]
struct CatAliasRow {
    alias: String,
    index: String,
}

async fn fetch_cat_indices(
    state: &Arc<AppState>,
    endpoint: &EndpointConfig,
    pattern: &str,
) -> Result<Vec<WizardSourceItem>, String> {
    let url = format!("{}/_cat/indices", endpoint.url);
    let mut request = state
        .client
        .get(&url)
        .query(&[
            ("format", "json"),
            ("h", "index,docs.count,store.size"),
            ("index", pattern),
        ]);
    if let Some(auth) = &endpoint.auth {
        request = request.basic_auth(auth.username.clone(), auth.password.clone());
    }
    let response = request
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| format!("Failed to list indices: {e}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "Failed to list indices: {}",
            response.status()
        ));
    }
    let rows = response
        .json::<Vec<CatIndexRow>>()
        .await
        .map_err(|e| format!("Failed to parse indices response: {e}"))?;
    let items = rows
        .into_iter()
        .map(|row| WizardSourceItem {
            name: row.index,
            kind: "index".to_string(),
            docs: row
                .docs_count
                .and_then(|value| value.replace(',', "").parse::<u64>().ok()),
            size: row.store_size,
            indices: Vec::new(),
        })
        .collect::<Vec<_>>();
    Ok(items)
}

async fn fetch_cat_aliases(
    state: &Arc<AppState>,
    endpoint: &EndpointConfig,
    pattern: &str,
) -> Result<Vec<WizardSourceItem>, String> {
    let url = format!("{}/_cat/aliases", endpoint.url);
    let mut request = state
        .client
        .get(&url)
        .query(&[("format", "json"), ("h", "alias,index"), ("name", pattern)]);
    if let Some(auth) = &endpoint.auth {
        request = request.basic_auth(auth.username.clone(), auth.password.clone());
    }
    let response = request
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| format!("Failed to list aliases: {e}"))?;
    let rows = if response.status().is_success() {
        response
            .json::<Vec<CatAliasRow>>()
            .await
            .map_err(|e| format!("Failed to parse aliases response: {e}"))?
    } else if response.status() == StatusCode::BAD_REQUEST && pattern != "*" {
        // Some ES versions reject the name filter for _cat/aliases; fallback to client-side filtering.
        let mut fallback = state
            .client
            .get(&url)
            .query(&[("format", "json"), ("h", "alias,index")]);
        if let Some(auth) = &endpoint.auth {
            fallback = fallback.basic_auth(auth.username.clone(), auth.password.clone());
        }
        let fallback_response = fallback
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| format!("Failed to list aliases: {e}"))?;
        if !fallback_response.status().is_success() {
            return Err(format!(
                "Failed to list aliases: {}",
                fallback_response.status()
            ));
        }
        let all_rows = fallback_response
            .json::<Vec<CatAliasRow>>()
            .await
            .map_err(|e| format!("Failed to parse aliases response: {e}"))?;
        all_rows
            .into_iter()
            .filter(|row| wildcard_match(&row.alias, pattern))
            .collect()
    } else {
        return Err(format!(
            "Failed to list aliases: {}",
            response.status()
        ));
    };
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        map.entry(row.alias).or_default().push(row.index);
    }
    let mut items = Vec::new();
    for (alias, indices) in map {
        items.push(WizardSourceItem {
            name: alias,
            kind: "alias".to_string(),
            docs: None,
            size: None,
            indices,
        });
    }
    Ok(items)
}

fn wildcard_match(value: &str, pattern: &str) -> bool {
    let value = value.to_ascii_lowercase();
    let pattern = pattern.to_ascii_lowercase();
    wildcard_match_bytes(value.as_bytes(), pattern.as_bytes())
}

fn wildcard_match_bytes(value: &[u8], pattern: &[u8]) -> bool {
    let mut dp = vec![false; pattern.len() + 1];
    dp[0] = true;
    for (i, &p) in pattern.iter().enumerate() {
        if p == b'*' {
            dp[i + 1] = dp[i];
        }
    }
    for &c in value {
        let mut next = vec![false; pattern.len() + 1];
        for (i, &p) in pattern.iter().enumerate() {
            match p {
                b'?' => next[i + 1] = dp[i],
                b'*' => next[i + 1] = next[i] || dp[i + 1],
                _ => next[i + 1] = dp[i] && p == c,
            }
        }
        dp = next;
    }
    dp[pattern.len()]
}

async fn update_max_concurrent_jobs(
    State(state): State<Arc<AppState>>,
    Form(form): Form<MaxConcurrentForm>,
) -> impl IntoResponse {
    let mut queues = state.destination_queues.lock().await;
    let current = queues
        .values()
        .filter_map(|queue| queue.max_concurrent_jobs)
        .next()
        .or(state.default_max_concurrent_jobs)
        .unwrap_or(0);
    let mut next = if let Some(value) = form.value {
        value as i32
    } else {
        let delta = form.delta.unwrap_or(0);
        current as i32 + delta
    };
    if next < 0 {
        next = 0;
    }
    let next_value = if next == 0 {
        None
    } else {
        Some(next as usize)
    };
    for queue in queues.values_mut() {
        queue.max_concurrent_jobs = next_value;
    }
    drop(queues);
    state.queue_notify.notify_one();
    Json(json!({
        "max_concurrent_jobs": next_value
    }))
}

async fn update_max_concurrent_jobs_for_endpoint(
    State(state): State<Arc<AppState>>,
    Path(endpoint_id): Path<String>,
    Form(form): Form<MaxConcurrentForm>,
) -> impl IntoResponse {
    let mut queues = state.destination_queues.lock().await;
    let queue = match queues.get_mut(&endpoint_id) {
        Some(queue) => queue,
        None => return (StatusCode::NOT_FOUND, "Unknown destination").into_response(),
    };
    let current = queue
        .max_concurrent_jobs
        .or(state.default_max_concurrent_jobs)
        .unwrap_or(0);
    let mut next = if let Some(value) = form.value {
        value as i32
    } else {
        let delta = form.delta.unwrap_or(0);
        current as i32 + delta
    };
    if next < 0 {
        next = 0;
    }
    queue.max_concurrent_jobs = if next == 0 {
        None
    } else {
        Some(next as usize)
    };
    let updated = queue.max_concurrent_jobs;
    drop(queues);
    state.queue_notify.notify_one();
    Json(json!({
        "endpoint_id": endpoint_id,
        "max_concurrent_jobs": updated
    }))
    .into_response()
}

async fn run_view(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    match build_run_view(&state, &run_id).await {
        Some(run) => {
            let template = RunTemplate {
                run,
                base_path: state.base_path.clone(),
                active_nav: "dashboard".to_string(),
            };
            Html(render_template(&template))
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "Run not found").into_response(),
    }
}

async fn job_view(
    State(state): State<Arc<AppState>>,
    Path((run_id, job_id)): Path<(String, String)>,
) -> impl IntoResponse {
    match build_job_view(&state, &run_id, &job_id).await {
        Some(job) => {
            let template = JobTemplate {
                run_id,
                job,
                base_path: state.base_path.clone(),
                active_nav: "dashboard".to_string(),
            };
            Html(render_template(&template))
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "Job not found").into_response(),
    }
}

#[derive(Deserialize)]
struct StreamQuery {
    stream: Option<String>,
}

async fn job_stream(
    State(state): State<Arc<AppState>>,
    Path((run_id, job_id)): Path<(String, String)>,
    Query(query): Query<StreamQuery>,
) -> impl IntoResponse {
    let stream_kind = query.stream.unwrap_or_else(|| "stdout".to_string());
    let sender = {
        let runs = state.runs.read().await;
        let run = match runs.runs.get(&run_id) {
            Some(run) => run,
            None => return (StatusCode::NOT_FOUND, "Run not found").into_response(),
        };
        let job = match run.jobs.get(&job_id) {
            Some(job) => job,
            None => return (StatusCode::NOT_FOUND, "Job not found").into_response(),
        };
        if stream_kind == "stderr" {
            job.stderr_tx.clone()
        } else {
            job.stdout_tx.clone()
        }
    };

    let rx = sender.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|msg| match msg {
        Ok(event) => {
            let payload = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
            Some(Ok::<axum::response::sse::Event, Infallible>(
                axum::response::sse::Event::default().data(payload),
            ))
        }
        Err(_) => None,
    });

    Sse::new(stream).into_response()
}

async fn job_status_stream(
    State(state): State<Arc<AppState>>,
    Path((run_id, job_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let seconds = if state.refresh_seconds == 0 {
        5
    } else {
        state.refresh_seconds.max(1)
    };
    let interval = tokio::time::interval(Duration::from_secs(seconds));
    let stream = IntervalStream::new(interval).then(move |_| {
        let state = Arc::clone(&state);
        let run_id = run_id.clone();
        let job_id = job_id.clone();
        async move {
            let (status, progress_label, eta_label) = {
                let runs = state.runs.read().await;
                runs.runs
                    .get(&run_id)
                    .and_then(|run| run.jobs.get(&job_id))
                    .map(|job| {
                        let progress_label = job
                            .progress_percent
                            .map(|value| format!("{:.2} %", value));
                        let eta_label =
                            estimate_eta_label(&job.started_at, job.progress_percent);
                        (job.status.as_str().to_string(), progress_label, eta_label)
                    })
                    .unwrap_or_else(|| ("missing".to_string(), None, None))
            };
            let payload = serde_json::to_string(&serde_json::json!({
                "status": status,
                "progress_label": progress_label,
                "eta_label": eta_label
            }))
            .unwrap_or_else(|_| "{}".to_string());
            Ok::<axum::response::sse::Event, Infallible>(
                axum::response::sse::Event::default().data(payload),
            )
        }
    });
    Sse::new(stream).into_response()
}

async fn status_view(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let summary = {
        let metrics = state.metrics.read().await;
        build_metrics_summary(metrics.samples.back())
    };

    let template = StatusTemplate {
        base_path: state.base_path.clone(),
        summary,
        active_nav: "status".to_string(),
    };
    Html(render_template(&template)).into_response()
}

async fn status_snapshot(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let metrics = state.metrics.read().await;
    let samples: Vec<MetricsSample> = metrics.samples.iter().cloned().collect();
    Json(samples)
}

async fn status_stream(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rx = state.metrics_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|msg| match msg {
        Ok(event) => {
            let payload = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
            Some(Ok::<axum::response::sse::Event, Infallible>(
                axum::response::sse::Event::default().data(payload),
            ))
        }
        Err(_) => None,
    });
    Sse::new(stream).into_response()
}

async fn runs_stream(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let seconds = if state.refresh_seconds == 0 {
        5
    } else {
        state.refresh_seconds.max(1)
    };
    let interval = tokio::time::interval(Duration::from_secs(seconds));
    let stream = IntervalStream::new(interval).then(move |_| {
        let state = Arc::clone(&state);
        async move {
            let runs = build_run_summaries(&state).await;
            let queue_limits = build_queue_limits(&state).await;
            let payload = serde_json::to_string(&serde_json::json!({
                "runs": runs,
                "queue_limits": queue_limits
            }))
            .unwrap_or_else(|_| "{}".to_string());
            Ok::<axum::response::sse::Event, Infallible>(
                axum::response::sse::Event::default().data(payload),
            )
        }
    });
    Sse::new(stream).into_response()
}

async fn runs_snapshot(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let runs = build_run_summaries(&state).await;
    let queue_limits = build_queue_limits(&state).await;
    Json(json!({
        "runs": runs,
        "queue_limits": queue_limits
    }))
    .into_response()
}

async fn jobs_view(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let runs = build_run_options(&state).await;
    let template = JobsTemplate {
        base_path: state.base_path.clone(),
        runs,
        runs_json: runs_json(&state).await,
        active_nav: "jobs".to_string(),
    };
    Html(render_template(&template)).into_response()
}

async fn jobs_snapshot(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let jobs = build_jobs_list(&state).await;
    Json(jobs).into_response()
}

async fn jobs_stream(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let seconds = if state.refresh_seconds == 0 {
        5
    } else {
        state.refresh_seconds.max(1)
    };
    let interval = tokio::time::interval(Duration::from_secs(seconds));
    let stream = IntervalStream::new(interval).then(move |_| {
        let state = Arc::clone(&state);
        async move {
            let jobs = build_jobs_list(&state).await;
            let payload = serde_json::to_string(&jobs).unwrap_or_else(|_| "[]".to_string());
            Ok::<axum::response::sse::Event, Infallible>(
                axum::response::sse::Event::default().data(payload),
            )
        }
    });
    Sse::new(stream).into_response()
}

async fn run_stream(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    let seconds = if state.refresh_seconds == 0 {
        5
    } else {
        state.refresh_seconds.max(1)
    };
    let interval = tokio::time::interval(Duration::from_secs(seconds));
    let stream = IntervalStream::new(interval).then(move |_| {
        let state = Arc::clone(&state);
        let run_id = run_id.clone();
        async move {
            let payload = match build_run_view(&state, &run_id).await {
                Some(run) => serde_json::to_string(&run).unwrap_or_else(|_| "{}".to_string()),
                None => "{}".to_string(),
            };
            Ok::<axum::response::sse::Event, Infallible>(
                axum::response::sse::Event::default().data(payload),
            )
        }
    });
    Sse::new(stream).into_response()
}

async fn delete_run(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    let (run_dir, has_running) = {
        let runs = state.runs.read().await;
        let run = match runs.runs.get(&run_id) {
            Some(run) => run,
            None => return (StatusCode::NOT_FOUND, "Run not found").into_response(),
        };
        let has_running = run
            .jobs
            .values()
            .any(|job| matches!(job.status, JobStatus::Running));
        (state.runs_dir.join(&run_id), has_running)
    };

    if has_running {
        return (
            StatusCode::CONFLICT,
            "Run has running jobs; stop them before removing.",
        )
            .into_response();
    }

    {
        let mut runs = state.runs.write().await;
        runs.runs.remove(&run_id);
        runs.order.retain(|id| id != &run_id);
    }

    if let Err(err) = tokio::fs::remove_dir_all(&run_dir).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to remove run directory: {err}"),
        )
            .into_response();
    }

    (StatusCode::OK, "Removed").into_response()
}

async fn retry_failed(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    let job_ids = {
        let runs = state.runs.read().await;
        let run = match runs.runs.get(&run_id) {
            Some(run) => run,
            None => return (StatusCode::NOT_FOUND, "Run not found").into_response(),
        };
        run.jobs
            .values()
            .filter(|job| matches!(job.status, JobStatus::Failed))
            .map(|job| job.id.clone())
            .collect::<Vec<_>>()
    };

    if job_ids.is_empty() {
        return (StatusCode::OK, "No failed jobs to retry.").into_response();
    }

    for job_id in job_ids.iter() {
        if let Err(err) = start_job_runner(Arc::clone(&state), run_id.clone(), job_id.clone()).await
        {
            warn!("Failed to retry job {}: {}", job_id, err);
        }
    }

    (
        StatusCode::OK,
        format!("Retrying {} failed job(s).", job_ids.len()),
    )
        .into_response()
}

async fn export_run(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    let run_dir = state.runs_dir.join(&run_id);
    if !run_dir.exists() {
        return (StatusCode::NOT_FOUND, "Run not found").into_response();
    }

    let run_dir_clone = run_dir.clone();
    let payload = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, String> {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = FileOptions::<()>::default();

        for entry in WalkDir::new(&run_dir_clone).into_iter().filter_map(Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                continue;
            }
            let rel_path = path
                .strip_prefix(&run_dir_clone)
                .map_err(|e| e.to_string())?;
            let rel_name = rel_path.to_string_lossy();
            zip.start_file(rel_name, options)
                .map_err(|e| e.to_string())?;
            if should_strip_ansi(path, rel_path) {
                let content = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
                let stripped = strip_ansi_codes(&content);
                zip.write_all(stripped.as_bytes())
                    .map_err(|e: std::io::Error| e.to_string())?;
            } else {
                let mut file = std::fs::File::open(path).map_err(|e| e.to_string())?;
                std::io::copy(&mut file, &mut zip).map_err(|e| e.to_string())?;
            }
        }

        zip.finish()
            .map_err(|e| e.to_string())
            .map(|cursor| cursor.into_inner())
    })
    .await
    .map_err(|e| e.to_string());

    let payload = match payload {
        Ok(Ok(data)) => data,
        Ok(Err(err)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to export run: {err}"),
            )
                .into_response()
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to export run: {err}"),
            )
                .into_response()
        }
    };

    let file_name = format!("run-{}.zip", run_id);
    let mut response = axum::response::Response::new(axum::body::Body::from(payload));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/zip"),
    );
    if let Ok(value) = HeaderValue::from_str(&format!("attachment; filename=\"{}\"", file_name)) {
        response
            .headers_mut()
            .insert(header::CONTENT_DISPOSITION, value);
    }
    response
}

fn should_strip_ansi(path: &std::path::Path, rel_path: &std::path::Path) -> bool {
    if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
        return false;
    }
    matches!(rel_path.components().next(), Some(std::path::Component::Normal(name)) if name == "logs")
}

fn strip_ansi_codes(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            if matches!(chars.peek(), Some('[')) {
                chars.next();
                while let Some(next) = chars.next() {
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
                continue;
            }
        }
        output.push(ch);
    }
    output
}

async fn start_stage(
    State(state): State<Arc<AppState>>,
    Path((run_id, stage_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let job_ids = {
        let runs = state.runs.read().await;
        let run = match runs.runs.get(&run_id) {
            Some(run) => run,
            None => return (StatusCode::NOT_FOUND, "Run not found").into_response(),
        };
        let stage = match run.stages.iter().find(|s| s.id == stage_id) {
            Some(stage) => stage,
            None => return (StatusCode::NOT_FOUND, "Stage not found").into_response(),
        };
        stage.job_ids.clone()
    };

    for job_id in job_ids {
        let state_clone = Arc::clone(&state);
        let run_id_clone = run_id.clone();
        tokio::spawn(async move {
            if let Err(err) = start_job_runner(
                Arc::clone(&state_clone),
                run_id_clone.clone(),
                job_id.clone(),
            )
            .await {
                warn!("Failed to start job {}: {}", job_id, err);
            }
        });
    }

    Redirect::to(&with_base(&state, &format!("/runs/{}", run_id))).into_response()
}

#[derive(Deserialize)]
struct StartJobQuery {
    redirect: Option<String>,
}

async fn start_job(
    State(state): State<Arc<AppState>>,
    Path((run_id, job_id)): Path<(String, String)>,
    Query(query): Query<StartJobQuery>,
) -> impl IntoResponse {
    if let Err(err) = start_job_runner(Arc::clone(&state), run_id.clone(), job_id.clone()).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to start job: {}", err),
        )
            .into_response();
    }
    if query.redirect.as_deref() == Some("run") {
        return Redirect::to(&with_base(&state, &format!("/runs/{}", run_id))).into_response();
    }
    Redirect::to(&with_base(&state, &format!("/runs/{}/jobs/{}", run_id, job_id))).into_response()
}

async fn stop_job(
    State(state): State<Arc<AppState>>,
    Path((run_id, job_id)): Path<(String, String)>,
) -> impl IntoResponse {
    match stop_job_internal(&state, &run_id, &job_id).await {
        Ok(message) => (StatusCode::OK, message).into_response(),
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

async fn stop_stage(
    State(state): State<Arc<AppState>>,
    Path((run_id, stage_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let job_ids = {
        let runs = state.runs.read().await;
        let run = match runs.runs.get(&run_id) {
            Some(run) => run,
            None => return (StatusCode::NOT_FOUND, "run not found").into_response(),
        };
        let stage = match run.stages.iter().find(|stage| stage.id == stage_id) {
            Some(stage) => stage,
            None => return (StatusCode::NOT_FOUND, "stage not found").into_response(),
        };
        stage.job_ids.clone()
    };

    let mut stopped = 0usize;
    let mut errors = Vec::new();
    for job_id in job_ids {
        match stop_job_internal(&state, &run_id, &job_id).await {
            Ok(_) => stopped += 1,
            Err(err) => errors.push(err),
        }
    }

    if errors.is_empty() {
        Redirect::to(&with_base(&state, &format!("/runs/{}", run_id))).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            format!("Stop requested for {stopped} job(s). Errors: {}", errors.join("; ")),
        )
            .into_response()
    }
}

async fn stop_run(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    let job_ids = {
        let runs = state.runs.read().await;
        let run = match runs.runs.get(&run_id) {
            Some(run) => run,
            None => return (StatusCode::NOT_FOUND, "run not found").into_response(),
        };
        run.jobs.keys().cloned().collect::<Vec<_>>()
    };

    let mut stopped = 0usize;
    let mut errors = Vec::new();
    for job_id in job_ids {
        match stop_job_internal(&state, &run_id, &job_id).await {
            Ok(_) => stopped += 1,
            Err(err) => errors.push(err),
        }
    }

    if errors.is_empty() {
        Redirect::to(&with_base(&state, &format!("/runs/{}", run_id))).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            format!("Stop requested for {stopped} job(s). Errors: {}", errors.join("; ")),
        )
            .into_response()
    }
}

async fn start_job_runner(
    state: Arc<AppState>,
    run_id: String,
    job_id: String,
) -> Result<(), String> {
    let destination_id = {
        let runs = state.runs.read().await;
        let run = runs
            .runs
            .get(&run_id)
            .ok_or_else(|| "run not found".to_string())?;
        run.dst_endpoint.id.clone()
    };
    ensure_destination_queue(&state, &destination_id).await;
    let max_concurrent_jobs = {
        let queues = state.destination_queues.lock().await;
        queues
            .get(&destination_id)
            .and_then(|queue| queue.max_concurrent_jobs)
            .or(state.default_max_concurrent_jobs)
    };
    let (config_path, stdout_path, stderr_path, snapshot, dry_run, queued) = {
        let mut queued = false;
        let mut runs = state.runs.write().await;
        let running_count = count_running_jobs_for_destination(&runs, &destination_id);
        let run = runs
            .runs
            .get_mut(&run_id)
            .ok_or_else(|| "run not found".to_string())?;
        let (config_path, stdout_path, stderr_path, dry_run) = {
            let job = run
                .jobs
                .get_mut(&job_id)
                .ok_or_else(|| "job not found".to_string())?;
            if matches!(job.status, JobStatus::Running) {
                return Ok(());
            }
            if let Some(max) = max_concurrent_jobs {
                if running_count >= max {
                    job.status = JobStatus::Queued;
                    queued = true;
                }
            }
            if !queued {
                job.status = JobStatus::Running;
                job.exit_code = None;
                job.started_at = Some(now_string());
                job.finished_at = None;
                job.pid = None;
                job.progress_percent = None;
                job.completed_line = false;
                job.stdout_tail.clear();
                job.stderr_tail.clear();
            }
            (
                job.config_path.clone(),
                job.stdout_path.clone(),
                job.stderr_path.clone(),
                run.dry_run,
            )
        };
        let snapshot = run_snapshot(run);
        (config_path, stdout_path, stderr_path, snapshot, dry_run, queued)
    };
    let runs_dir = state.runs_dir.clone();
    tokio::spawn(async move {
        if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
            warn!("Failed to persist run: {}", err);
        }
    });
    if queued {
        enqueue_job(&state, run_id.clone(), job_id.clone(), destination_id.clone()).await;
        return Ok(());
    }

    remove_from_queue(&state, &run_id, &job_id).await;

    let state_clone = Arc::clone(&state);
    let run_id = run_id.to_string();
    let job_id = job_id.to_string();
    if dry_run {
        tokio::spawn(async move {
            if let Err(err) = run_dry_job(
                state_clone,
                run_id,
                job_id,
                config_path,
                stdout_path,
                stderr_path,
            )
            .await
            {
                warn!("Dry run failed: {}", err);
            }
        });
    } else {
        let es_copy_path = state.es_copy_path.clone();
        tokio::spawn(async move {
            if let Err(err) = run_job_process(
                state_clone,
                run_id,
                job_id,
                es_copy_path,
                config_path,
                stdout_path,
                stderr_path,
            )
            .await
            {
                warn!("Job failed to start: {}", err);
            }
        });
    }

    Ok(())
}

fn count_running_jobs_for_destination(runs: &RunStore, destination_id: &str) -> usize {
    runs.runs
        .values()
        .filter(|run| run.dst_endpoint.id == destination_id)
        .flat_map(|run| run.jobs.values())
        .filter(|job| matches!(job.status, JobStatus::Running))
        .count()
}

async fn enqueue_job(
    state: &Arc<AppState>,
    run_id: String,
    job_id: String,
    destination_id: String,
) {
    let mut queues = state.destination_queues.lock().await;
    let queue = queues
        .entry(destination_id.clone())
        .or_insert_with(|| DestinationQueue {
            max_concurrent_jobs: state.default_max_concurrent_jobs,
            queue: VecDeque::new(),
        });
    if queue
        .queue
        .iter()
        .any(|job| job.run_id == run_id && job.job_id == job_id)
    {
        return;
    }
    queue.queue.push_back(QueuedJob {
        run_id,
        job_id,
        destination_id,
    });
    state.queue_notify.notify_one();
}

async fn remove_from_queue(state: &Arc<AppState>, run_id: &str, job_id: &str) {
    let mut queues = state.destination_queues.lock().await;
    for queue in queues.values_mut() {
        queue.queue.retain(|job| !(job.run_id == run_id && job.job_id == job_id));
    }
}

fn start_queue_worker(state: Arc<AppState>) {
    tokio::spawn(async move {
        queue_worker(state).await;
    });
}

async fn queue_worker(state: Arc<AppState>) {
    loop {
        state.queue_notify.notified().await;
        loop {
            let next = {
                let runs = state.runs.read().await;
                let running_by_dest = count_jobs_by_destination(&runs);
                let mut queues = state.destination_queues.lock().await;
                let mut selected: Option<QueuedJob> = None;
                for (dest_id, queue_state) in queues.iter_mut() {
                    let max = queue_state
                        .max_concurrent_jobs
                        .or(state.default_max_concurrent_jobs)
                        .unwrap_or(usize::MAX);
                    let running = running_by_dest.get(dest_id).map(|c| c.0).unwrap_or(0);
                    if running >= max {
                        continue;
                    }
                    if let Some(job) = queue_state.queue.pop_front() {
                        selected = Some(job);
                        break;
                    }
                }
                selected
            };
            let Some(next) = next else { break };
            let _ = start_job_runner(Arc::clone(&state), next.run_id, next.job_id).await;
        }
    }
}

async fn run_job_process(
    state: Arc<AppState>,
    run_id: String,
    job_id: String,
    es_copy_path: PathBuf,
    config_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
) -> Result<(), String> {
    let mut command = Command::new(es_copy_path);
    command
        .arg("-c")
        .arg(config_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            let message = format!("Failed to start process: {}", err);
            let _ = append_log_line(
                &state,
                &run_id,
                &job_id,
                LogStream::Stderr,
                &message,
                &stderr_path,
            )
            .await;
            mark_job_finished(&state, &run_id, &job_id, JobStatus::Failed, Some(-1)).await;
            return Err(err.to_string());
        }
    };
    let pid = child.id();
    if let Some(pid) = pid {
        let mut runs = state.runs.write().await;
        if let Some(run) = runs.runs.get_mut(&run_id) {
            if let Some(job) = run.jobs.get_mut(&job_id) {
                job.pid = Some(pid);
            }
        }
    }

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "stdout unavailable".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "stderr unavailable".to_string())?;

    let stdout_task = tokio::spawn(read_stream_lines(
        state.clone(),
        run_id.clone(),
        job_id.clone(),
        LogStream::Stdout,
        stdout,
        stdout_path,
    ));
    let stderr_task = tokio::spawn(read_stream_lines(
        state.clone(),
        run_id.clone(),
        job_id.clone(),
        LogStream::Stderr,
        stderr,
        stderr_path,
    ));

    let status = child.wait().await.map_err(|e| e.to_string())?;
    let exit_code = status.code().unwrap_or(-1);

    let status = if exit_code == 0 {
        JobStatus::Succeeded
    } else {
        JobStatus::Failed
    };
    mark_job_finished(&state, &run_id, &job_id, status, Some(exit_code)).await;

    let _ = stdout_task.await;
    let _ = stderr_task.await;
    Ok(())
}

async fn stop_job_internal(
    state: &Arc<AppState>,
    run_id: &str,
    job_id: &str,
) -> Result<String, String> {
    let (pid, stderr_path, snapshot, should_remove_queue, stop_message) = {
        let mut runs = state.runs.write().await;
        let run = runs
            .runs
            .get_mut(run_id)
            .ok_or_else(|| "run not found".to_string())?;
        let (pid, stderr_path, should_remove_queue, stop_message) = {
            let job = run
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| "job not found".to_string())?;
            match job.status {
                JobStatus::Running => {
                    job.status = JobStatus::Stopped;
                    job.exit_code = Some(-15);
                    job.finished_at = Some(now_string());
                    job.completed_line = false;
                    (
                        job.pid,
                        job.stderr_path.clone(),
                        true,
                        "Stop requested.".to_string(),
                    )
                }
                JobStatus::Queued | JobStatus::Pending => {
                    job.status = JobStatus::Stopped;
                    job.exit_code = Some(-15);
                    job.finished_at = Some(now_string());
                    job.completed_line = false;
                    (
                        None,
                        job.stderr_path.clone(),
                        true,
                        "Queued job stopped.".to_string(),
                    )
                }
                _ => {
                    return Ok("Job already finished.".to_string());
                }
            }
        };
        let snapshot = run_snapshot(run);
        (
            pid,
            stderr_path,
            snapshot,
            should_remove_queue,
            stop_message,
        )
    };

    let runs_dir = state.runs_dir.clone();
    tokio::spawn(async move {
        if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
            warn!("Failed to persist run: {}", err);
        }
    });

    if should_remove_queue {
        remove_from_queue(state, run_id, job_id).await;
    }

    if let Some(pid) = pid {
        let result = unsafe { kill(pid as i32, SIGTERM) };
        if result != 0 {
            let err = std::io::Error::last_os_error();
            if !is_not_found(&err) {
                return Err(format!("Failed to stop job (pid={pid}): {}", err));
            }
        }
    }

    let _ = append_log_line(
        state,
        run_id,
        job_id,
        LogStream::Stderr,
        if pid.is_some() {
            "Stop requested (SIGTERM)."
        } else {
            "Stop requested (queued)."
        },
        &stderr_path,
    )
    .await;

    state.queue_notify.notify_one();

    Ok(stop_message)
}

async fn run_dry_job(
    state: Arc<AppState>,
    run_id: String,
    job_id: String,
    config_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
) -> Result<(), String> {
    let mut snapshot: Option<RunPersist> = None;
    {
        let mut runs = state.runs.write().await;
        if let Some(run) = runs.runs.get_mut(&run_id) {
            if let Some(job) = run.jobs.get_mut(&job_id) {
                job.progress_percent = Some(100.0);
                job.completed_line = true;
                snapshot = Some(run_snapshot(run));
            }
        }
    }
    if let Some(snapshot) = snapshot {
        let runs_dir = state.runs_dir.clone();
        tokio::spawn(async move {
            if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
                warn!("Failed to persist run: {}", err);
            }
        });
    }

    let _ = append_log_line(
        &state,
        &run_id,
        &job_id,
        LogStream::Stdout,
        "Dry run: skipping es-copy-indices execution.",
        &stdout_path,
    )
    .await;
    let _ = append_log_line(
        &state,
        &run_id,
        &job_id,
        LogStream::Stdout,
        &format!("config_path=\"{}\"", config_path.to_string_lossy()),
        &stdout_path,
    )
    .await;
    if let Ok(content) = tokio::fs::read_to_string(&config_path).await {
        let _ = append_log_line(
            &state,
            &run_id,
            &job_id,
            LogStream::Stdout,
            "--- config.toml ---",
            &stdout_path,
        )
        .await;
        for line in content.lines() {
            let line = redact_config_line(line, state.redact_logs);
            let _ = append_log_line(
                &state,
                &run_id,
                &job_id,
                LogStream::Stdout,
                &line,
                &stdout_path,
            )
            .await;
        }
        let _ = append_log_line(
            &state,
            &run_id,
            &job_id,
            LogStream::Stdout,
            "--------------------",
            &stdout_path,
        )
        .await;
    }
    let mut env_vars = std::env::vars().collect::<Vec<_>>();
    env_vars.sort_by(|a, b| a.0.cmp(&b.0));
    let _ = append_log_line(
        &state,
        &run_id,
        &job_id,
        LogStream::Stderr,
        "Dry run environment (sorted):",
        &stderr_path,
    )
    .await;
    for (key, value) in env_vars {
        let value = redact_env_value(&key, &value, state.redact_logs);
        let _ = append_log_line(
            &state,
            &run_id,
            &job_id,
            LogStream::Stderr,
            &format!("{}={}", key, value),
            &stderr_path,
        )
        .await;
    }
    let _ = append_log_line(
        &state,
        &run_id,
        &job_id,
        LogStream::Stdout,
        "Application completed!",
        &stdout_path,
    )
    .await;
    mark_job_finished(&state, &run_id, &job_id, JobStatus::Succeeded, Some(0)).await;
    Ok(())
}

async fn read_stream_lines(
    state: Arc<AppState>,
    run_id: String,
    job_id: String,
    stream: LogStream,
    reader: impl tokio::io::AsyncRead + Unpin,
    log_path: PathBuf,
) {
    let mut reader = BufReader::new(reader).lines();
    while let Ok(Some(line)) = reader.next_line().await {
        if let Err(err) = append_log_line(&state, &run_id, &job_id, stream.clone(), &line, &log_path).await {
            warn!("Failed to append log: {}", err);
        }
    }
}

async fn append_log_line(
    state: &Arc<AppState>,
    run_id: &str,
    job_id: &str,
    stream: LogStream,
    line: &str,
    log_path: &PathBuf,
) -> Result<(), String> {
    let mut snapshot: Option<RunPersist> = None;
    let (sender, should_snapshot) = {
        let mut runs = state.runs.write().await;
        let run = runs
            .runs
            .get_mut(run_id)
            .ok_or_else(|| "run not found".to_string())?;
        let mut progress_changed = false;
        let sender = {
            let job = run
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| "job not found".to_string())?;
            let tail = if matches!(stream, LogStream::Stdout) {
                &mut job.stdout_tail
            } else {
                &mut job.stderr_tail
            };
            tail.push_back(line.to_string());
            while tail.len() > MAX_TAIL_LINES {
                tail.pop_front();
            }
            if let Some(progress) = extract_progress_percent(line) {
                let previous = job.progress_percent.unwrap_or(-1.0);
                if (progress - previous).abs() >= 0.1 {
                    job.progress_percent = Some(progress);
                    progress_changed = true;
                }
            }
            if line.contains("Application completed!") {
                job.completed_line = true;
                progress_changed = true;
            }
            if matches!(stream, LogStream::Stdout) {
                job.stdout_tx.clone()
            } else {
                job.stderr_tx.clone()
            }
        };
        if progress_changed {
            snapshot = Some(run_snapshot(run));
        }
        (sender, progress_changed)
    };

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await
        .map_err(|e| e.to_string())?;
    file.write_all(line.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    file.write_all(b"\n").await.map_err(|e| e.to_string())?;

    let _ = sender.send(LogEvent {
        stream,
        line: line.to_string(),
    });
    if should_snapshot {
        if let Some(snapshot) = snapshot {
            let runs_dir = state.runs_dir.clone();
            tokio::spawn(async move {
                if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
                    warn!("Failed to persist run: {}", err);
                }
            });
        }
    }
    Ok(())
}

fn extract_progress_percent(line: &str) -> Option<f64> {
    let start = line.rfind('(')?;
    let end = line[start..].find("%)")?;
    let slice = line.get(start + 1..start + end)?;
    let value = slice.trim().trim_end_matches('%');
    value.parse::<f64>().ok()
}

fn build_metrics_summary(sample: Option<&MetricsSample>) -> MetricsSummary {
    if let Some(sample) = sample {
        MetricsSummary {
            host_cpu: sample.host_cpu,
            proc_cpu: sample.proc_cpu,
            children_cpu: sample.children_cpu,
            total_cpu: sample.total_cpu,
            host_mem_used_mb: mem_to_mb(sample.host_mem_used_kb),
            host_mem_total_mb: mem_to_mb(sample.host_mem_total_kb),
            proc_mem_mb: mem_to_mb(sample.proc_mem_kb),
            children_mem_mb: mem_to_mb(sample.children_mem_kb),
            total_mem_mb: mem_to_mb(sample.total_mem_kb),
            running_jobs: sample.running_jobs,
            queued_jobs: sample.queued_jobs,
            load1: sample.load1,
            load5: sample.load5,
            load15: sample.load15,
        }
    } else {
        MetricsSummary::default()
    }
}

fn mem_to_mb(value: u64) -> u64 {
    value / 1024 / 1024
}

fn render_template<T: Template>(template: &T) -> String {
    let mut out = String::new();
    if let Err(err) = template.render_into(&mut out) {
        return format!("Template error: {err}");
    }
    out
}

fn start_metrics_sampler(
    metrics: Arc<RwLock<MetricsState>>,
    sender: broadcast::Sender<MetricsSample>,
    runs: Arc<RwLock<RunStore>>,
    seconds: u64,
) {
    let interval_seconds = if seconds == 0 { 5 } else { seconds };
    tokio::spawn(async move {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new();
        system.refresh_cpu_all();
        system.refresh_memory();
        system.refresh_processes(ProcessesToUpdate::All, true);
        tokio::time::sleep(Duration::from_millis(200)).await;
        system.refresh_cpu_all();
        system.refresh_memory();
        system.refresh_processes(ProcessesToUpdate::All, true);

        let mut ticker = tokio::time::interval(Duration::from_secs(interval_seconds));
        loop {
            ticker.tick().await;
            system.refresh_cpu_all();
            system.refresh_memory();
            system.refresh_processes(ProcessesToUpdate::All, true);

            let host_cpu = system.global_cpu_usage();
            let host_mem_total = system.total_memory();
            let host_mem_used = system.used_memory();
            let load = System::load_average();
            let (proc_cpu, proc_mem) = if let Some(process) = system.process(pid) {
                (process.cpu_usage(), process.memory())
            } else {
                (0.0, 0)
            };
            let mut children_cpu = 0.0f32;
            let mut children_mem = 0u64;
            for process in system.processes().values() {
                if process.pid() == pid {
                    continue;
                }
                let mut parent = process.parent();
                while let Some(ppid) = parent {
                    if ppid == pid {
                        children_cpu += process.cpu_usage();
                        children_mem += process.memory();
                        break;
                    }
                    parent = system.process(ppid).and_then(|p| p.parent());
                }
            }
            let total_cpu = proc_cpu + children_cpu;
            let total_mem = proc_mem + children_mem;
            let (running_jobs, queued_jobs) = {
                let guard = runs.read().await;
                let running = guard
                    .runs
                    .values()
                    .flat_map(|run| run.jobs.values())
                    .filter(|job| matches!(job.status, JobStatus::Running))
                    .count()
                    ;
                let queued = guard
                    .runs
                    .values()
                    .flat_map(|run| run.jobs.values())
                    .filter(|job| matches!(job.status, JobStatus::Queued))
                    .count();
                (running, queued)
            };

            let sample = MetricsSample {
                ts: chrono::Utc::now().timestamp(),
                host_cpu,
                host_mem_used_kb: host_mem_used,
                host_mem_total_kb: host_mem_total,
                proc_cpu,
                proc_mem_kb: proc_mem,
                children_cpu,
                children_mem_kb: children_mem,
                total_cpu,
                total_mem_kb: total_mem,
                running_jobs,
                queued_jobs,
                max_concurrent_jobs: None,
                load1: load.one,
                load5: load.five,
                load15: load.fifteen,
            };

            {
                let mut guard = metrics.write().await;
                guard.samples.push_back(sample.clone());
                while guard.samples.len() > 360 {
                    guard.samples.pop_front();
                }
            }

            let _ = sender.send(sample);
        }
    });
}

async fn mark_job_finished(
    state: &Arc<AppState>,
    run_id: &str,
    job_id: &str,
    status: JobStatus,
    exit_code: Option<i32>,
) {
    let snapshot = {
        let mut runs = state.runs.write().await;
        let run = match runs.runs.get_mut(run_id) {
            Some(run) => run,
            None => return,
        };
        let job = match run.jobs.get_mut(job_id) {
            Some(job) => job,
            None => return,
        };
        if matches!(job.status, JobStatus::Stopped) {
            return;
        }
        job.status = status;
        job.exit_code = exit_code;
        job.finished_at = Some(now_string());
        run_snapshot(run)
    };

    let runs_dir = state.runs_dir.clone();
    tokio::spawn(async move {
        if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
            warn!("Failed to persist run: {}", err);
        }
    });

    state.queue_notify.notify_one();
}

async fn build_run_summaries(state: &Arc<AppState>) -> Vec<RunSummary> {
    let runs = state.runs.read().await;
    let mut summaries = Vec::new();
    for run_id in runs.order.iter().rev() {
        if let Some(run) = runs.runs.get(run_id) {
            let mut jobs_running = 0;
            let mut jobs_queued = 0;
            let mut jobs_failed = 0;
            let mut jobs_succeeded = 0;
            let jobs_total = run.jobs.len();
            for job in run.jobs.values() {
                match job.status {
                    JobStatus::Running => jobs_running += 1,
                    JobStatus::Queued => jobs_queued += 1,
                    JobStatus::Failed => jobs_failed += 1,
                    JobStatus::Succeeded => jobs_succeeded += 1,
                    JobStatus::Stopped => jobs_failed += 1,
                    JobStatus::Pending => {}
                }
            }
            summaries.push(RunSummary {
                id: run.id.clone(),
                created_at: run.created_at.clone(),
                jobs_total,
                jobs_running,
                jobs_queued,
                jobs_failed,
                jobs_succeeded,
                src_name: run
                    .src_endpoint
                    .name
                    .clone()
                    .if_empty_then("unknown"),
                dst_name: run
                    .dst_endpoint
                    .name
                    .clone()
                    .if_empty_then("unknown"),
                dst_id: run.dst_endpoint.id.clone(),
                template_name: run.template.name.clone().if_empty_then("unnamed"),
                dry_run: run.dry_run,
                copy_suffix: run.copy_suffix.clone(),
                alias_suffix: run.alias_suffix.clone(),
                wizard: run.wizard.is_some(),
                run_mode: run.run_mode.clone(),
            });
        }
    }
    summaries
}

fn count_jobs_by_destination(runs: &RunStore) -> HashMap<String, (usize, usize)> {
    let mut counts = HashMap::new();
    for run in runs.runs.values() {
        let entry = counts
            .entry(run.dst_endpoint.id.clone())
            .or_insert((0usize, 0usize));
        for job in run.jobs.values() {
            match job.status {
                JobStatus::Running => entry.0 += 1,
                JobStatus::Queued => entry.1 += 1,
                _ => {}
            }
        }
    }
    counts
}

async fn build_queue_limits(state: &Arc<AppState>) -> Vec<QueueLimitView> {
    let runs = state.runs.read().await;
    let counts = count_jobs_by_destination(&runs);
    let queues = state.destination_queues.lock().await;
    let mut views = Vec::new();
    for endpoint in &state.endpoints {
        let (running, queued) = counts
            .get(&endpoint.id)
            .copied()
            .unwrap_or((0, 0));
        let max_concurrent_jobs = queues
            .get(&endpoint.id)
            .map(|queue| queue.max_concurrent_jobs)
            .unwrap_or(state.default_max_concurrent_jobs);
        views.push(QueueLimitView {
            endpoint_id: endpoint.id.clone(),
            endpoint_name: endpoint.name.clone(),
            running_jobs: running,
            queued_jobs: queued,
            max_concurrent_jobs,
        });
    }
    views
}

async fn build_run_options(state: &Arc<AppState>) -> Vec<RunOption> {
    let mut summaries = build_run_summaries(state).await;
    summaries.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    summaries
        .into_iter()
        .map(|run| RunOption {
            id: run.id.clone(),
            label: format!(
                "{} - {} → {} ({})",
                run.id, run.src_name, run.dst_name, run.created_at
            ),
        })
        .collect()
}

async fn build_jobs_list(state: &Arc<AppState>) -> Vec<JobListEntry> {
    let runs = state.runs.read().await;
    let mut entries = Vec::new();
    let mut run_list: Vec<&RunState> = runs.runs.values().collect();
    run_list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    for run in run_list {
        let run_label = format!(
            "{} - {} → {}",
            run.id, run.src_endpoint.name, run.dst_endpoint.name
        );
        let run_env_label = format!(
            "{} → {}",
            run.src_endpoint.name, run.dst_endpoint.name
        );
        let mut job_list: Vec<&JobState> = run.jobs.values().collect();
        job_list.sort_by(|a, b| a.name.cmp(&b.name));
        for job in job_list {
            let (progress_label, progress_width, eta_label) = match job.status {
                JobStatus::Running => {
                    let label = job.progress_percent.map(|value| format!("{:.2} %", value));
                    let width = job
                        .progress_percent
                        .map(|value| value.max(0.0).min(100.0).round() as u8);
                    let eta = estimate_eta_label(&job.started_at, job.progress_percent);
                    (label, width, eta)
                }
                JobStatus::Succeeded => {
                    let label = job.progress_percent.map(|value| format!("{:.2} %", value));
                    let width = job
                        .progress_percent
                        .map(|value| value.max(0.0).min(100.0).round() as u8);
                    (label, width, None)
                }
                _ => (None, None, None),
            };
            let can_stop = matches!(job.status, JobStatus::Running | JobStatus::Queued);
            let can_start = !matches!(job.status, JobStatus::Running | JobStatus::Queued);
            entries.push(JobListEntry {
                run_id: run.id.clone(),
                run_label: run_label.clone(),
                run_env_label: run_env_label.clone(),
                stage_name: job.stage_name.clone(),
                job_name: job.name.clone(),
                status: job.status.clone(),
                progress_label,
                progress_width,
                eta_label,
                logs_url: with_base(state, &format!("/runs/{}/jobs/{}", run.id, job.id)),
                start_url: with_base(state, &format!("/runs/{}/jobs/{}/start", run.id, job.id)),
                stop_url: with_base(state, &format!("/runs/{}/jobs/{}/stop", run.id, job.id)),
                can_start,
                can_stop,
            });
        }
    }
    entries
}

async fn build_run_view(state: &Arc<AppState>, run_id: &str) -> Option<RunView> {
    let runs = state.runs.read().await;
    let run = runs.runs.get(run_id)?;
    let mut stages = Vec::new();
    for stage in &run.stages {
        let mut jobs = Vec::new();
        let mut split_details = Vec::new();
        for job_id in &stage.job_ids {
            if let Some(job) = run.jobs.get(job_id) {
                let (progress_label, progress_width, eta_label) = match job.status {
                    JobStatus::Running => {
                        let label = job.progress_percent.map(|value| format!("{:.2} %", value));
                        let width = job
                            .progress_percent
                            .map(|value| value.max(0.0).min(100.0).round() as u8);
                        let eta = estimate_eta_label(&job.started_at, job.progress_percent);
                        (label, width, eta)
                    }
                    JobStatus::Succeeded => {
                        let label = job.progress_percent.map(|value| format!("{:.2} %", value));
                        let width = job
                            .progress_percent
                            .map(|value| value.max(0.0).min(100.0).round() as u8);
                        (label, width, None)
                    }
                    _ => (None, None, None),
                };
                jobs.push(JobSummary {
                    id: job.id.clone(),
                    name: job.name.clone(),
                    status: job.status.clone(),
                    progress_label,
                    progress_width,
                    eta_label,
                    completed_line: job.completed_line,
                });
                if let Some(field_name) = &job.split_field {
                    split_details.push(SplitDetailView {
                        job_name: job.name.clone(),
                        field_name: field_name.clone(),
                        date_from: job.split_from.clone(),
                        date_to: job.split_to.clone(),
                        leftover: job.split_leftover,
                        query: job.split_query.clone(),
                        doc_count: job.split_doc_count,
                    });
                }
            }
        }
        stages.push(StageView {
            id: stage.id.clone(),
            name: stage.name.clone(),
            jobs,
            split_details,
        });
    }
    Some(RunView {
        id: run.id.clone(),
        created_at: run.created_at.clone(),
        stages,
        src_endpoint: run.src_endpoint.clone(),
        dst_endpoint: run.dst_endpoint.clone(),
        template: run.template.clone(),
        dry_run: run.dry_run,
        copy_suffix: run.copy_suffix.clone(),
        alias_suffix: run.alias_suffix.clone(),
        wizard: run.wizard.clone(),
        run_mode: run.run_mode.clone(),
    })
}

async fn build_job_view(state: &Arc<AppState>, run_id: &str, job_id: &str) -> Option<JobView> {
    let runs = state.runs.read().await;
    let run = runs.runs.get(run_id)?;
    let job = run.jobs.get(job_id)?;
    let eta_label = estimate_eta_label(&job.started_at, job.progress_percent);
    let progress_label = job
        .progress_percent
        .map(|value| format!("{:.2} %", value));
    Some(JobView {
        id: job.id.clone(),
        name: job.name.clone(),
        stage_name: job.stage_name.clone(),
        status: job.status.clone(),
        stdout_tail: job.stdout_tail.iter().cloned().collect(),
        stderr_tail: job.stderr_tail.iter().cloned().collect(),
        progress_label,
        eta_label,
    })
}

async fn create_run_state_with_filter(
    state: &Arc<AppState>,
    template: &TemplateConfig,
    src_endpoint: &EndpointConfig,
    dst_endpoint: &EndpointConfig,
    dry_run: bool,
    copy_suffix_override: Option<String>,
    alias_suffix_override: Option<String>,
    selected_indices: Option<&HashSet<String>>,
    run_mode: RunMode,
) -> Result<String, String> {
    let stages = plan_stages_filtered(
        template,
        state,
        src_endpoint,
        selected_indices,
        run_mode.clone(),
    )
    .await?;
    create_run_state_from_stages(
        state,
        stages,
        template_snapshot(template),
        src_endpoint,
        dst_endpoint,
        dry_run,
        copy_suffix_override,
        alias_suffix_override,
        None,
        run_mode,
    )
    .await
}

async fn create_run_state_from_stages(
    state: &Arc<AppState>,
    stages: Vec<StagePlan>,
    template_snapshot: TemplateSnapshot,
    src_endpoint: &EndpointConfig,
    dst_endpoint: &EndpointConfig,
    dry_run: bool,
    copy_suffix_override: Option<String>,
    alias_suffix_override: Option<String>,
    wizard: Option<WizardSnapshot>,
    run_mode: RunMode,
) -> Result<String, String> {
    let run_id = unique_run_id(&state.runs_dir);
    let run_dir = state.runs_dir.join(&run_id);
    let configs_dir = run_dir.join("configs");
    let logs_dir = run_dir.join("logs");
    tokio::fs::create_dir_all(&configs_dir)
        .await
        .map_err(|e| e.to_string())?;
    tokio::fs::create_dir_all(&logs_dir)
        .await
        .map_err(|e| e.to_string())?;

    let copy_suffix = if let Some(raw) = copy_suffix_override {
        if raw.is_empty() { None } else { Some(raw) }
    } else {
        state.copy_suffix.clone()
    };
    let alias_suffix = if let Some(raw) = alias_suffix_override {
        if raw.is_empty() { None } else { Some(raw) }
    } else {
        state.alias_suffix.clone()
    };

    let src_effective = src_endpoint.clone();
    let mut dst_effective = dst_endpoint.clone();
    if run_mode == RunMode::Backup {
        if let Some(dir) = dst_effective.backup_dir.clone() {
            let adjusted = PathBuf::from(dir).join(&run_id);
            dst_effective.backup_dir = Some(adjusted.to_string_lossy().to_string());
        }
    }

    let mut stage_states = Vec::new();
    let mut job_states = HashMap::new();
    for stage in stages {
        let stage_id = slugify(&stage.name);
        let mut job_ids = Vec::new();
        for job in stage.jobs {
            let job_id = slugify(&job.name);
            job_ids.push(job_id.clone());

            let stdout_path = logs_dir.join(format!("{}.stdout.log", job_id));
            let stderr_path = logs_dir.join(format!("{}.stderr.log", job_id));
            let config_path = configs_dir.join(format!("{}.toml", job_id));

            let output_config = build_output_config(
                state,
                &job,
                &src_effective,
                &dst_effective,
                copy_suffix.as_deref(),
                alias_suffix.as_deref(),
                run_mode.clone(),
            )?;
            let split_field = job.index.split.as_ref().map(|s| s.field_name.clone());
            let split_query = output_config
                .indices
                .get(0)
                .and_then(|index| index.custom.as_ref())
                .and_then(|custom| custom.query.clone());
            let toml = toml::to_string_pretty(&output_config).map_err(|e| e.to_string())?;
            tokio::fs::write(&config_path, toml)
                .await
                .map_err(|e| e.to_string())?;

            let (stdout_tx, _) = broadcast::channel(200);
            let (stderr_tx, _) = broadcast::channel(200);

            job_states.insert(
                job_id.clone(),
                JobState {
                    id: job_id,
                    name: job.name,
                    stage_id: stage_id.clone(),
                    stage_name: stage.name.clone(),
                    index_name: job.index.name.clone(),
                    status: JobStatus::Pending,
                    exit_code: None,
                    started_at: None,
                    finished_at: None,
                    pid: None,
                    config_path,
                    stdout_path,
                    stderr_path,
                    stdout_tail: VecDeque::with_capacity(MAX_TAIL_LINES),
                    stderr_tail: VecDeque::with_capacity(MAX_TAIL_LINES),
                    stdout_tx,
                    stderr_tx,
                    progress_percent: None,
                    completed_line: false,
                    split_field,
                    split_from: job.date_from.clone(),
                    split_to: job.date_to.clone(),
                    split_leftover: job.leftover,
                    split_query,
                    split_doc_count: job.split_doc_count,
                },
            );
        }
        stage_states.push(StageState {
            id: stage_id,
            name: stage.name,
            job_ids,
        });
    }

    let created_at = now_string();
    let run_state = RunState {
        id: run_id.clone(),
        created_at,
        template: template_snapshot,
        src_endpoint: endpoint_snapshot(&src_effective),
        dst_endpoint: endpoint_snapshot(&dst_effective),
        dry_run,
        copy_suffix: copy_suffix.clone(),
        alias_suffix: alias_suffix.clone(),
        wizard,
        run_mode,
        stages: stage_states,
        jobs: job_states,
    };

    let snapshot = run_snapshot(&run_state);
    save_run_snapshot(&state.runs_dir, snapshot)
        .await
        .map_err(|e| e.to_string())?;

    let mut runs = state.runs.write().await;
    runs.order.push(run_id.clone());
    runs.runs.insert(run_id.clone(), run_state);
    Ok(run_id)
}

async fn load_runs(state: &AppState) {
    let mut runs = RunStore::default();
    let mut queued = Vec::new();
    let mut entries = match tokio::fs::read_dir(&state.runs_dir).await {
        Ok(entries) => entries,
        Err(err) => {
            warn!("Failed to read runs dir: {}", err);
            return;
        }
    };

    let mut run_entries = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let run_id = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };
        let run_file = path.join("run.json");
        if !run_file.exists() {
            continue;
        }
        run_entries.push((run_id, path));
    }

    run_entries.sort_by(|a, b| a.0.cmp(&b.0));

    for (run_id, path) in run_entries {
        let run_file = path.join("run.json");
        match tokio::fs::read_to_string(&run_file).await {
            Ok(content) => {
                let mut sanitized = false;
                let persist = match serde_json::from_str::<RunPersist>(&content) {
                    Ok(persist) => Some(persist),
                    Err(err) => {
                        let stripped = strip_ansi_codes(&content);
                        if stripped != content {
                            match serde_json::from_str::<RunPersist>(&stripped) {
                                Ok(persist) => {
                                    warn!(
                                        "Parsed run.json {:?} after stripping ANSI control codes.",
                                        run_file
                                    );
                                    sanitized = true;
                                    Some(persist)
                                }
                                Err(err) => {
                                    warn!("Failed to parse run.json {:?}: {}", run_file, err);
                                    None
                                }
                            }
                        } else {
                            warn!("Failed to parse run.json {:?}: {}", run_file, err);
                            None
                        }
                    }
                };
                let Some(persist) = persist else {
                    let bad_path = path.join(format!("run.json.bad-{}", file_timestamp()));
                    if let Err(rename_err) = tokio::fs::rename(&run_file, &bad_path).await {
                        warn!(
                            "Failed to quarantine run.json {:?}: {}",
                            bad_path, rename_err
                        );
                    } else {
                        let message = format!(
                            "Quarantined corrupted run.json for run {} as {}",
                            run_id,
                            bad_path.display()
                        );
                        let mut guard = state.quarantined_runs.write().await;
                        guard.push(message);
                    }
                    continue;
                };

                let mut job_map = HashMap::new();
                let mut run_changed = sanitized;
                for mut job in persist.jobs {
                    let (stdout_tx, _) = broadcast::channel(200);
                    let (stderr_tx, _) = broadcast::channel(200);
                    let stdout_tail =
                        read_tail_lines(job.stdout_path.as_str(), MAX_TAIL_LINES).await;
                    let stderr_tail =
                        read_tail_lines(job.stderr_path.as_str(), MAX_TAIL_LINES).await;
                    let completed_from_tail = stdout_tail
                        .iter()
                        .any(|line| line.contains("Application completed!"));
                    if completed_from_tail {
                        job.completed_line = true;
                    }
                    if matches!(job.status, JobStatus::Running) {
                        if job.completed_line {
                            job.status = JobStatus::Succeeded;
                            job.exit_code = Some(0);
                            job.finished_at = Some(now_string());
                            run_changed = true;
                        } else {
                            job.status = JobStatus::Failed;
                            job.exit_code = Some(-1);
                            job.finished_at = Some(now_string());
                            job.completed_line = false;
                            run_changed = true;
                        }
                    }
                    if matches!(job.status, JobStatus::Queued) {
                        queued.push(QueuedJob {
                            run_id: run_id.clone(),
                            job_id: job.id.clone(),
                            destination_id: persist.dst_endpoint.id.clone(),
                        });
                    }
                    job_map.insert(
                        job.id.clone(),
                        JobState {
                            id: job.id,
                            name: job.name,
                            stage_id: job.stage_id,
                            stage_name: job.stage_name,
                            index_name: job.index_name,
                            status: job.status,
                            exit_code: job.exit_code,
                            started_at: job.started_at,
                            finished_at: job.finished_at,
                            pid: None,
                            config_path: PathBuf::from(job.config_path),
                            stdout_path: PathBuf::from(job.stdout_path),
                            stderr_path: PathBuf::from(job.stderr_path),
                            stdout_tail: VecDeque::from(stdout_tail),
                            stderr_tail: VecDeque::from(stderr_tail),
                            stdout_tx,
                            stderr_tx,
                            progress_percent: job.progress_percent,
                            completed_line: job.completed_line,
                            split_field: job.split_field,
                            split_from: job.split_from,
                            split_to: job.split_to,
                            split_leftover: job.split_leftover,
                            split_query: job.split_query,
                            split_doc_count: job.split_doc_count,
                        },
                    );
                }
                let stages = persist
                    .stages
                    .into_iter()
                    .map(|stage| StageState {
                        id: stage.id,
                        name: stage.name,
                        job_ids: stage.job_ids,
                    })
                    .collect::<Vec<_>>();
                runs.order.push(run_id.clone());
                let run_state = RunState {
                    id: persist.id,
                    created_at: persist.created_at,
                    template: persist.template,
                    src_endpoint: persist.src_endpoint,
                    dst_endpoint: persist.dst_endpoint,
                    dry_run: persist.dry_run,
                    copy_suffix: persist.copy_suffix,
                    alias_suffix: persist.alias_suffix,
                    wizard: persist.wizard,
                    run_mode: persist.run_mode,
                    stages,
                    jobs: job_map,
                };
                if run_changed {
                    let snapshot = run_snapshot(&run_state);
                    let runs_dir = state.runs_dir.clone();
                    tokio::spawn(async move {
                        if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
                            warn!("Failed to persist run: {}", err);
                        }
                    });
                }
                runs.runs.insert(run_id, run_state);
            }
            Err(err) => warn!("Failed to read run.json: {}", err),
        }
    }

    let mut store = state.runs.write().await;
    store.order = runs.order;
    store.runs = runs.runs;

    if !queued.is_empty() {
        let mut queues = state.destination_queues.lock().await;
        for item in queued {
            let queue = queues.entry(item.destination_id.clone()).or_insert_with(|| {
                DestinationQueue {
                    max_concurrent_jobs: state.default_max_concurrent_jobs,
                    queue: VecDeque::new(),
                }
            });
            if !queue
                .queue
                .iter()
                .any(|job| job.run_id == item.run_id && job.job_id == item.job_id)
            {
                queue.queue.push_back(item);
            }
        }
    }
    let queues = state.destination_queues.lock().await;
    if queues.values().any(|queue| !queue.queue.is_empty()) {
        state.queue_notify.notify_one();
    }
}

async fn read_tail_lines(path: &str, limit: usize) -> Vec<String> {
    if let Ok(content) = tokio::fs::read_to_string(path).await {
        let mut lines = content.lines().map(|l| l.to_string()).collect::<Vec<_>>();
        if lines.len() > limit {
            lines.drain(0..lines.len() - limit);
        }
        lines
    } else {
        Vec::new()
    }
}

fn run_snapshot(run: &RunState) -> RunPersist {
    let stages = run
        .stages
        .iter()
        .map(|stage| StagePersist {
            id: stage.id.clone(),
            name: stage.name.clone(),
            job_ids: stage.job_ids.clone(),
        })
        .collect::<Vec<_>>();
    let jobs = run
        .jobs
        .values()
        .map(|job| JobPersist {
            id: job.id.clone(),
            name: job.name.clone(),
            stage_id: job.stage_id.clone(),
            stage_name: job.stage_name.clone(),
            index_name: job.index_name.clone(),
            status: job.status.clone(),
            exit_code: job.exit_code,
            started_at: job.started_at.clone(),
            finished_at: job.finished_at.clone(),
            config_path: job.config_path.to_string_lossy().to_string(),
            stdout_path: job.stdout_path.to_string_lossy().to_string(),
            stderr_path: job.stderr_path.to_string_lossy().to_string(),
            progress_percent: job.progress_percent,
            completed_line: job.completed_line,
            split_field: job.split_field.clone(),
            split_from: job.split_from.clone(),
            split_to: job.split_to.clone(),
            split_leftover: job.split_leftover,
            split_query: job.split_query.clone(),
            split_doc_count: job.split_doc_count,
        })
        .collect::<Vec<_>>();
    RunPersist {
        id: run.id.clone(),
        created_at: run.created_at.clone(),
        template: run.template.clone(),
        src_endpoint: run.src_endpoint.clone(),
        dst_endpoint: run.dst_endpoint.clone(),
        dry_run: run.dry_run,
        copy_suffix: run.copy_suffix.clone(),
        alias_suffix: run.alias_suffix.clone(),
        wizard: run.wizard.clone(),
        run_mode: run.run_mode.clone(),
        stages,
        jobs,
    }
}

async fn save_run_snapshot(runs_dir: &PathBuf, run: RunPersist) -> Result<(), String> {
    let dir = runs_dir.join(&run.id);
    if !dir.exists() {
        return Ok(());
    }
    let path = dir.join("run.json");
    let tmp_path = dir.join(format!("run.json.tmp-{}", file_timestamp()));
    if let Err(err) = tokio::fs::create_dir_all(&dir).await {
        if is_not_found(&err) {
            return Ok(());
        }
        return Err(err.to_string());
    }
    let content = serde_json::to_string_pretty(&run).map_err(|e| e.to_string())?;
    if let Err(err) = tokio::fs::write(&tmp_path, content).await {
        if is_not_found(&err) {
            return Ok(());
        }
        return Err(err.to_string());
    }
    if let Err(err) = tokio::fs::rename(&tmp_path, &path).await {
        if is_not_found(&err) {
            return Ok(());
        }
        return Err(err.to_string());
    }
    Ok(())
}

fn is_not_found(err: &std::io::Error) -> bool {
    err.kind() == ErrorKind::NotFound || err.raw_os_error() == Some(2)
}

#[derive(Clone, Debug)]
struct MainConfigBundle {
    endpoints: Vec<EndpointConfig>,
    backup_dir: Option<PathBuf>,
}

fn resolve_backup_dir_value(value: Option<String>) -> Option<PathBuf> {
    let raw = value?.trim().to_string();
    if raw.is_empty() {
        return None;
    }
    resolve_backup_dir_path(PathBuf::from(raw))
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component),
        }
    }
    normalized
}

fn resolve_backup_dir_path(path: PathBuf) -> Option<PathBuf> {
    if path.as_os_str().is_empty() {
        return None;
    }
    if path.is_absolute() {
        return Some(normalize_path(path));
    }
    let base = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    Some(normalize_path(base.join(path)))
}

fn load_main_config(path: &PathBuf) -> MainConfigBundle {
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read main config {:?}: {e}", path));
    let config: MainConfigFile =
        toml::from_str(&content).unwrap_or_else(|e| panic!("Invalid main config {:?}: {e}", path));
    let backup_dir = resolve_backup_dir_value(config.global.and_then(|g| g.backup_dir));
    let mut used = HashMap::new();
    let endpoints = config
        .endpoints
        .into_iter()
        .enumerate()
        .map(|(idx, endpoint)| {
            let base = slugify(&endpoint.name);
            let counter = used.entry(base.clone()).or_insert(0usize);
            let id = if *counter == 0 {
                base
            } else {
                format!("{}-{}", base, counter)
            };
            *counter += 1;
            let tenants: Vec<String> = endpoint
                .tenants
                .into_iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect();
            if tenants.is_empty() {
                panic!(
                    "Endpoint {:?} is missing tenants in {:?}",
                    endpoint.name, path
                );
            }
            EndpointConfig {
                id: if id.is_empty() {
                    format!("endpoint-{}", idx + 1)
                } else {
                    id
                },
                name: endpoint.name,
                url: endpoint.url,
                prefix: endpoint.prefix,
                number_of_replicas: endpoint.number_of_replicas,
                keep_alive: endpoint.keep_alive,
                auth: endpoint.auth,
                backup_dir: endpoint.backup_dir,
                tenants,
            }
        })
        .collect();
    MainConfigBundle {
        endpoints,
        backup_dir,
    }
}

fn load_templates(path: &PathBuf) -> Vec<TemplateConfig> {
    let mut templates = Vec::new();
    let mut entries = std::fs::read_dir(path)
        .unwrap_or_else(|e| panic!("Failed to read templates dir {:?}: {e}", path));
    let mut files = Vec::new();
    while let Some(entry) = entries.next() {
        let entry = entry.unwrap_or_else(|e| panic!("Failed to read templates dir: {e}"));
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                if ext.eq_ignore_ascii_case("toml") {
                    files.push(path);
                }
            }
        }
    }
    files.sort();
    let mut used = HashMap::new();
    for (idx, path) in files.into_iter().enumerate() {
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("Failed to read template {:?}: {e}", path));
        let config: TemplateFile =
            toml::from_str(&content).unwrap_or_else(|e| panic!("Invalid template {:?}: {e}", path));
        let file_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("template")
            .to_string();
        let display_name = config
            .global
            .as_ref()
            .and_then(|g| g.name.clone())
            .unwrap_or_else(|| file_name.clone());
        let global_replicas = config.global.as_ref().and_then(|g| g.number_of_replicas);
        let tenants: Vec<String> = config
            .global
            .as_ref()
            .map(|global| {
                global
                    .tenants
                    .iter()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if tenants.is_empty() {
            panic!("Template {:?} is missing [global].tenants", path);
        }
        let mut indices = config.indices;
        if let Some(default_replicas) = global_replicas {
            for index in &mut indices {
                if index.number_of_replicas.is_none() {
                    index.number_of_replicas = Some(default_replicas);
                }
            }
        }
        let base = slugify(&file_name);
        let counter = used.entry(base.clone()).or_insert(0usize);
        let id = if *counter == 0 {
            base
        } else {
            format!("{}-{}", base, counter)
        };
        *counter += 1;
        templates.push(TemplateConfig {
            id: if id.is_empty() {
                format!("template-{}", idx + 1)
            } else {
                id
            },
            name: display_name,
            path,
            number_of_replicas: global_replicas,
            indices,
            tenants,
        });
    }
    templates
}

fn endpoint_by_id<'a>(state: &'a AppState, id: &str) -> Option<&'a EndpointConfig> {
    state.endpoints.iter().find(|endpoint| endpoint.id == id)
}

fn template_by_id<'a>(state: &'a AppState, id: &str) -> Option<&'a TemplateConfig> {
    state.templates.iter().find(|template| template.id == id)
}

fn tenants_overlap(template: &TemplateConfig, endpoint: &EndpointConfig) -> bool {
    template
        .tenants
        .iter()
        .any(|tenant| endpoint.tenants.iter().any(|value| value == tenant))
}

fn endpoint_snapshot(endpoint: &EndpointConfig) -> EndpointSnapshot {
    EndpointSnapshot {
        id: endpoint.id.clone(),
        name: endpoint.name.clone(),
        url: endpoint.url.clone(),
        prefix: endpoint.prefix.clone(),
        number_of_replicas: endpoint.number_of_replicas,
        keep_alive: endpoint.keep_alive.clone(),
        backup_dir: endpoint.backup_dir.clone(),
        tenants: endpoint.tenants.clone(),
    }
}

fn template_snapshot(template: &TemplateConfig) -> TemplateSnapshot {
    TemplateSnapshot {
        id: template.id.clone(),
        name: template.name.clone(),
        path: template.path.to_string_lossy().to_string(),
        number_of_replicas: template.number_of_replicas,
        tenants: template.tenants.clone(),
    }
}

fn build_endpoint_views(state: &AppState) -> Vec<EndpointView> {
    state
        .endpoints
        .iter()
        .map(|endpoint| EndpointView {
            id: endpoint.id.clone(),
            name: endpoint.name.clone(),
            url: endpoint.url.clone(),
            prefix: endpoint.prefix.clone(),
            keep_alive: endpoint.keep_alive.clone(),
            tenants: endpoint.tenants.clone(),
        })
        .collect()
}

fn build_template_views(state: &AppState) -> Vec<TemplateView> {
    state
        .templates
        .iter()
        .map(|template| TemplateView {
            id: template.id.clone(),
            name: template.name.clone(),
            file_name: template
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("template")
                .to_string(),
            indices_count: template.indices.len(),
            number_of_replicas: template.number_of_replicas,
            indices: template
                .indices
                .iter()
                .map(|index| TemplateIndexView {
                    name: index.name.clone(),
                    has_split: index.split.is_some(),
                })
                .collect(),
            tenants: template.tenants.clone(),
        })
        .collect()
}

fn endpoints_json(state: &AppState) -> String {
    let endpoints = build_endpoint_views(state);
    serde_json::to_string(&endpoints).unwrap_or_else(|_| "[]".to_string())
}

fn templates_json(state: &AppState) -> String {
    let templates = build_template_views(state);
    serde_json::to_string(&templates).unwrap_or_else(|_| "[]".to_string())
}

async fn metrics_samples_json(state: &AppState) -> String {
    let metrics = state.metrics.read().await;
    let samples: Vec<MetricsSample> = metrics.samples.iter().cloned().collect();
    serde_json::to_string(&samples).unwrap_or_else(|_| "[]".to_string())
}

async fn runs_json(state: &Arc<AppState>) -> String {
    let runs = build_run_options(state).await;
    serde_json::to_string(&runs).unwrap_or_else(|_| "[]".to_string())
}

fn build_run_id() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let now = chrono::Utc::now();
    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}-{:04}", now.format("%Y%m%d-%H%M%S"), counter)
}

fn unique_run_id(runs_dir: &PathBuf) -> String {
    let mut candidate = build_run_id();
    let mut attempt = 0;
    while runs_dir.join(&candidate).exists() {
        attempt += 1;
        candidate = format!("{}-{}", build_run_id(), attempt);
    }
    candidate
}

fn now_string() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn file_timestamp() -> String {
    chrono::Utc::now().format("%Y%m%d-%H%M%S-%f").to_string()
}

fn estimate_eta_label(started_at: &Option<String>, progress_percent: Option<f64>) -> Option<String> {
    let started_at = started_at.as_ref()?;
    let progress = progress_percent?;
    if progress <= 0.0 || progress >= 100.0 {
        return None;
    }
    let parsed = chrono::DateTime::parse_from_rfc3339(started_at).ok()?;
    let started = parsed.with_timezone(&chrono::Utc);
    let elapsed = chrono::Utc::now().signed_duration_since(started).num_seconds();
    if elapsed <= 0 {
        return None;
    }
    let total_estimate = (elapsed as f64) * (100.0 / progress);
    let remaining = total_estimate - (elapsed as f64);
    if remaining <= 0.0 {
        return None;
    }
    Some(format!("ETA {}", format_duration(remaining.round() as i64)))
}

fn format_duration(seconds: i64) -> String {
    let mut remaining = seconds.max(0);
    let hours = remaining / 3600;
    remaining %= 3600;
    let minutes = remaining / 60;
    let secs = remaining % 60;
    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}

fn slugify(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>()
        .trim_matches('_')
        .to_lowercase()
}

trait StringExt {
    fn if_empty_then(self, fallback: &str) -> String;
}

impl StringExt for String {
    fn if_empty_then(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

async fn plan_stages_filtered(
    template: &TemplateConfig,
    state: &AppState,
    src_endpoint: &EndpointConfig,
    selected_indices: Option<&HashSet<String>>,
    run_mode: RunMode,
) -> Result<Vec<StagePlan>, String> {
    let mut stages: Vec<StagePlan> = Vec::new();
    let mut stage_index: HashMap<String, usize> = HashMap::new();
    let mut push_job = |stage_name: String, job: JobPlan| {
        if let Some(index) = stage_index.get(&stage_name).copied() {
            if let Some(stage) = stages.get_mut(index) {
                stage.jobs.push(job);
            }
        } else {
            let index = stages.len();
            stage_index.insert(stage_name.clone(), index);
            stages.push(StagePlan {
                name: stage_name,
                jobs: vec![job],
            });
        }
    };
    for index in &template.indices {
        if let Some(selected) = selected_indices {
            if !selected.contains(&index.name) {
                continue;
            }
        }
        if let Some(split) = &index.split {
            if matches!(run_mode, RunMode::Copy | RunMode::Backup) {
                let stage_name = format!("{}-{}", DEFAULT_STAGE_NAME, index.name);
                let date_ranges =
                    generate_date_intervals(state, src_endpoint, index, split).await?;
                let mut leftover = index.clone();
                leftover.split = Some(split.clone());
                push_job(
                    stage_name.clone(),
                    JobPlan {
                        name: format!(
                            "{}-{}-{}-leftover",
                            DEFAULT_STAGE_NAME, index.name, SPLIT_SUFFIX
                        ),
                        index: leftover,
                        date_from: None,
                        date_to: None,
                        leftover: true,
                        split_doc_count: None,
                    },
                );
                for (i, range) in date_ranges.into_iter().enumerate() {
                    push_job(
                        stage_name.clone(),
                        JobPlan {
                            name: format!(
                                "{}-{}-{}-{}",
                                DEFAULT_STAGE_NAME,
                                index.name,
                                SPLIT_SUFFIX,
                                i + 1
                            ),
                            index: index.clone(),
                            date_from: range.from,
                            date_to: range.to,
                            leftover: false,
                            split_doc_count: range.doc_count,
                        },
                    );
                }
                continue;
            }
            if matches!(run_mode, RunMode::Restore) {
                let stage_name = format!("{}-{}", DEFAULT_STAGE_NAME, index.name);
                let date_ranges =
                    generate_date_intervals_from_backup(state, src_endpoint, index, split).await?;
                let mut leftover = index.clone();
                leftover.split = Some(split.clone());
                push_job(
                    stage_name.clone(),
                    JobPlan {
                        name: format!(
                            "{}-{}-{}-leftover",
                            DEFAULT_STAGE_NAME, index.name, SPLIT_SUFFIX
                        ),
                        index: leftover,
                        date_from: None,
                        date_to: None,
                        leftover: true,
                        split_doc_count: None,
                    },
                );
                for (i, range) in date_ranges.into_iter().enumerate() {
                    push_job(
                        stage_name.clone(),
                        JobPlan {
                            name: format!(
                                "{}-{}-{}-{}",
                                DEFAULT_STAGE_NAME,
                                index.name,
                                SPLIT_SUFFIX,
                                i + 1
                            ),
                            index: index.clone(),
                            date_from: range.from,
                            date_to: range.to,
                            leftover: false,
                            split_doc_count: range.doc_count,
                        },
                    );
                }
                continue;
            }
            let stage_name = format!("{}-{}", DEFAULT_STAGE_NAME, index.name);
            push_job(
                stage_name,
                JobPlan {
                    name: format!("{}-{}", DEFAULT_STAGE_NAME, index.name),
                    index: index.clone(),
                    date_from: None,
                    date_to: None,
                    leftover: false,
                    split_doc_count: None,
                },
            );
            continue;
        }
        let stage_name = DEFAULT_STAGE_NAME.to_string();
        push_job(
            stage_name,
            JobPlan {
                name: format!("{}-{}", DEFAULT_STAGE_NAME, index.name),
                index: index.clone(),
                date_from: None,
                date_to: None,
                leftover: false,
                split_doc_count: None,
            },
        );
    }
    Ok(stages)
}

async fn generate_date_intervals(
    state: &AppState,
    src_endpoint: &EndpointConfig,
    index: &InputIndex,
    split: &SplitConfig,
) -> Result<Vec<SplitRange>, String> {
    let percents = generate_ranges(split.number_of_parts);
    let payload = json!({
        "size": 0,
        "aggs": {
            "quartiles": {
                "percentiles": { "field": split.field_name, "percents": percents }
            }
        }
    });

    let index_name = build_index_name(
        &src_endpoint.prefix,
        &index.name,
        state.from_suffix.as_deref(),
    );
    let url = format!("{}/{}/_search", src_endpoint.url, index_name);
    let mut request = state.client.get(&url).json(&payload);
    if let Some(auth) = &src_endpoint.auth {
        request = request.basic_auth(auth.username.clone(), auth.password.clone());
    }
    let response = request
        .timeout(Duration::from_secs(120))
        .send()
        .await
        .map_err(|e| format!("percentile request failed: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!(
            "percentile request failed: {} for index {} at {}",
            status, index_name, url
        ));
    }
    let value: serde_json::Value = response
        .json::<serde_json::Value>()
        .await
        .map_err(|e| format!("percentile parse failed: {e}"))?;
    let values = value
        .get("aggregations")
        .and_then(|v| v.get("quartiles"))
        .and_then(|v| v.get("values"))
        .and_then(|v| v.as_object())
        .ok_or_else(|| "percentile response missing values".to_string())?;

    let mut percentile_values: Vec<(f64, Option<String>)> = Vec::new();
    for (key, val) in values {
        if let Some(num) = key.strip_suffix("_as_string") {
            let parsed: f64 = num.parse().unwrap_or(0.0);
            let value = val.as_str().map(|s| s.to_string());
            percentile_values.push((parsed, value));
        }
    }
    percentile_values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let mut ordered_values = percentile_values
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>();

    ordered_values.insert(0, None);
    ordered_values.push(None);

    let mut ranges = Vec::new();
    for window in ordered_values.windows(2) {
        ranges.push(SplitRange {
            from: window[0].clone(),
            to: window[1].clone(),
            doc_count: None,
        });
    }

    if ranges.len() < split.number_of_parts as usize {
        return Err("percentile ranges shorter than expected".to_string());
    }

    let mut ranges: Vec<SplitRange> = ranges
        .into_iter()
        .take(split.number_of_parts as usize)
        .collect();

    let range_specs = ranges
        .iter()
        .map(|range| {
            let mut obj = serde_json::Map::new();
            if let Some(from) = &range.from {
                obj.insert("from".to_string(), json!(from));
            }
            if let Some(to) = &range.to {
                obj.insert("to".to_string(), json!(to));
            }
            json!(obj)
        })
        .collect::<Vec<_>>();
    let payload = json!({
        "size": 0,
        "aggs": {
            "ranges": {
                "range": { "field": split.field_name, "ranges": range_specs }
            }
        }
    });
    let mut request = state.client.get(&url).json(&payload);
    if let Some(auth) = &src_endpoint.auth {
        request = request.basic_auth(auth.username.clone(), auth.password.clone());
    }
    match request.timeout(Duration::from_secs(120)).send().await {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(value) = response.json::<serde_json::Value>().await {
                    if let Some(buckets) = value
                        .get("aggregations")
                        .and_then(|v| v.get("ranges"))
                        .and_then(|v| v.get("buckets"))
                        .and_then(|v| v.as_array())
                    {
                        for (idx, bucket) in buckets.iter().enumerate() {
                            if let Some(count) = bucket.get("doc_count").and_then(|v| v.as_u64())
                            {
                                if let Some(range) = ranges.get_mut(idx) {
                                    range.doc_count = Some(count);
                                }
                            }
                        }
                    }
                }
            } else {
                warn!(
                    "split count request failed: {} for index {} at {}",
                    response.status(),
                    index_name,
                    url
                );
            }
        }
        Err(err) => {
            warn!(
                "split count request failed: {} for index {} at {}",
                err, index_name, url
            );
        }
    }

    Ok(ranges)
}

async fn generate_date_intervals_from_backup(
    _state: &AppState,
    src_endpoint: &EndpointConfig,
    index: &InputIndex,
    split: &SplitConfig,
) -> Result<Vec<SplitRange>, String> {
    let backup_dir = src_endpoint
        .backup_dir
        .as_ref()
        .ok_or_else(|| "backup_dir is missing for restore split".to_string())?;
    let backup_dir = PathBuf::from(backup_dir);
    let index_dir = backup::resolve_index_dir(&backup_dir, &index.name)
        .map_err(|err| format!("Failed to resolve backup index directory: {err}"))?;
    let metadata: backup::BackupMetadata =
        backup::read_json_file(&index_dir.join("metadata.json"))
            .map_err(|e| format!("Failed to read metadata.json: {e}"))?;

    let field = metadata
        .quantile_field
        .clone()
        .ok_or_else(|| format!("backup metadata missing quantile_field for {}", index.name))?;
    if field != split.field_name {
        warn!(
            "backup quantile field {} does not match split field {}; using {}",
            field, split.field_name, field
        );
    }
    let centroids = metadata
        .quantile_digest
        .clone()
        .ok_or_else(|| format!("backup metadata missing quantile_digest for {}", index.name))?;
    if centroids.is_empty() {
        return Err(format!(
            "backup metadata quantile_digest empty for {}",
            index.name
        ));
    }

    let percents = generate_ranges(split.number_of_parts);
    let mut percentile_values: Vec<Option<String>> = Vec::new();
    for percent in percents {
        let value = quantile_from_centroids(&centroids, percent as f64)
            .ok_or_else(|| format!("Failed to compute quantile {} for {}", percent, index.name))?;
        let value = quantile_value_to_string(value).ok_or_else(|| {
            format!(
                "Failed to format quantile {} for {}",
                percent, index.name
            )
        })?;
        percentile_values.push(Some(value));
    }

    let mut ordered_values = percentile_values;
    ordered_values.insert(0, None);
    ordered_values.push(None);

    let mut ranges = Vec::new();
    for window in ordered_values.windows(2) {
        ranges.push(SplitRange {
            from: window[0].clone(),
            to: window[1].clone(),
            doc_count: None,
        });
    }

    if ranges.len() < split.number_of_parts as usize {
        return Err("percentile ranges shorter than expected".to_string());
    }

    Ok(ranges
        .into_iter()
        .take(split.number_of_parts as usize)
        .collect())
}

fn quantile_from_centroids(centroids: &[backup::QuantileCentroid], percent: f64) -> Option<f64> {
    if centroids.is_empty() {
        return None;
    }
    let mut ordered: Vec<backup::QuantileCentroid> = centroids.to_vec();
    ordered.sort_by(|a, b| {
        a.mean
            .partial_cmp(&b.mean)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let total: f64 = ordered.iter().map(|c| c.count).sum();
    if total <= 0.0 {
        return None;
    }
    let target = total * (percent / 100.0);
    let mut cumulative = 0.0;
    for centroid in ordered.iter() {
        cumulative += centroid.count;
        if cumulative >= target {
            return Some(centroid.mean);
        }
    }
    ordered.last().map(|c| c.mean)
}

fn quantile_value_to_string(value: f64) -> Option<String> {
    if !value.is_finite() {
        return None;
    }
    let millis = value.round() as i64;
    Some(
        chrono::Utc
            .timestamp_millis_opt(millis)
            .single()
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| millis.to_string()),
    )
}

fn generate_ranges(parts: u64) -> Vec<u64> {
    let limit = 100;
    let step = limit / parts.max(1);
    (1..parts).map(|i| step * i).collect()
}

fn build_index_name(prefix: &str, name: &str, suffix: Option<&str>) -> String {
    let mut result = format!("{}{}", prefix, name);
    if let Some(suffix) = suffix {
        if !suffix.is_empty() {
            result = format!("{}-{}", result, suffix);
        }
    }
    result
}

fn build_alias_name(prefix: &str, name: &str, suffix: Option<&str>) -> String {
    let mut result = format!("{}{}", prefix, name);
    if let Some(suffix) = suffix {
        if !suffix.is_empty() {
            result = format!("{}-{}", result, suffix);
        }
    }
    result
}

fn build_name_of_copy(prefix: &str, name: &str, timestamp: &str, suffix: Option<&str>) -> String {
    let mut result = format!("{}{}-{}", prefix, name, timestamp);
    if let Some(suffix) = suffix {
        if !suffix.is_empty() {
            result = format!("{}-{}", result, suffix);
        }
    }
    result
}

fn escape_shell_value(value: &str) -> String {
    value.replace('$', "$$")
}

fn build_output_config(
    state: &AppState,
    job: &JobPlan,
    src_endpoint: &EndpointConfig,
    dst_endpoint: &EndpointConfig,
    copy_suffix: Option<&str>,
    alias_suffix: Option<&str>,
    run_mode: RunMode,
) -> Result<OutputConfig, String> {
    let timestamp = state
        .timestamp
        .clone()
        .unwrap_or_else(|| chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string());
    let number_of_shards = job
        .index
        .number_of_shards
        .unwrap_or(1)
        .max(1);
    let number_of_replicas = job
        .index
        .number_of_replicas
        .unwrap_or(dst_endpoint.number_of_replicas);

    let src_prefix = if job.index.use_src_prefix {
        src_endpoint.prefix.as_str()
    } else {
        ""
    };
    let dst_prefix = if job.index.use_dst_prefix {
        dst_endpoint.prefix.as_str()
    } else {
        ""
    };
    let from_suffix = if job.index.use_from_suffix {
        state.from_suffix.as_deref()
    } else {
        None
    };
    let index_name = build_index_name(src_prefix, &job.index.name, from_suffix);
    let dest_base_name = job
        .index
        .dest_name
        .as_deref()
        .unwrap_or(&job.index.name);
    let name_of_copy = if job.index.use_dest_name_as_is {
        dest_base_name.to_string()
    } else {
        build_name_of_copy(dst_prefix, dest_base_name, &timestamp, copy_suffix)
    };
    let alias_base_name = job
        .index
        .alias_name
        .as_deref()
        .unwrap_or(dest_base_name);
    let alias_name = if job.index.use_alias_name_as_is {
        alias_base_name.to_string()
    } else {
        build_alias_name(dst_prefix, alias_base_name, alias_suffix)
    };

    let mut custom_mapping = job
        .index
        .custom
        .as_ref()
        .and_then(|c| c.mapping.clone());
    if custom_mapping.as_deref().unwrap_or("").is_empty() {
        custom_mapping = None;
    }

    let mut custom_query = None;
    if let Some(split) = &job.index.split {
        if job.leftover {
            custom_query = Some(format!(
                "{{ \"bool\": {{ \"must_not\": [ {{ \"exists\": {{ \"field\": \"{}\" }} }} ] }} }}",
                split.field_name
            ));
        } else if job.date_from.is_some() || job.date_to.is_some() {
            let mut range = String::new();
            if let Some(from) = &job.date_from {
                range.push_str(&format!("\"gte\": \"{}\"", from));
            }
            if let Some(to) = &job.date_to {
                if !range.is_empty() {
                    range.push_str(", ");
                }
                range.push_str(&format!("\"lt\": \"{}\"", to));
            }
            custom_query = Some(format!(
                "{{ \"bool\": {{ \"must\": [{{ \"match_all\": {{}} }}], \"filter\": [ {{ \"range\": {{ \"{}\": {{ {} }} }} }} ] }} }}",
                split.field_name,
                range
            ));
        }
    }

    let has_custom = custom_mapping.is_some() || custom_query.is_some();
    let copy_mapping = if custom_mapping.is_some() {
        false
    } else {
        job.index.copy_mapping
    };

    let backup_quantile_field = if run_mode == RunMode::Backup {
        job.index
            .split
            .as_ref()
            .map(|split| split.field_name.clone())
    } else {
        None
    };

    let routing_field = job.index.routing_field.clone();
    let pre_create_doc_source = routing_field.as_ref().map(|_| {
        format!(
            "{{ \"es_copy_indices\": {{ \"pre_created\": true, \"job_name\": \"{}\" }} }}",
            job.name
        )
    });

    let endpoints = vec![
        OutputEndpoint {
            name: "es-source".to_string(),
            url: if src_endpoint.backup_dir.is_some() {
                None
            } else {
                Some(src_endpoint.url.clone())
            },
            root_certificates: state.ca_path().map(|p| p.to_string_lossy().to_string()),
            insecure: if state.insecure { Some(true) } else { None },
            basic_auth: src_endpoint.auth.as_ref().map(|auth| OutputBasicAuth {
                username: escape_shell_value(&auth.username),
                password: auth.password.as_ref().map(|value| escape_shell_value(value)),
            }),
            backup_dir: src_endpoint.backup_dir.clone(),
        },
        OutputEndpoint {
            name: "es-destination".to_string(),
            url: if dst_endpoint.backup_dir.is_some() {
                None
            } else {
                Some(dst_endpoint.url.clone())
            },
            root_certificates: state.ca_path().map(|p| p.to_string_lossy().to_string()),
            insecure: if state.insecure { Some(true) } else { None },
            basic_auth: dst_endpoint.auth.as_ref().map(|auth| OutputBasicAuth {
                username: escape_shell_value(&auth.username),
                password: auth.password.as_ref().map(|value| escape_shell_value(value)),
            }),
            backup_dir: dst_endpoint.backup_dir.clone(),
        },
    ];

    let alias_remove_if_exists = job
        .index
        .alias_remove_if_exists
        .unwrap_or(state.alias_remove_if_exists);
    let index = OutputIndex {
        buffer_size: job.index.buffer_size,
        copy_content: job.index.copy_content,
        copy_mapping,
        delete_if_exists: job.index.delete_if_exists,
        from: "es-source".to_string(),
        keep_alive: src_endpoint.keep_alive.clone(),
        name: index_name,
        name_of_copy,
        number_of_replicas,
        number_of_shards,
        to: "es-destination".to_string(),
        backup_quantile_field,
        routing_field,
        pre_create_doc_source,
        alias: if job.index.alias_enabled {
            Some(OutputAlias {
                name: alias_name,
                remove_if_exists: alias_remove_if_exists,
            })
        } else {
            None
        },
        custom: if has_custom {
            Some(OutputCustom {
                query: custom_query,
                mapping: custom_mapping,
            })
        } else {
            None
        },
    };

    let audit = if state.audit {
        Some(OutputAudit {
            enabled: true,
            file_name: format!("audit/{}.log", job.name),
        })
    } else {
        None
    };

    Ok(OutputConfig {
        endpoints,
        indices: vec![index],
        audit,
    })
}

impl AppState {
    fn ca_path(&self) -> Option<PathBuf> {
        self.ca_path.clone()
    }
}

fn build_reqwest_client(
    ca_path: Option<&PathBuf>,
    insecure: bool,
) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder();
    if insecure {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(path) = ca_path {
        let certs = load_certificates(path)?;
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }
    builder.build().map_err(|e| e.to_string())
}

fn load_certificates(path: &PathBuf) -> Result<Vec<Certificate>, String> {
    let mut certs = Vec::new();
    let entries = std::fs::read_dir(path).map_err(|e| e.to_string())?;
    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let file_path = entry.path();
        if !file_path.is_file() {
            continue;
        }
        if let Ok(content) = std::fs::read(&file_path) {
            if let Ok(cert) = Certificate::from_pem(&content) {
                certs.push(cert);
            }
        }
    }
    Ok(certs)
}

fn normalize_base_path(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return "/".to_string();
    }
    let mut result = trimmed.to_string();
    if !result.starts_with('/') {
        result.insert(0, '/');
    }
    while result.ends_with('/') {
        result.pop();
    }
    result
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "password"
            | "passwd"
            | "passphrase"
            | "secret"
            | "token"
            | "api_key"
            | "apikey"
            | "access_key"
            | "secret_key"
            | "authorization"
    )
}

fn redact_env_value(key: &str, value: &str, enabled: bool) -> String {
    if enabled && is_sensitive_key(key) {
        REDACTED_VALUE.to_string()
    } else {
        value.to_string()
    }
}

fn is_word_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn find_key_case_insensitive(haystack: &[u8], key: &[u8], start: usize) -> Option<usize> {
    if key.is_empty() || haystack.len() < key.len() || start >= haystack.len() {
        return None;
    }
    let key_len = key.len();
    for i in start..=haystack.len() - key_len {
        let mut matched = true;
        for j in 0..key_len {
            let h = haystack[i + j].to_ascii_lowercase();
            if h != key[j] {
                matched = false;
                break;
            }
        }
        if matched {
            return Some(i);
        }
    }
    None
}

fn redact_line_for_key(line: &str, key: &str) -> String {
    let mut output = line.to_string();
    let key_bytes = key.as_bytes();
    let mut search_start = 0;

    loop {
        let bytes = output.as_bytes();
        let Some(match_idx) = find_key_case_insensitive(bytes, key_bytes, search_start) else {
            break;
        };

        let before = if match_idx == 0 { None } else { bytes.get(match_idx - 1) };
        let after = bytes.get(match_idx + key_bytes.len());
        let before_ok = before.map_or(true, |b| !is_word_char(*b));
        let after_ok = after.map_or(true, |b| !is_word_char(*b));
        if !before_ok || !after_ok {
            search_start = match_idx + key_bytes.len();
            continue;
        }

        let mut eq_idx = None;
        for i in match_idx + key_bytes.len()..bytes.len() {
            if bytes[i] == b'=' {
                eq_idx = Some(i);
                break;
            }
        }
        let Some(eq_idx) = eq_idx else {
            break;
        };

        let mut value_start = eq_idx + 1;
        while value_start < bytes.len() && bytes[value_start].is_ascii_whitespace() {
            value_start += 1;
        }
        if value_start >= bytes.len() {
            break;
        }

        let (value_end, wrap_with_quotes, quote_char) = if bytes[value_start] == b'"' || bytes[value_start] == b'\'' {
            let quote = bytes[value_start];
            let mut end = value_start + 1;
            while end < bytes.len() {
                if bytes[end] == quote && bytes.get(end.wrapping_sub(1)) != Some(&b'\\') {
                    break;
                }
                end += 1;
            }
            if end >= bytes.len() {
                break;
            }
            (end, true, quote as char)
        } else {
            let mut end = value_start;
            while end < bytes.len() {
                let b = bytes[end];
                if b.is_ascii_whitespace() || b == b',' || b == b'}' || b == b']' {
                    break;
                }
                end += 1;
            }
            (end, false, '"')
        };

        let mut replaced = String::with_capacity(output.len() + REDACTED_VALUE.len());
        replaced.push_str(&output[..value_start]);
        if wrap_with_quotes {
            replaced.push(quote_char);
            replaced.push_str(REDACTED_VALUE);
            replaced.push(quote_char);
            replaced.push_str(&output[value_end + 1..]);
        } else {
            replaced.push_str(REDACTED_VALUE);
            replaced.push_str(&output[value_end..]);
        }
        output = replaced;
        search_start = value_start + REDACTED_VALUE.len();
    }

    output
}

fn redact_config_line(line: &str, enabled: bool) -> String {
    if !enabled {
        return line.to_string();
    }
    let mut redacted = line.to_string();
    for key in [
        "password",
        "passwd",
        "passphrase",
        "secret",
        "token",
        "api_key",
        "apikey",
        "access_key",
        "secret_key",
        "authorization",
    ] {
        redacted = redact_line_for_key(&redacted, key);
    }
    redacted
}

fn with_base(state: &AppState, path: &str) -> String {
    if state.base_path == "/" {
        path.to_string()
    } else {
        format!("{}{}", state.base_path, path)
    }
}
