use askama::Template;
use axum::extract::{Form, Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Sse};
use axum::Json;
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use reqwest::Certificate;
use serde::{Deserialize, Serialize};
use serde_json::json;
use libc::{kill, SIGTERM};
use std::collections::{HashMap, VecDeque};
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

#[derive(Parser, Debug)]
#[command(name = "es-copy-indices-server")]
struct ServerArgs {
    #[arg(long, value_name = "PATH")]
    main_config: PathBuf,
    #[arg(long = "env-templates", alias = "templates", value_name = "DIR")]
    templates_dir: PathBuf,
    #[arg(long, value_name = "PATH")]
    ca_path: Option<PathBuf>,
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
    metrics: Arc<RwLock<MetricsState>>,
    metrics_tx: broadcast::Sender<MetricsSample>,
    timestamp: Option<String>,
    from_suffix: Option<String>,
    copy_suffix: Option<String>,
    alias_suffix: Option<String>,
    alias_remove_if_exists: bool,
    audit: bool,
    runs: Arc<RwLock<RunStore>>,
    quarantined_runs: Arc<RwLock<Vec<String>>>,
    job_queue: Arc<Mutex<VecDeque<QueuedJob>>>,
    max_concurrent_jobs: Arc<RwLock<Option<usize>>>,
    queue_notify: Arc<Notify>,
}

#[derive(Default)]
struct RunStore {
    order: Vec<String>,
    runs: HashMap<String, RunState>,
}

#[derive(Clone, Debug)]
struct QueuedJob {
    run_id: String,
    job_id: String,
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

#[derive(Clone, Debug)]
struct RunState {
    id: String,
    created_at: String,
    template: TemplateSnapshot,
    src_endpoint: EndpointSnapshot,
    dst_endpoint: EndpointSnapshot,
    dry_run: bool,
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MainConfigFile {
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
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct EndpointSnapshot {
    id: String,
    name: String,
    url: String,
    prefix: String,
    number_of_replicas: u64,
    keep_alive: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct TemplateSnapshot {
    id: String,
    name: String,
    path: String,
    #[serde(default)]
    number_of_replicas: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
struct TemplateGlobal {
    name: Option<String>,
    number_of_replicas: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
struct TemplateFile {
    global: Option<TemplateGlobal>,
    indices: Vec<InputIndex>,
}

#[derive(Debug, Deserialize, Clone)]
struct InputIndex {
    name: String,
    buffer_size: u64,
    routing_field: Option<String>,
    number_of_shards: Option<u64>,
    number_of_replicas: Option<u64>,
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
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    root_certificates: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    basic_auth: Option<OutputBasicAuth>,
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
    refresh_seconds: u64,
    base_path: String,
    endpoints: Vec<EndpointView>,
    templates: Vec<TemplateView>,
    endpoints_json: String,
    templates_json: String,
    max_concurrent_jobs: Option<usize>,
    running_total: usize,
    queued_total: usize,
}

#[derive(Template)]
#[template(path = "server/run.html")]
struct RunTemplate {
    run: RunView,
    base_path: String,
}

#[derive(Template)]
#[template(path = "server/job.html")]
struct JobTemplate {
    run_id: String,
    job: JobView,
    base_path: String,
}

#[derive(Template)]
#[template(path = "server/status.html")]
struct StatusTemplate {
    base_path: String,
    refresh_seconds: u64,
    summary: MetricsSummary,
}

#[derive(Template)]
#[template(path = "server/config.html")]
struct ConfigTemplate {
    base_path: String,
    endpoints: Vec<EndpointView>,
    templates: Vec<TemplateView>,
}

#[derive(Clone, Serialize)]
struct EndpointView {
    id: String,
    name: String,
    url: String,
    prefix: String,
    keep_alive: String,
}

#[derive(Clone, Serialize)]
struct TemplateView {
    id: String,
    name: String,
    file_name: String,
    indices_count: usize,
    number_of_replicas: Option<u64>,
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
    template_name: String,
    dry_run: bool,
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
    max_concurrent_jobs: Option<usize>,
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
    if let Some(path) = &args.ca_path {
        if !path.is_dir() {
            warn!("--ca-path {:?} is not a directory; HTTPS may fail.", path);
        }
    }
    if args.insecure && args.ca_path.is_none() {
        warn!("--insecure affects percentile queries only; es-copy-indices still needs CA files for HTTPS.");
    }
    let endpoints = load_main_config(&args.main_config);
    let templates = load_templates(&args.templates_dir);
    if endpoints.is_empty() {
        panic!("No endpoints found in {:?}", args.main_config);
    }
    if templates.is_empty() {
        panic!("No templates found in {:?}", args.templates_dir);
    }

    let client = build_reqwest_client(args.ca_path.as_ref(), args.insecure)
        .unwrap_or_else(|e| panic!("Failed to build HTTP client: {e}"));

    let runs_dir = args.runs_dir.clone();
    if let Err(e) = tokio::fs::create_dir_all(&runs_dir).await {
        panic!("Failed to create runs dir {:?}: {:?}", runs_dir, e);
    }

    let metrics = Arc::new(RwLock::new(MetricsState::default()));
    let (metrics_tx, _) = broadcast::channel(200);
    let state = Arc::new(AppState {
        endpoints,
        templates,
        client,
        runs_dir,
        es_copy_path: args.es_copy_path,
        refresh_seconds: args.refresh_seconds,
        base_path: normalize_base_path(&args.base_path),
        ca_path: args.ca_path.clone(),
        metrics: Arc::clone(&metrics),
        metrics_tx,
        timestamp: args.timestamp,
        from_suffix: args.from_suffix,
        copy_suffix: args.copy_suffix,
        alias_suffix: args.alias_suffix,
        alias_remove_if_exists: args.alias_remove_if_exists,
        audit: args.audit,
        runs: Arc::new(RwLock::new(RunStore::default())),
        quarantined_runs: Arc::new(RwLock::new(Vec::new())),
        job_queue: Arc::new(Mutex::new(VecDeque::new())),
        max_concurrent_jobs: Arc::new(RwLock::new(if args.max_concurrent_jobs == 0 {
            None
        } else {
            Some(args.max_concurrent_jobs)
        })),
        queue_notify: Arc::new(Notify::new()),
    });

    load_runs(&state).await;
    start_queue_worker(Arc::clone(&state));
    start_metrics_sampler(
        Arc::clone(&state.metrics),
        state.metrics_tx.clone(),
        Arc::clone(&state.runs),
        Arc::clone(&state.max_concurrent_jobs),
        args.metrics_seconds,
    );

    let routes = Router::new()
        .route("/", get(index))
        .route("/config", get(config_view))
        .route("/status", get(status_view))
        .route("/status/snapshot", get(status_snapshot))
        .route("/status/stream", get(status_stream))
        .route("/runs", post(create_run))
        .route("/runs/snapshot", get(runs_snapshot))
        .route("/runs/stream", get(runs_stream))
        .route(
            "/settings/max-concurrent-jobs",
            post(update_max_concurrent_jobs),
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
    let max_concurrent_jobs = state.max_concurrent_jobs.read().await.clone();
    let template = IndexTemplate {
        runs,
        refresh_seconds: 0,
        base_path: state.base_path.clone(),
        endpoints: build_endpoint_views(&state),
        templates: build_template_views(&state),
        endpoints_json: endpoints_json(&state),
        templates_json: templates_json(&state),
        max_concurrent_jobs,
        running_total,
        queued_total,
    };
    Html(render_template(&template)).into_response()
}

async fn config_view(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let template = ConfigTemplate {
        base_path: state.base_path.clone(),
        endpoints: build_endpoint_views(&state),
        templates: build_template_views(&state),
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
}

#[derive(Deserialize)]
struct MaxConcurrentForm {
    delta: Option<i32>,
    value: Option<usize>,
}

async fn create_run(
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateRunForm>,
) -> impl IntoResponse {
    let src_endpoint = match endpoint_by_id(&state, &form.src_endpoint_id) {
        Some(endpoint) => endpoint.clone(),
        None => return (StatusCode::BAD_REQUEST, "Unknown source endpoint").into_response(),
    };
    let dst_endpoint = match endpoint_by_id(&state, &form.dst_endpoint_id) {
        Some(endpoint) => endpoint.clone(),
        None => return (StatusCode::BAD_REQUEST, "Unknown destination endpoint").into_response(),
    };
    let template = match template_by_id(&state, &form.template_id) {
        Some(template) => template.clone(),
        None => return (StatusCode::BAD_REQUEST, "Unknown template").into_response(),
    };
    let dry_run = form.dry_run.is_some();

    match create_run_state(&state, &template, &src_endpoint, &dst_endpoint, dry_run).await {
        Ok(run_id) => Redirect::to(&with_base(&state, &format!("/runs/{}", run_id)))
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create run: {}", err),
        )
            .into_response(),
    }
}

async fn update_max_concurrent_jobs(
    State(state): State<Arc<AppState>>,
    Form(form): Form<MaxConcurrentForm>,
) -> impl IntoResponse {
    let mut guard = state.max_concurrent_jobs.write().await;
    let current = guard.unwrap_or(0);
    let mut next = if let Some(value) = form.value {
        value as i32
    } else {
        let delta = form.delta.unwrap_or(0);
        current as i32 + delta
    };
    if next < 0 {
        next = 0;
    }
    if next == 0 {
        *guard = None;
    } else {
        *guard = Some(next as usize);
    }
    drop(guard);
    state.queue_notify.notify_one();
    Json(json!({
        "max_concurrent_jobs": state.max_concurrent_jobs.read().await.clone()
    }))
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
        refresh_seconds: 0,
        summary,
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
            let payload = serde_json::to_string(&runs).unwrap_or_else(|_| "[]".to_string());
            Ok::<axum::response::sse::Event, Infallible>(
                axum::response::sse::Event::default().data(payload),
            )
        }
    });
    Sse::new(stream).into_response()
}

async fn runs_snapshot(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let runs = build_run_summaries(&state).await;
    Json(runs).into_response()
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
        (StatusCode::OK, format!("Stop requested for {stopped} job(s).")).into_response()
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
        (StatusCode::OK, format!("Stop requested for {stopped} job(s).")).into_response()
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
    let max_concurrent_jobs = state.max_concurrent_jobs.read().await.clone();
    let running_count = if max_concurrent_jobs.is_some() {
        let runs = state.runs.read().await;
        count_running_jobs(&runs)
    } else {
        0
    };

    let mut queued = false;
    let (config_path, stdout_path, stderr_path, snapshot, dry_run) = {
        let mut runs = state.runs.write().await;
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
        (config_path, stdout_path, stderr_path, snapshot, dry_run)
    };
    let runs_dir = state.runs_dir.clone();
    tokio::spawn(async move {
        if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
            warn!("Failed to persist run: {}", err);
        }
    });
    if queued {
        enqueue_job(&state, run_id.clone(), job_id.clone()).await;
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

fn count_running_jobs(runs: &RunStore) -> usize {
    runs.runs
        .values()
        .flat_map(|run| run.jobs.values())
        .filter(|job| matches!(job.status, JobStatus::Running))
        .count()
}

async fn enqueue_job(state: &Arc<AppState>, run_id: String, job_id: String) {
    let mut queue = state.job_queue.lock().await;
    if queue.iter().any(|job| job.run_id == run_id && job.job_id == job_id) {
        return;
    }
    queue.push_back(QueuedJob { run_id, job_id });
    state.queue_notify.notify_one();
}

async fn remove_from_queue(state: &Arc<AppState>, run_id: &str, job_id: &str) {
    let mut queue = state.job_queue.lock().await;
    queue.retain(|job| !(job.run_id == run_id && job.job_id == job_id));
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
            let max = state
                .max_concurrent_jobs
                .read()
                .await
                .unwrap_or(usize::MAX);
            let running = {
                let runs = state.runs.read().await;
                count_running_jobs(&runs)
            };
            if running >= max {
                break;
            }
            let next = {
                let mut queue = state.job_queue.lock().await;
                queue.pop_front()
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
    let (pid, stderr_path, snapshot) = {
        let mut runs = state.runs.write().await;
        let run = runs
            .runs
            .get_mut(run_id)
            .ok_or_else(|| "run not found".to_string())?;
        let (pid, stderr_path) = {
            let job = run
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| "job not found".to_string())?;
            if !matches!(job.status, JobStatus::Running) {
                return Ok("Job not running.".to_string());
            }
            job.status = JobStatus::Stopped;
            job.exit_code = Some(-15);
            job.finished_at = Some(now_string());
            job.completed_line = false;
            (job.pid, job.stderr_path.clone())
        };
        let snapshot = run_snapshot(run);
        (pid, stderr_path, snapshot)
    };

    let runs_dir = state.runs_dir.clone();
    tokio::spawn(async move {
        if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
            warn!("Failed to persist run: {}", err);
        }
    });

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
        "Stop requested (SIGTERM).",
        &stderr_path,
    )
    .await;

    state.queue_notify.notify_one();

    Ok("Stop requested.".to_string())
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
            let _ = append_log_line(
                &state,
                &run_id,
                &job_id,
                LogStream::Stdout,
                line,
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
            max_concurrent_jobs: sample.max_concurrent_jobs,
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
    max_concurrent_jobs: Arc<RwLock<Option<usize>>>,
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

            let max_concurrent_jobs = max_concurrent_jobs.read().await.clone();
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
                max_concurrent_jobs,
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
                template_name: run.template.name.clone().if_empty_then("unnamed"),
                dry_run: run.dry_run,
            });
        }
    }
    summaries
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
                let (progress_label, progress_width) = match job.status {
                    JobStatus::Running => {
                        let label = job.progress_percent.map(|value| {
                            let base = format!("{:.2} %", value);
                            if let Some(eta) = estimate_eta_label(&job.started_at, job.progress_percent) {
                                format!("{base} · {eta}")
                            } else {
                                base
                            }
                        });
                        let width = job
                            .progress_percent
                            .map(|value| value.max(0.0).min(100.0).round() as u8);
                        (label, width)
                    }
                    JobStatus::Succeeded => {
                        let label = job.progress_percent.map(|value| format!("{:.2} %", value));
                        let width = job
                            .progress_percent
                            .map(|value| value.max(0.0).min(100.0).round() as u8);
                        (label, width)
                    }
                    _ => (None, None),
                };
                jobs.push(JobSummary {
                    id: job.id.clone(),
                    name: job.name.clone(),
                    status: job.status.clone(),
                    progress_label,
                    progress_width,
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

async fn create_run_state(
    state: &Arc<AppState>,
    template: &TemplateConfig,
    src_endpoint: &EndpointConfig,
    dst_endpoint: &EndpointConfig,
    dry_run: bool,
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

    let stages = plan_stages(template, state, src_endpoint).await?;

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

            let output_config = build_output_config(state, &job, src_endpoint, dst_endpoint)?;
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
        template: template_snapshot(template),
        src_endpoint: endpoint_snapshot(src_endpoint),
        dst_endpoint: endpoint_snapshot(dst_endpoint),
        dry_run,
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
                    if matches!(job.status, JobStatus::Running) {
                        job.status = JobStatus::Failed;
                        job.exit_code = Some(-1);
                        job.finished_at = Some(now_string());
                        job.completed_line = false;
                        run_changed = true;
                    }
                    if matches!(job.status, JobStatus::Queued) {
                        queued.push(QueuedJob {
                            run_id: run_id.clone(),
                            job_id: job.id.clone(),
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
        let mut queue = state.job_queue.lock().await;
        for item in queued {
            if !queue
                .iter()
                .any(|job| job.run_id == item.run_id && job.job_id == item.job_id)
            {
                queue.push_back(item);
            }
        }
    }
    let queue = state.job_queue.lock().await;
    if !queue.is_empty() {
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

fn load_main_config(path: &PathBuf) -> Vec<EndpointConfig> {
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read main config {:?}: {e}", path));
    let config: MainConfigFile =
        toml::from_str(&content).unwrap_or_else(|e| panic!("Invalid main config {:?}: {e}", path));
    let mut used = HashMap::new();
    config
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
            }
        })
        .collect()
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

fn endpoint_snapshot(endpoint: &EndpointConfig) -> EndpointSnapshot {
    EndpointSnapshot {
        id: endpoint.id.clone(),
        name: endpoint.name.clone(),
        url: endpoint.url.clone(),
        prefix: endpoint.prefix.clone(),
        number_of_replicas: endpoint.number_of_replicas,
        keep_alive: endpoint.keep_alive.clone(),
    }
}

fn template_snapshot(template: &TemplateConfig) -> TemplateSnapshot {
    TemplateSnapshot {
        id: template.id.clone(),
        name: template.name.clone(),
        path: template.path.to_string_lossy().to_string(),
        number_of_replicas: template.number_of_replicas,
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

async fn plan_stages(
    template: &TemplateConfig,
    state: &AppState,
    src_endpoint: &EndpointConfig,
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
        if let Some(split) = &index.split {
            let stage_name = format!("{}-{}", DEFAULT_STAGE_NAME, index.name);
            let date_ranges =
                generate_date_intervals(state, src_endpoint, index, split).await?;
            let mut leftover = index.clone();
            leftover.split = Some(split.clone());
            push_job(
                stage_name.clone(),
                JobPlan {
                name: format!("{}-{}-{}-leftover", DEFAULT_STAGE_NAME, index.name, SPLIT_SUFFIX),
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
                    name: format!("{}-{}-{}-{}", DEFAULT_STAGE_NAME, index.name, SPLIT_SUFFIX, i + 1),
                    index: index.clone(),
                    date_from: range.from,
                    date_to: range.to,
                    leftover: false,
                    split_doc_count: range.doc_count,
                },
                );
            }
        } else {
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

fn build_output_config(
    state: &AppState,
    job: &JobPlan,
    src_endpoint: &EndpointConfig,
    dst_endpoint: &EndpointConfig,
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

    let index_name = build_index_name(
        &src_endpoint.prefix,
        &job.index.name,
        state.from_suffix.as_deref(),
    );
    let name_of_copy = build_name_of_copy(
        &dst_endpoint.prefix,
        &job.index.name,
        &timestamp,
        state.copy_suffix.as_deref(),
    );
    let alias_name = build_alias_name(
        &dst_endpoint.prefix,
        &job.index.name,
        state.alias_suffix.as_deref(),
    );

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
    let copy_mapping = !custom_mapping.is_some();

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
            url: src_endpoint.url.clone(),
            root_certificates: state.ca_path().map(|p| p.to_string_lossy().to_string()),
            basic_auth: src_endpoint.auth.as_ref().map(|auth| OutputBasicAuth {
                username: auth.username.clone(),
                password: auth.password.clone(),
            }),
        },
        OutputEndpoint {
            name: "es-destination".to_string(),
            url: dst_endpoint.url.clone(),
            root_certificates: state.ca_path().map(|p| p.to_string_lossy().to_string()),
            basic_auth: dst_endpoint.auth.as_ref().map(|auth| OutputBasicAuth {
                username: auth.username.clone(),
                password: auth.password.clone(),
            }),
        },
    ];

    let index = OutputIndex {
        buffer_size: job.index.buffer_size,
        copy_content: true,
        copy_mapping,
        delete_if_exists: false,
        from: "es-source".to_string(),
        keep_alive: src_endpoint.keep_alive.clone(),
        name: index_name,
        name_of_copy,
        number_of_replicas,
        number_of_shards,
        to: "es-destination".to_string(),
        routing_field,
        pre_create_doc_source,
        alias: Some(OutputAlias {
            name: alias_name,
            remove_if_exists: state.alias_remove_if_exists,
        }),
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

fn with_base(state: &AppState, path: &str) -> String {
    if state.base_path == "/" {
        path.to_string()
    } else {
        format!("{}{}", state.base_path, path)
    }
}
