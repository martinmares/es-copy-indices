use askama::Template;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Sse};
use axum::Json;
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use reqwest::Certificate;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{broadcast, RwLock};
use std::convert::Infallible;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::{info, warn};

const DEFAULT_STAGE_NAME: &str = "copy";
const SPLIT_SUFFIX: &str = "split";
const MAX_TAIL_LINES: usize = 200;

#[derive(Parser, Debug)]
#[command(name = "es-copy-indices-server")]
struct ServerArgs {
    #[arg(long, value_name = "PATH")]
    src_config: PathBuf,
    #[arg(long, value_name = "PATH")]
    dst_config: Option<PathBuf>,
    #[arg(long, env = "ES_ENDPOINT_SRC")]
    src_url: String,
    #[arg(long, env = "ES_ENDPOINT_DST")]
    dst_url: String,
    #[arg(long, env = "ES_USER_SRC")]
    src_user: Option<String>,
    #[arg(long, env = "ES_PASSWD_SRC")]
    src_pass: Option<String>,
    #[arg(long, env = "ES_USER_DST")]
    dst_user: Option<String>,
    #[arg(long, env = "ES_PASSWD_DST")]
    dst_pass: Option<String>,
    #[arg(long, value_name = "PATH")]
    ca_path: Option<PathBuf>,
    #[arg(long)]
    insecure: bool,
    #[arg(long, default_value = "runs")]
    runs_dir: PathBuf,
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: String,
    #[arg(long, default_value = "es-copy-indices")]
    es_copy_path: PathBuf,
    #[arg(long, default_value = "/")]
    base_path: String,
    #[arg(long, default_value_t = 5)]
    refresh_seconds: u64,
    #[arg(long, default_value_t = 5)]
    metrics_seconds: u64,
    #[arg(long, env = "TIMESTAMP")]
    timestamp: Option<String>,
    #[arg(long, env = "FROM_INDEX_NAME_SUFFIX")]
    from_suffix: Option<String>,
    #[arg(long, env = "INDEX_COPY_SUFFIX")]
    copy_suffix: Option<String>,
    #[arg(long, env = "ALIAS_SUFFIX")]
    alias_suffix: Option<String>,
    #[arg(long, env = "ALIAS_REMOVE_IF_EXISTS")]
    alias_remove_if_exists: bool,
    #[arg(long, env = "AUDIT")]
    audit: bool,
}

#[derive(Clone)]
struct AppState {
    src_env: InputEnvironment,
    dst_env: InputEnvironment,
    src_endpoint: EndpointSettings,
    dst_endpoint: EndpointSettings,
    client: reqwest::Client,
    runs_dir: PathBuf,
    es_copy_path: PathBuf,
    refresh_seconds: u64,
    base_path: String,
    metrics: Arc<RwLock<MetricsState>>,
    metrics_tx: broadcast::Sender<MetricsSample>,
    timestamp: Option<String>,
    from_suffix: Option<String>,
    copy_suffix: Option<String>,
    alias_suffix: Option<String>,
    alias_remove_if_exists: bool,
    audit: bool,
    runs: Arc<RwLock<RunStore>>,
}

#[derive(Default)]
struct RunStore {
    order: Vec<String>,
    runs: HashMap<String, RunState>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RunPersist {
    id: String,
    created_at: String,
    src_config_path: String,
    dst_config_path: String,
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
}

#[derive(Clone, Debug)]
struct RunState {
    id: String,
    created_at: String,
    src_config_path: PathBuf,
    dst_config_path: PathBuf,
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
    Succeeded,
    Failed,
}

impl JobStatus {
    fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Succeeded => "succeeded",
            JobStatus::Failed => "failed",
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
    config_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    stdout_tail: VecDeque<String>,
    stderr_tail: VecDeque<String>,
    stdout_tx: broadcast::Sender<LogEvent>,
    stderr_tx: broadcast::Sender<LogEvent>,
    progress_percent: Option<f64>,
    completed_line: bool,
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

#[derive(Clone)]
struct EndpointSettings {
    name: String,
    url: String,
    user: Option<String>,
    pass: Option<String>,
    ca_path: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Clone)]
struct InputConfig {
    env: InputEnv,
    indices: Vec<InputIndex>,
}

#[derive(Debug, Deserialize, Clone)]
struct InputEnv {
    prefix: String,
    number_of_replicas: u64,
    keep_alive: String,
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
struct InputEnvironment {
    name: String,
    prefix: String,
    number_of_replicas: u64,
    keep_alive: String,
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
}

#[derive(Template)]
#[template(path = "server/run.html")]
struct RunTemplate {
    run: RunView,
    refresh_seconds: u64,
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

#[derive(Clone)]
struct RunSummary {
    id: String,
    created_at: String,
    jobs_total: usize,
    jobs_running: usize,
    jobs_failed: usize,
    jobs_succeeded: usize,
}

#[derive(Clone)]
struct RunView {
    id: String,
    created_at: String,
    stages: Vec<StageView>,
}

#[derive(Clone)]
struct StageView {
    id: String,
    name: String,
    jobs: Vec<JobSummary>,
}

#[derive(Clone)]
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
    load1: f64,
    load5: f64,
    load15: f64,
}

#[derive(Clone, Debug, Default)]
struct MetricsSummary {
    host_cpu: f32,
    proc_cpu: f32,
    host_mem_used_mb: u64,
    host_mem_total_mb: u64,
    proc_mem_mb: u64,
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
    let src_env = load_env_config(&args.src_config);
    let dst_config_path = args.dst_config.clone().unwrap_or_else(|| args.src_config.clone());
    let dst_env = load_env_config(&dst_config_path);

    let client = build_reqwest_client(args.ca_path.as_ref(), args.insecure)
        .unwrap_or_else(|e| panic!("Failed to build HTTP client: {e}"));

    let runs_dir = args.runs_dir.clone();
    if let Err(e) = tokio::fs::create_dir_all(&runs_dir).await {
        panic!("Failed to create runs dir {:?}: {:?}", runs_dir, e);
    }

    let metrics = Arc::new(RwLock::new(MetricsState::default()));
    let (metrics_tx, _) = broadcast::channel(200);
    let state = AppState {
        src_env,
        dst_env,
        src_endpoint: EndpointSettings {
            name: "es-source".to_string(),
            url: args.src_url,
            user: args.src_user,
            pass: args.src_pass,
            ca_path: args.ca_path.clone(),
        },
        dst_endpoint: EndpointSettings {
            name: "es-destination".to_string(),
            url: args.dst_url,
            user: args.dst_user,
            pass: args.dst_pass,
            ca_path: args.ca_path.clone(),
        },
        client,
        runs_dir,
        es_copy_path: args.es_copy_path,
        refresh_seconds: args.refresh_seconds,
        base_path: normalize_base_path(&args.base_path),
        metrics: Arc::clone(&metrics),
        metrics_tx,
        timestamp: args.timestamp,
        from_suffix: args.from_suffix,
        copy_suffix: args.copy_suffix,
        alias_suffix: args.alias_suffix,
        alias_remove_if_exists: args.alias_remove_if_exists,
        audit: args.audit,
        runs: Arc::new(RwLock::new(RunStore::default())),
    };

    load_runs(&state).await;
    start_metrics_sampler(
        Arc::clone(&state.metrics),
        state.metrics_tx.clone(),
        args.metrics_seconds,
    );

    let routes = Router::new()
        .route("/", get(index))
        .route("/status", get(status_view))
        .route("/status/snapshot", get(status_snapshot))
        .route("/status/stream", get(status_stream))
        .route("/runs", post(create_run))
        .route("/runs/{run_id}", get(run_view))
        .route("/runs/{run_id}/stages/{stage_id}/start", post(start_stage))
        .route("/runs/{run_id}/jobs/{job_id}/start", post(start_job))
        .route("/runs/{run_id}/jobs/{job_id}", get(job_view))
        .route("/runs/{run_id}/jobs/{job_id}/stream", get(job_stream));

    let app = if state.base_path == "/" {
        routes
    } else {
        let base_with_slash = format!("{}/", state.base_path);
        Router::new()
            .route("/", get(root_redirect))
            .route(&base_with_slash, get(base_trailing_redirect))
            .nest(&state.base_path, routes)
    }
    .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(&args.bind)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind {}: {e}", &args.bind));
    info!("Server listening on {}", &args.bind);
    axum::serve(listener, app).await.unwrap();
}

async fn index(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let runs = build_run_summaries(&state).await;
    let template = IndexTemplate {
        runs,
        refresh_seconds: state.refresh_seconds,
        base_path: state.base_path.clone(),
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

async fn create_run(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match create_run_state(&state).await {
        Ok(run_id) => Redirect::to(&with_base(&state, &format!("/runs/{}", run_id)))
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create run: {}", err),
        )
            .into_response(),
    }
}

async fn run_view(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    match build_run_view(&state, &run_id).await {
        Some(run) => {
            let template = RunTemplate {
                run,
                refresh_seconds: state.refresh_seconds,
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
            if let Err(err) = start_job_runner(&state_clone, &run_id_clone, &job_id).await {
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
    if let Err(err) = start_job_runner(&state, &run_id, &job_id).await {
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

async fn start_job_runner(
    state: &Arc<AppState>,
    run_id: &str,
    job_id: &str,
) -> Result<(), String> {
    let (config_path, stdout_path, stderr_path, snapshot) = {
        let mut runs = state.runs.write().await;
        let run = runs
            .runs
            .get_mut(run_id)
            .ok_or_else(|| "run not found".to_string())?;
        let (config_path, stdout_path, stderr_path) = {
            let job = run
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| "job not found".to_string())?;
            if matches!(job.status, JobStatus::Running) {
                return Ok(());
            }
            job.status = JobStatus::Running;
            job.started_at = Some(now_string());
            (
                job.config_path.clone(),
                job.stdout_path.clone(),
                job.stderr_path.clone(),
            )
        };
        let snapshot = run_snapshot(run);
        (config_path, stdout_path, stderr_path, snapshot)
    };
    let runs_dir = state.runs_dir.clone();
    tokio::spawn(async move {
        if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
            warn!("Failed to persist run: {}", err);
        }
    });

    let state_clone = Arc::clone(state);
    let run_id = run_id.to_string();
    let job_id = job_id.to_string();
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

    Ok(())
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
            host_mem_used_mb: mem_to_mb(sample.host_mem_used_kb),
            host_mem_total_mb: mem_to_mb(sample.host_mem_total_kb),
            proc_mem_mb: mem_to_mb(sample.proc_mem_kb),
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
    seconds: u64,
) {
    let interval_seconds = if seconds == 0 { 5 } else { seconds };
    tokio::spawn(async move {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new();
        system.refresh_cpu_all();
        system.refresh_memory();
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        tokio::time::sleep(Duration::from_millis(200)).await;
        system.refresh_cpu_all();
        system.refresh_memory();
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);

        let mut ticker = tokio::time::interval(Duration::from_secs(interval_seconds));
        loop {
            ticker.tick().await;
            system.refresh_cpu_all();
            system.refresh_memory();
            system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);

            let host_cpu = system.global_cpu_usage();
            let host_mem_total = system.total_memory();
            let host_mem_used = system.used_memory();
            let load = System::load_average();
            let (proc_cpu, proc_mem) = if let Some(process) = system.process(pid) {
                (process.cpu_usage(), process.memory())
            } else {
                (0.0, 0)
            };

            let sample = MetricsSample {
                ts: chrono::Utc::now().timestamp(),
                host_cpu,
                host_mem_used_kb: host_mem_used,
                host_mem_total_kb: host_mem_total,
                proc_cpu,
                proc_mem_kb: proc_mem,
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
    let mut runs = state.runs.write().await;
    if let Some(run) = runs.runs.get_mut(run_id) {
        if let Some(job) = run.jobs.get_mut(job_id) {
            job.status = status;
            job.exit_code = exit_code;
            job.finished_at = Some(now_string());
            let snapshot = run_snapshot(run);
            let runs_dir = state.runs_dir.clone();
            tokio::spawn(async move {
                if let Err(err) = save_run_snapshot(&runs_dir, snapshot).await {
                    warn!("Failed to persist run: {}", err);
                }
            });
        }
    }
}

async fn build_run_summaries(state: &Arc<AppState>) -> Vec<RunSummary> {
    let runs = state.runs.read().await;
    let mut summaries = Vec::new();
    for run_id in runs.order.iter().rev() {
        if let Some(run) = runs.runs.get(run_id) {
            let mut jobs_running = 0;
            let mut jobs_failed = 0;
            let mut jobs_succeeded = 0;
            let jobs_total = run.jobs.len();
            for job in run.jobs.values() {
                match job.status {
                    JobStatus::Running => jobs_running += 1,
                    JobStatus::Failed => jobs_failed += 1,
                    JobStatus::Succeeded => jobs_succeeded += 1,
                    JobStatus::Pending => {}
                }
            }
            summaries.push(RunSummary {
                id: run.id.clone(),
                created_at: run.created_at.clone(),
                jobs_total,
                jobs_running,
                jobs_failed,
                jobs_succeeded,
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
        for job_id in &stage.job_ids {
            if let Some(job) = run.jobs.get(job_id) {
                let progress_label = job
                    .progress_percent
                    .map(|value| format!("{:.2} %", value));
                let progress_width = job
                    .progress_percent
                    .map(|value| value.max(0.0).min(100.0).round() as u8);
                jobs.push(JobSummary {
                    id: job.id.clone(),
                    name: job.name.clone(),
                    status: job.status.clone(),
                    progress_label,
                    progress_width,
                    completed_line: job.completed_line,
                });
            }
        }
        stages.push(StageView {
            id: stage.id.clone(),
            name: stage.name.clone(),
            jobs,
        });
    }
    Some(RunView {
        id: run.id.clone(),
        created_at: run.created_at.clone(),
        stages,
    })
}

async fn build_job_view(state: &Arc<AppState>, run_id: &str, job_id: &str) -> Option<JobView> {
    let runs = state.runs.read().await;
    let run = runs.runs.get(run_id)?;
    let job = run.jobs.get(job_id)?;
    Some(JobView {
        id: job.id.clone(),
        name: job.name.clone(),
        stage_name: job.stage_name.clone(),
        status: job.status.clone(),
        stdout_tail: job.stdout_tail.iter().cloned().collect(),
        stderr_tail: job.stderr_tail.iter().cloned().collect(),
    })
}

async fn create_run_state(state: &Arc<AppState>) -> Result<String, String> {
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

    let stages = plan_stages(&state.src_env, state).await?;

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

            let output_config = build_output_config(state, &job, &state.src_env, &state.dst_env)?;
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
                    config_path,
                    stdout_path,
                    stderr_path,
                    stdout_tail: VecDeque::with_capacity(MAX_TAIL_LINES),
                    stderr_tail: VecDeque::with_capacity(MAX_TAIL_LINES),
                    stdout_tx,
                    stderr_tx,
                    progress_percent: None,
                    completed_line: false,
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
        src_config_path: state.src_env.name.clone().into(),
        dst_config_path: state.dst_env.name.clone().into(),
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
    let mut entries = match tokio::fs::read_dir(&state.runs_dir).await {
        Ok(entries) => entries,
        Err(err) => {
            warn!("Failed to read runs dir: {}", err);
            return;
        }
    };

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
        match tokio::fs::read_to_string(&run_file).await {
            Ok(content) => match serde_json::from_str::<RunPersist>(&content) {
                Ok(persist) => {
                    let mut job_map = HashMap::new();
                    let mut run_changed = false;
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
                                config_path: PathBuf::from(job.config_path),
                                stdout_path: PathBuf::from(job.stdout_path),
                                stderr_path: PathBuf::from(job.stderr_path),
                                stdout_tail: VecDeque::from(stdout_tail),
                                stderr_tail: VecDeque::from(stderr_tail),
                                stdout_tx,
                                stderr_tx,
                                progress_percent: job.progress_percent,
                                completed_line: job.completed_line,
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
                        src_config_path: PathBuf::from(persist.src_config_path),
                        dst_config_path: PathBuf::from(persist.dst_config_path),
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
                    runs.runs.insert(
                        run_id,
                        run_state,
                    );
                }
                Err(err) => warn!("Failed to parse run.json: {}", err),
            },
            Err(err) => warn!("Failed to read run.json: {}", err),
        }
    }

    let mut store = state.runs.write().await;
    store.order = runs.order;
    store.runs = runs.runs;
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
        })
        .collect::<Vec<_>>();
    RunPersist {
        id: run.id.clone(),
        created_at: run.created_at.clone(),
        src_config_path: run.src_config_path.to_string_lossy().to_string(),
        dst_config_path: run.dst_config_path.to_string_lossy().to_string(),
        stages,
        jobs,
    }
}

async fn save_run_snapshot(runs_dir: &PathBuf, run: RunPersist) -> Result<(), String> {
    let path = runs_dir.join(&run.id).join("run.json");
    let content = serde_json::to_string_pretty(&run).map_err(|e| e.to_string())?;
    tokio::fs::write(path, content)
        .await
        .map_err(|e| e.to_string())
}

fn load_env_config(path: &PathBuf) -> InputEnvironment {
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read config {:?}: {e}", path));
    let config: InputConfig =
        toml::from_str(&content).unwrap_or_else(|e| panic!("Invalid config {:?}: {e}", path));
    let force_one_shard = std::env::var("ONE_SHARD_ONLY")
        .map(|value| value.to_lowercase() == "true")
        .unwrap_or(false);
    let indices = config
        .indices
        .into_iter()
        .map(|mut index| {
            if force_one_shard {
                index.number_of_shards = Some(1);
            }
            index
        })
        .collect::<Vec<_>>();
    InputEnvironment {
        name: path.to_string_lossy().to_string(),
        prefix: config.env.prefix,
        number_of_replicas: config.env.number_of_replicas,
        keep_alive: config.env.keep_alive,
        indices,
    }
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

fn slugify(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>()
        .trim_matches('_')
        .to_lowercase()
}

async fn plan_stages(
    src_env: &InputEnvironment,
    state: &AppState,
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
    for index in &src_env.indices {
        if let Some(split) = &index.split {
            let stage_name = format!("{}-{}", DEFAULT_STAGE_NAME, index.name);
            let date_ranges = generate_date_intervals(state, src_env, index, split).await?;
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
            },
            );
            for (i, (from, to)) in date_ranges.into_iter().enumerate() {
                push_job(
                    stage_name.clone(),
                    JobPlan {
                    name: format!("{}-{}-{}-{}", DEFAULT_STAGE_NAME, index.name, SPLIT_SUFFIX, i + 1),
                    index: index.clone(),
                    date_from: from,
                    date_to: to,
                    leftover: false,
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
            },
            );
        }
    }
    Ok(stages)
}

async fn generate_date_intervals(
    state: &AppState,
    src_env: &InputEnvironment,
    index: &InputIndex,
    split: &SplitConfig,
) -> Result<Vec<(Option<String>, Option<String>)>, String> {
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
        &src_env.prefix,
        &index.name,
        state.from_suffix.as_deref(),
    );
    let url = format!("{}/{}/_search", state.src_endpoint.url, index_name);
    let mut request = state.client.get(url).json(&payload);
    if let Some(user) = &state.src_endpoint.user {
        request = request.basic_auth(user, state.src_endpoint.pass.clone());
    }
    let response = request
        .timeout(Duration::from_secs(120))
        .send()
        .await
        .map_err(|e| format!("percentile request failed: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("percentile request failed: {}", status));
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
        ranges.push((window[0].clone(), window[1].clone()));
    }

    if ranges.len() < split.number_of_parts as usize {
        return Err("percentile ranges shorter than expected".to_string());
    }

    Ok(ranges.into_iter().take(split.number_of_parts as usize).collect())
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
    src_env: &InputEnvironment,
    dst_env: &InputEnvironment,
) -> Result<OutputConfig, String> {
    let dst_index = dst_env
        .indices
        .iter()
        .find(|i| i.name == job.index.name);

    let timestamp = state
        .timestamp
        .clone()
        .unwrap_or_else(|| chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string());
    let number_of_shards = job
        .index
        .number_of_shards
        .unwrap_or(1)
        .max(1);
    let number_of_replicas = dst_index
        .and_then(|idx| idx.number_of_replicas)
        .unwrap_or(dst_env.number_of_replicas);

    let index_name = build_index_name(
        &src_env.prefix,
        &job.index.name,
        state.from_suffix.as_deref(),
    );
    let name_of_copy = build_name_of_copy(
        &dst_env.prefix,
        &job.index.name,
        &timestamp,
        state.copy_suffix.as_deref(),
    );
    let alias_name = build_alias_name(
        &dst_env.prefix,
        &job.index.name,
        state.alias_suffix.as_deref(),
    );

    let mut custom_mapping = dst_index.and_then(|idx| idx.custom.as_ref()).and_then(|c| c.mapping.clone());
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
            name: state.src_endpoint.name.clone(),
            url: state.src_endpoint.url.clone(),
            root_certificates: state.ca_path().map(|p| p.to_string_lossy().to_string()),
            basic_auth: state
                .src_endpoint
                .user
                .clone()
                .map(|user| OutputBasicAuth {
                    username: user,
                    password: state.src_endpoint.pass.clone(),
                }),
        },
        OutputEndpoint {
            name: state.dst_endpoint.name.clone(),
            url: state.dst_endpoint.url.clone(),
            root_certificates: state.ca_path().map(|p| p.to_string_lossy().to_string()),
            basic_auth: state
                .dst_endpoint
                .user
                .clone()
                .map(|user| OutputBasicAuth {
                    username: user,
                    password: state.dst_endpoint.pass.clone(),
                }),
        },
    ];

    let index = OutputIndex {
        buffer_size: job.index.buffer_size,
        copy_content: true,
        copy_mapping,
        delete_if_exists: false,
        from: state.src_endpoint.name.clone(),
        keep_alive: src_env.keep_alive.clone(),
        name: index_name,
        name_of_copy,
        number_of_replicas,
        number_of_shards,
        to: state.dst_endpoint.name.clone(),
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
        self.src_endpoint.ca_path.clone()
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
