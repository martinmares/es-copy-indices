use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
};

pub enum What {
    PreCreateRequest,
    PreCreateResponseOk,
    PreCreateResponseErr,
    BulkRequest,
    BulkResponseOk,
    BulkResponseErr,
}

impl What {
    pub fn as_str(&self) -> &'static str {
        match self {
            What::PreCreateRequest => "PreCreateRequest",
            What::PreCreateResponseOk => "PreCreateResponseOk",
            What::PreCreateResponseErr => "PreCreateResponseErr",
            What::BulkRequest => "BulkRequest",
            What::BulkResponseOk => "BulkResponseOk",
            What::BulkResponseErr => "BulkResponseErr",
        }
    }
}

pub struct AuditBuilder {
    file_handler: Option<File>, // Použití Option místo Result
}

impl AuditBuilder {
    /// Konstruktor ::new
    pub async fn new(file_name: &str) -> Self {
        // Create all parent dirs
        if let Some(parent) = std::path::Path::new(file_name).parent() {
            let _ = fs::create_dir_all(parent).await;
        }

        // Open file handler
        let file_handler = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_name)
            .await
            .ok(); // Převede Result<File, Error> na Option<File>

        Self { file_handler }
    }

    pub async fn append_to_file(&mut self, data: &str) -> std::io::Result<()> {
        if let Some(file) = self.file_handler.as_mut() {
            file.write_all(data.as_bytes()).await?;
            file.flush().await?;
        }
        Ok(())
    }
}
