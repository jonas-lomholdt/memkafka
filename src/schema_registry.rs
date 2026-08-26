use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{Path, Request, State, rejection::JsonRejection},
    http::{HeaderValue, StatusCode, header},
    middleware::{Next, from_fn},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredSchema {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    pub schema: String,
}

#[derive(Clone, Debug)]
pub struct Registry {
    inner: Arc<RwLock<RegistryStore>>,
}

#[derive(Debug)]
struct RegistryStore {
    next_schema_id: i32,
    schemas_by_text: HashMap<String, i32>,
    schemas_by_id: HashMap<i32, String>,
    subjects: BTreeMap<String, Vec<RegisteredSchema>>,
}

impl Default for RegistryStore {
    fn default() -> Self {
        Self {
            next_schema_id: 1,
            schemas_by_text: HashMap::new(),
            schemas_by_id: HashMap::new(),
            subjects: BTreeMap::new(),
        }
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RegistryStore::default())),
        }
    }

    pub async fn register(&self, subject: &str, schema: &str) -> RegisteredSchema {
        let mut store = self.inner.write().await;
        if let Some(existing) = store
            .subjects
            .get(subject)
            .and_then(|versions| versions.iter().find(|version| version.schema == schema))
        {
            return existing.clone();
        }

        let id = match store.schemas_by_text.get(schema) {
            Some(id) => *id,
            None => {
                let id = store.next_schema_id;
                store.next_schema_id = store
                    .next_schema_id
                    .checked_add(1)
                    .expect("schema ID space exhausted");
                store.schemas_by_text.insert(schema.to_owned(), id);
                store.schemas_by_id.insert(id, schema.to_owned());
                id
            }
        };
        let versions = store.subjects.entry(subject.to_owned()).or_default();
        let version = i32::try_from(versions.len() + 1).expect("subject version space exhausted");
        let registered = RegisteredSchema {
            subject: subject.to_owned(),
            version,
            id,
            schema: schema.to_owned(),
        };
        versions.push(registered.clone());
        info!(subject, id, version, "registered schema");
        registered
    }

    pub async fn subjects(&self) -> Vec<String> {
        self.inner.read().await.subjects.keys().cloned().collect()
    }

    pub async fn versions(&self, subject: &str) -> Result<Vec<i32>, RegistryError> {
        self.inner
            .read()
            .await
            .subjects
            .get(subject)
            .map(|versions| versions.iter().map(|version| version.version).collect())
            .ok_or(RegistryError::SubjectNotFound)
    }

    pub async fn lookup(
        &self,
        subject: &str,
        schema: &str,
    ) -> Result<RegisteredSchema, RegistryError> {
        let store = self.inner.read().await;
        store
            .subjects
            .get(subject)
            .ok_or(RegistryError::SubjectNotFound)?
            .iter()
            .find(|version| version.schema == schema)
            .cloned()
            .ok_or(RegistryError::SchemaNotFound)
    }

    pub async fn version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, RegistryError> {
        let store = self.inner.read().await;
        store
            .subjects
            .get(subject)
            .ok_or(RegistryError::SubjectNotFound)?
            .iter()
            .find(|registered| registered.version == version)
            .cloned()
            .ok_or(RegistryError::VersionNotFound)
    }

    pub async fn latest(&self, subject: &str) -> Result<RegisteredSchema, RegistryError> {
        self.inner
            .read()
            .await
            .subjects
            .get(subject)
            .ok_or(RegistryError::SubjectNotFound)?
            .last()
            .cloned()
            .ok_or(RegistryError::SubjectNotFound)
    }

    pub async fn schema_by_id(&self, id: i32) -> Result<String, RegistryError> {
        self.inner
            .read()
            .await
            .schemas_by_id
            .get(&id)
            .cloned()
            .ok_or(RegistryError::SchemaNotFound)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryError {
    SubjectNotFound,
    VersionNotFound,
    SchemaNotFound,
}

pub fn router(registry: Registry) -> Router {
    Router::new()
        .route("/subjects", get(list_subjects))
        .route("/subjects/{subject}", axum::routing::post(lookup_schema))
        .route(
            "/subjects/{subject}/versions",
            get(list_versions).post(register_schema),
        )
        .route(
            "/subjects/{subject}/versions/{version}",
            get(get_subject_version),
        )
        .route("/schemas/ids/{id}", get(get_schema_by_id))
        .route("/config", get(get_global_config))
        .route("/config/{subject}", get(get_subject_config))
        .layer(from_fn(use_schema_registry_content_type))
        .with_state(registry)
}

async fn use_schema_registry_content_type(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    if response.headers().get(header::CONTENT_TYPE)
        == Some(&HeaderValue::from_static("application/json"))
    {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.schemaregistry.v1+json"),
        );
    }
    response
}

#[derive(Debug, Deserialize)]
struct SchemaRequest {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<Value>,
}

impl SchemaRequest {
    fn validate(self) -> Result<String, HttpError> {
        if self.schema.trim().is_empty() {
            return Err(HttpError::invalid_schema("Schema must not be empty"));
        }
        if let Some(schema_type) = self.schema_type
            && schema_type != "AVRO"
        {
            return Err(HttpError::invalid_schema(format!(
                "Unsupported schema type '{schema_type}'"
            )));
        }
        if !self.references.is_empty() {
            return Err(HttpError::invalid_schema(
                "Schema references are not supported",
            ));
        }
        apache_avro::Schema::parse_str(&self.schema)
            .map_err(|_| HttpError::invalid_schema("Invalid Avro schema"))?;
        Ok(self.schema)
    }
}

#[derive(Debug, Serialize)]
struct RegisterResponse {
    id: i32,
}

#[derive(Debug, Serialize)]
struct RegisteredSchemaResponse {
    subject: String,
    version: i32,
    id: i32,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: &'static str,
}

impl From<RegisteredSchema> for RegisteredSchemaResponse {
    fn from(schema: RegisteredSchema) -> Self {
        Self {
            subject: schema.subject,
            version: schema.version,
            id: schema.id,
            schema: schema.schema,
            schema_type: "AVRO",
        }
    }
}

#[derive(Debug, Serialize)]
struct SchemaResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: &'static str,
}

#[derive(Debug, Serialize)]
struct ConfigResponse {
    #[serde(rename = "compatibilityLevel")]
    compatibility_level: &'static str,
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error_code: i32,
    message: String,
}

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    error_code: i32,
    message: String,
}

impl HttpError {
    fn subject_not_found(subject: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error_code: 40401,
            message: format!("Subject '{subject}' not found"),
        }
    }

    fn version_not_found(version: i32) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error_code: 40402,
            message: format!("Version {version} not found"),
        }
    }

    fn schema_not_found(description: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error_code: 40403,
            message: description.into(),
        }
    }

    fn invalid_schema(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            error_code: 42201,
            message: message.into(),
        }
    }

    fn invalid_version(version: &str) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            error_code: 42202,
            message: format!("Invalid version '{version}'"),
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorEnvelope {
                error_code: self.error_code,
                message: self.message,
            }),
        )
            .into_response()
    }
}

async fn list_subjects(State(registry): State<Registry>) -> Json<Vec<String>> {
    Json(registry.subjects().await)
}

async fn register_schema(
    State(registry): State<Registry>,
    Path(subject): Path<String>,
    request: Result<Json<SchemaRequest>, JsonRejection>,
) -> Result<Json<RegisterResponse>, HttpError> {
    let schema = request
        .map_err(|error| HttpError::invalid_schema(error.body_text()))?
        .0
        .validate()?;
    let registered = registry.register(&subject, &schema).await;
    Ok(Json(RegisterResponse { id: registered.id }))
}

async fn lookup_schema(
    State(registry): State<Registry>,
    Path(subject): Path<String>,
    request: Result<Json<SchemaRequest>, JsonRejection>,
) -> Result<Json<RegisteredSchemaResponse>, HttpError> {
    let schema = request
        .map_err(|error| HttpError::invalid_schema(error.body_text()))?
        .0
        .validate()?;
    registry
        .lookup(&subject, &schema)
        .await
        .map(RegisteredSchemaResponse::from)
        .map(Json)
        .map_err(|error| match error {
            RegistryError::SubjectNotFound => HttpError::subject_not_found(&subject),
            RegistryError::SchemaNotFound => {
                HttpError::schema_not_found(format!("Schema not found under subject '{subject}'"))
            }
            RegistryError::VersionNotFound => unreachable!("lookup does not return version errors"),
        })
}

async fn list_versions(
    State(registry): State<Registry>,
    Path(subject): Path<String>,
) -> Result<Json<Vec<i32>>, HttpError> {
    registry
        .versions(&subject)
        .await
        .map(Json)
        .map_err(|_| HttpError::subject_not_found(&subject))
}

async fn get_subject_version(
    State(registry): State<Registry>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Json<RegisteredSchemaResponse>, HttpError> {
    let registered = if version == "latest" {
        registry.latest(&subject).await
    } else {
        let parsed = version
            .parse::<i32>()
            .ok()
            .filter(|version| *version > 0)
            .ok_or_else(|| HttpError::invalid_version(&version))?;
        registry.version(&subject, parsed).await
    };
    registered
        .map(RegisteredSchemaResponse::from)
        .map(Json)
        .map_err(|error| match error {
            RegistryError::SubjectNotFound => HttpError::subject_not_found(&subject),
            RegistryError::VersionNotFound => HttpError::version_not_found(
                version
                    .parse()
                    .expect("numeric version was validated above"),
            ),
            RegistryError::SchemaNotFound => {
                unreachable!("version reads do not return schema errors")
            }
        })
}

async fn get_schema_by_id(
    State(registry): State<Registry>,
    Path(id): Path<String>,
) -> Result<Json<SchemaResponse>, HttpError> {
    let id = id
        .parse::<i32>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| HttpError::schema_not_found(format!("Schema {id} not found")))?;
    registry
        .schema_by_id(id)
        .await
        .map(|schema| {
            Json(SchemaResponse {
                schema,
                schema_type: "AVRO",
            })
        })
        .map_err(|_| HttpError::schema_not_found(format!("Schema {id} not found")))
}

async fn get_global_config() -> Json<ConfigResponse> {
    Json(ConfigResponse {
        compatibility_level: "NONE",
    })
}

async fn get_subject_config(
    State(registry): State<Registry>,
    Path(subject): Path<String>,
) -> Result<Json<ConfigResponse>, HttpError> {
    registry
        .versions(&subject)
        .await
        .map_err(|_| HttpError::subject_not_found(&subject))?;
    Ok(Json(ConfigResponse {
        compatibility_level: "NONE",
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::{Registry, RegistryError, router};

    const FIRST_SCHEMA: &str =
        r#"{"type":"record","name":"First","fields":[{"name":"value","type":"string"}]}"#;
    const SECOND_SCHEMA: &str =
        r#"{"type":"record","name":"Second","fields":[{"name":"count","type":"int"}]}"#;
    const FORMATTED_FIRST_SCHEMA: &str = r#"{ "type": "record", "name": "First", "fields": [{ "name": "value", "type": "string" }] }"#;

    #[tokio::test]
    async fn allocates_global_ids_and_subject_versions_with_exact_deduplication() {
        let registry = Registry::new();

        let first = registry.register("alpha-value", FIRST_SCHEMA).await;
        let duplicate = registry.register("alpha-value", FIRST_SCHEMA).await;
        let second = registry.register("alpha-value", SECOND_SCHEMA).await;
        let formatted = registry
            .register("alpha-value", FORMATTED_FIRST_SCHEMA)
            .await;
        let reused = registry.register("beta-value", FIRST_SCHEMA).await;

        assert_eq!((first.id, first.version), (1, 1));
        assert_eq!(duplicate, first);
        assert_eq!((second.id, second.version), (2, 2));
        assert_eq!((formatted.id, formatted.version), (3, 3));
        assert_eq!((reused.id, reused.version), (1, 1));
        assert_eq!(registry.subjects().await, vec!["alpha-value", "beta-value"]);
        assert_eq!(
            registry.versions("alpha-value").await.unwrap(),
            vec![1, 2, 3]
        );
        assert_eq!(
            registry.lookup("alpha-value", FIRST_SCHEMA).await.unwrap(),
            first
        );
        assert_eq!(registry.latest("alpha-value").await.unwrap(), formatted);
        assert_eq!(registry.schema_by_id(1).await.unwrap(), FIRST_SCHEMA);
    }

    #[tokio::test]
    async fn reports_missing_registry_resources() {
        let registry = Registry::new();
        registry.register("known-value", FIRST_SCHEMA).await;

        assert_eq!(
            registry.versions("missing-value").await,
            Err(RegistryError::SubjectNotFound)
        );
        assert_eq!(
            registry.version("known-value", 2).await,
            Err(RegistryError::VersionNotFound)
        );
        assert_eq!(
            registry.lookup("known-value", SECOND_SCHEMA).await,
            Err(RegistryError::SchemaNotFound)
        );
        assert_eq!(
            registry.schema_by_id(99).await,
            Err(RegistryError::SchemaNotFound)
        );
    }

    #[tokio::test]
    async fn concurrent_registration_keeps_ids_and_versions_unique() {
        let registry = Registry::new();
        let mut tasks = Vec::new();
        for index in 0..32 {
            let registry = registry.clone();
            tasks.push(tokio::spawn(async move {
                let schema = format!(r#"{{"type":"record","name":"Event{index}","fields":[]}}"#);
                registry.register("events-value", &schema).await
            }));
        }

        let mut ids = BTreeSet::new();
        let mut versions = BTreeSet::new();
        for task in tasks {
            let registered = task.await.unwrap();
            ids.insert(registered.id);
            versions.insert(registered.version);
        }

        assert_eq!(ids, (1..=32).collect());
        assert_eq!(versions, (1..=32).collect());
    }

    #[tokio::test]
    async fn exposes_registration_lookup_and_config_routes() {
        let app = router(Registry::new());
        let config_response = app
            .clone()
            .oneshot(Request::get("/config").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            config_response.headers()[header::CONTENT_TYPE],
            "application/vnd.schemaregistry.v1+json"
        );
        let registered = request_json(
            app.clone(),
            "POST",
            "/subjects/orders-value/versions",
            Some(json!({"schema": FIRST_SCHEMA})),
        )
        .await;
        assert_eq!(registered, (StatusCode::OK, json!({"id": 1})));

        let duplicate = request_json(
            app.clone(),
            "POST",
            "/subjects/orders-value/versions?normalize=false",
            Some(json!({"schema": FIRST_SCHEMA, "schemaType": "AVRO", "references": []})),
        )
        .await;
        assert_eq!(duplicate, (StatusCode::OK, json!({"id": 1})));

        assert_eq!(
            request_json(app.clone(), "GET", "/subjects", None).await,
            (StatusCode::OK, json!(["orders-value"]))
        );
        assert_eq!(
            request_json(app.clone(), "GET", "/subjects/orders-value/versions", None).await,
            (StatusCode::OK, json!([1]))
        );

        let expected_schema = json!({
            "subject": "orders-value",
            "version": 1,
            "id": 1,
            "schema": FIRST_SCHEMA,
            "schemaType": "AVRO"
        });
        assert_eq!(
            request_json(
                app.clone(),
                "GET",
                "/subjects/orders-value/versions/1",
                None
            )
            .await,
            (StatusCode::OK, expected_schema.clone())
        );
        assert_eq!(
            request_json(
                app.clone(),
                "GET",
                "/subjects/orders-value/versions/latest",
                None
            )
            .await,
            (StatusCode::OK, expected_schema.clone())
        );
        assert_eq!(
            request_json(
                app.clone(),
                "POST",
                "/subjects/orders-value",
                Some(json!({"schema": FIRST_SCHEMA})),
            )
            .await,
            (StatusCode::OK, expected_schema)
        );
        assert_eq!(
            request_json(app.clone(), "GET", "/schemas/ids/1", None).await,
            (
                StatusCode::OK,
                json!({"schema": FIRST_SCHEMA, "schemaType": "AVRO"})
            )
        );
        assert_eq!(
            request_json(app.clone(), "GET", "/config", None).await,
            (StatusCode::OK, json!({"compatibilityLevel": "NONE"}))
        );
        assert_eq!(
            request_json(app, "GET", "/config/orders-value", None).await,
            (StatusCode::OK, json!({"compatibilityLevel": "NONE"}))
        );
    }

    #[tokio::test]
    async fn returns_confluent_errors_for_missing_invalid_and_unsupported_requests() {
        let app = router(Registry::new());
        assert_error(
            request_json(app.clone(), "GET", "/subjects/missing/versions", None).await,
            StatusCode::NOT_FOUND,
            40401,
        );
        assert_error(
            request_json(app.clone(), "GET", "/subjects/missing/versions/1", None).await,
            StatusCode::NOT_FOUND,
            40401,
        );
        assert_error(
            request_json(
                app.clone(),
                "POST",
                "/subjects/invalid/versions",
                Some(json!({"schema": "{\"type\":\"record\"}"})),
            )
            .await,
            StatusCode::UNPROCESSABLE_ENTITY,
            42201,
        );
        assert_eq!(
            request_json(app.clone(), "GET", "/subjects", None).await,
            (StatusCode::OK, json!([]))
        );
        assert_eq!(
            request_json(
                app.clone(),
                "POST",
                "/subjects/known/versions",
                Some(json!({"schema": FIRST_SCHEMA})),
            )
            .await,
            (StatusCode::OK, json!({"id": 1}))
        );
        assert_error(
            request_json(app.clone(), "GET", "/subjects/known/versions/2", None).await,
            StatusCode::NOT_FOUND,
            40402,
        );
        assert_error(
            request_json(app.clone(), "GET", "/subjects/known/versions/zero", None).await,
            StatusCode::UNPROCESSABLE_ENTITY,
            42202,
        );
        assert_error(
            request_json(app.clone(), "GET", "/schemas/ids/99", None).await,
            StatusCode::NOT_FOUND,
            40403,
        );
        assert_error(
            request_json(
                app.clone(),
                "POST",
                "/subjects/known/versions",
                Some(json!({"schema": FIRST_SCHEMA, "schemaType": "PROTOBUF"})),
            )
            .await,
            StatusCode::UNPROCESSABLE_ENTITY,
            42201,
        );
        assert_error(
            request_json(
                app,
                "POST",
                "/subjects/known/versions",
                Some(json!({"schema": FIRST_SCHEMA, "references": [{"name": "x"}]})),
            )
            .await,
            StatusCode::UNPROCESSABLE_ENTITY,
            42201,
        );
    }

    async fn request_json(
        app: axum::Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(body) => {
                request = request.header("content-type", "application/vnd.schemaregistry.v1+json");
                Body::from(body.to_string())
            }
            None => Body::empty(),
        };
        let response = app.oneshot(request.body(body).unwrap()).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    fn assert_error(actual: (StatusCode, Value), status: StatusCode, code: i32) {
        assert_eq!(actual.0, status);
        assert_eq!(actual.1["error_code"], code);
        assert!(
            actual.1["message"]
                .as_str()
                .is_some_and(|message| !message.is_empty())
        );
    }
}
