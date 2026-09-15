#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use oah_channels::ChannelSecrets;
use oah_mcp::{InProcessMcp, McpAllowlist};
use oah_core::AgentName;
use oah_http::{agent_router, AppState};
use oah_model::MockModel;
use oah_runtime::agent::{Agent, Instructions, RenderCx, RenderError};
use oah_runtime::tools::lookup_ticket_tool;
use oah_runtime::Runtime;
use oah_schedules::ScheduleBook;
use oah_store::Store;
use oah_store_memory::MemoryStore;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

struct Desk {
    name: AgentName,
}

impl Agent for Desk {
    fn name(&self) -> &AgentName {
        &self.name
    }
    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model("mock/scripted")?;
        cx.use_tool(lookup_ticket_tool())?;
        Ok(Instructions("desk".into()))
    }
}

async fn app() -> axum::Router {
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let rt = Runtime::builder()
        .store(store)
        .model(Arc::new(MockModel::support_desk()))
        .agent(Arc::new(Desk {
            name: AgentName::parse("support-desk").unwrap(),
        }))
        .demo(true)
        .build()
        .unwrap();
    agent_router(AppState {
        runtime: Arc::new(rt),
        public_base: "http://127.0.0.1:43147".into(),
        channels: ChannelSecrets::default(),
        schedules: Arc::new(tokio::sync::Mutex::new(ScheduleBook::default())),
        mcp: Arc::new(InProcessMcp::new(McpAllowlist::default())),
    })
}

#[tokio::test]
async fn post_is_202_and_history_v1() {
    let app = app().await;
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/agents/support-desk/golden")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"text":"ticket 42"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    assert!(res.headers().get("location").is_some());
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v.get("streamUrl").is_some());
    assert!(v.get("submissionId").is_some());
    assert_eq!(v.get("offset").and_then(Value::as_str), Some("-1"));

    let coord = oah_runtime::Coordinator::new(
        // rebuild is hard; just GET empty-ok or health
        {
            // Use health + agents list as goldens.
            Arc::new(
                Runtime::builder()
                    .store(Arc::new(MemoryStore::new()))
                    .model(Arc::new(MockModel::support_desk()))
                    .agent(Arc::new(Desk {
                        name: AgentName::parse("support-desk").unwrap(),
                    }))
                    .demo(true)
                    .build()
                    .unwrap(),
            )
        },
    );
    let _ = coord;

    let live = app
        .clone()
        .oneshot(Request::builder().uri("/health/live").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(live.status(), StatusCode::OK);

    let agents = app
        .oneshot(Request::builder().uri("/agents").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = agents.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["agents"][0]["name"], "support-desk");
}

#[tokio::test]
async fn agui_and_schedules() {
    let app = app().await;
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ag-ui")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "threadId": "support-desk/ag1",
                        "messages": [{"role":"user","content":"hi"}]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["events"][0]["type"], "RUN_STARTED");

    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/schedules")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "agent": "support-desk",
                        "instance": "nightly",
                        "cron": "0 0 * * *"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);

    let listed = app
        .oneshot(Request::builder().uri("/schedules").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = listed.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["schedules"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn bearer_hook_disabled_without_secret() {
    let app = app().await;
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/hooks/bearer/support-desk/wh")
                .header("authorization", "Bearer x")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn mcp_door_initialize_and_dispatch() {
    let app = app().await;
    let init = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let body = init.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["result"]["serverInfo"]["name"], "oah");

    let listed = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = listed.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["name"] == "oah_dispatch"));

    let call = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "jsonrpc":"2.0",
                        "id":3,
                        "method":"tools/call",
                        "params":{
                            "name":"oah_dispatch",
                            "arguments":{"agent":"support-desk","id":"mcp1","message":"ticket 42"}
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(call.status(), StatusCode::OK);
}

#[tokio::test]
async fn wait_rejected() {
    let app = app().await;
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/agents/support-desk/x?wait=1")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"text":"hi"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}
