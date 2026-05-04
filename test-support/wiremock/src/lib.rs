use serde_json::{json, Value};
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_SERVER_ID: AtomicUsize = AtomicUsize::new(1);

pub mod matchers {
    use super::Matcher;

    pub fn method(value: &str) -> Matcher {
        Matcher::Method(value.to_ascii_uppercase())
    }

    pub fn path(value: &str) -> Matcher {
        Matcher::Path(value.to_string())
    }

    pub fn header(name: &str, value: &str) -> Matcher {
        Matcher::Header(name.to_ascii_lowercase(), value.to_string())
    }
}

#[derive(Clone, Debug)]
pub enum Matcher {
    Method(String),
    Path(String),
    Header(String, String),
}

#[derive(Clone, Debug)]
pub struct ResponseTemplate {
    status: u16,
    body: String,
}

impl ResponseTemplate {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            body: String::new(),
        }
    }

    pub fn set_body_json(mut self, value: Value) -> Self {
        self.body = serde_json::to_string(&value).expect("serialize JSON response body");
        self
    }
}

#[derive(Clone, Debug)]
struct MockDefinition {
    matchers: Vec<Matcher>,
    response: ResponseTemplate,
}

#[derive(Debug)]
pub struct Mock {
    definition: MockDefinition,
}

impl Mock {
    pub fn given(matcher: Matcher) -> Self {
        Self {
            definition: MockDefinition {
                matchers: vec![matcher],
                response: ResponseTemplate::new(200),
            },
        }
    }

    pub fn and(mut self, matcher: Matcher) -> Self {
        self.definition.matchers.push(matcher);
        self
    }

    pub fn respond_with(mut self, response: ResponseTemplate) -> Self {
        self.definition.response = response;
        self
    }

    pub async fn mount(self, server: &MockServer) {
        let key = server.env_key();
        let mut mocks: Vec<Value> = env::var(&key)
            .ok()
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default();
        mocks.push(self.definition.to_json());
        env::set_var(key, serde_json::to_string(&mocks).expect("serialize mocks"));
    }
}

#[derive(Debug)]
pub struct MockServer {
    id: usize,
}

impl MockServer {
    pub async fn start() -> Self {
        Self {
            id: NEXT_SERVER_ID.fetch_add(1, Ordering::SeqCst),
        }
    }

    pub fn uri(&self) -> String {
        format!("http://wiremock-{}.mnemosctl.test", self.id)
    }

    fn env_key(&self) -> String {
        format!("MNEMOSCTL_WIREMOCK_{}", self.id)
    }
}

impl MockDefinition {
    fn to_json(&self) -> Value {
        json!({
            "matchers": self
                .matchers
                .iter()
                .map(Matcher::to_json)
                .collect::<Vec<_>>(),
            "response": {
                "status": self.response.status,
                "body": self.response.body,
            }
        })
    }
}

impl Matcher {
    fn to_json(&self) -> Value {
        match self {
            Matcher::Method(value) => json!({
                "kind": "method",
                "value": value,
            }),
            Matcher::Path(value) => json!({
                "kind": "path",
                "value": value,
            }),
            Matcher::Header(name, value) => json!({
                "kind": "header",
                "name": name,
                "value": value,
            }),
        }
    }
}
