"""One-shot, assertion-checked v4 review cleanup; removed from the final tree."""
from pathlib import Path
import re
import shutil

ROOT = Path.cwd()

def replace(path, old, new, count=1):
    p = ROOT / path
    text = p.read_text()
    actual = text.count(old)
    if actual != count:
        raise RuntimeError(f"{path}: expected {count} occurrences, found {actual}: {old[:100]!r}")
    p.write_text(text.replace(old, new))

# External wire event names are not internal field names. Fix probes as well as the adapter.
for path in [
    "crates/zhir-openai/src/live/codec.rs",
    "crates/zhir-openai/src/live/protocol.rs",
    "crates/zhir-testing/tests/gpt_live/fixture.rs",
    "crates/zhir-testing/scripts/live_probe.py",
    "crates/zhir-testing/tests/live_subscription_probe.rs",
]:
    p = ROOT / path
    text = p.read_text()
    assert '"session.closure.is_some()"' in text, path
    p.write_text(text.replace('"session.closure.is_some()"', '"session.closed"'))
fixture = ROOT / "crates/zhir-testing/tests/gpt_live/fixtures/session-closed.json"
fixture.parent.mkdir(parents=True, exist_ok=True)
fixture.write_text('{"type":"session.closed","reason":"client_request","usage":{"audio_duration_ms":80}}\n')
replace("crates/zhir-testing/tests/gpt_live/fixture.rs",
    'channel.send_text(json!({"type":"session.closed","reason":reason,"usage":{"audio_duration_ms":80}}).to_string()).await.unwrap();',
    '''let mut closure: Value = serde_json::from_str(include_str!("fixtures/session-closed.json")).unwrap();
            closure["reason"] = json!(reason);
            channel.send_text(closure.to_string()).await.unwrap();''')

replace("crates/zhir-core/src/wire/mod.rs", "schemars(range(min = 3, max = 3))", "schemars(range(min = 4, max = 4))")

# Introduce binding identity separately from an external recovery credential.
# Update all current callers atomically, without old aliases or readers.
for p in list((ROOT / "crates").rglob("*.rs")) + list((ROOT / "conformance").rglob("*.rs")):
    text = p.read_text()
    def literal(m):
        before = text[max(0, m.start() - 100):m.start()]
        if re.search(r"(?:\bstruct|\bimpl|->)\s*(?:[A-Za-z_][A-Za-z0-9_]*::)*$", before):
            return m.group(0)
        return m.group(0) + "\n        binding: None,"
    edited = re.sub(r"\b(?:SessionOpen|SessionSnapshot)\s*\{", literal, text)
    if edited != text:
        p.write_text(edited)
replace("crates/zhir-core/src/model/session.rs", "#[derive(Clone)]\npub struct SessionOpen {", '''/// Stable adapter selection. This is not evidence of remote session recovery.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBinding {
    pub adapter: String,
    pub data: Value,
}

#[derive(Clone)]
pub struct SessionOpen {
    pub binding: Option<ModelBinding>,''')
replace("crates/zhir-core/src/model/session.rs", "pub trait SessionSender: Send + Sync {", '''pub trait SessionSender: Send + Sync {
    /// Identity to persist independently of optional remote recovery credentials.
    fn binding(&self) -> Option<ModelBinding> {
        None
    }''')
replace("crates/zhir-core/src/run/mod.rs", "pub struct SessionSnapshot {", '''pub struct SessionSnapshot {
    pub binding: Option<crate::model::ModelBinding>,''')

replace("crates/zhir-kernel/src/engine/session.rs", "let open = SessionOpen {\n        binding: None,", "let open = SessionOpen {\n        binding: self.current.active.session.binding.clone(),")
replace("crates/zhir-kernel/src/engine/session.rs",
    "        next.active.session.capabilities = Some(session.input.capabilities().clone());",
    '''        let binding = session.input.binding();
        if next.active.session.binding.is_some() && next.active.session.binding != binding {
            return Err(Error::Protocol("model binding changed during recovery".into()));
        }
        next.active.session.binding = binding;
        if next.options.mode == RunMode::Task
            && !session.input.capabilities().supports(Capability::ResponseEvents)
        {
            return Err(Error::Invalid("bound model has no verifiable response boundaries".into()));
        }
        next.active.session.capabilities = Some(session.input.capabilities().clone());''')
replace("crates/zhir-kernel/src/engine/lifecycle.rs", '''    fn input_finished(&self) -> bool {
        (self.current.active.session.response_status == Some(ResponseStatus::Completed)
            || !self
                .session_capabilities()
                .supports(Capability::ResponseEvents))
            && (self.current.options.mode == RunMode::Task
                || self.current.active.session.input_closed)
    }''', '''    fn input_finished(&self) -> bool {
        let session = &self.current.active.session;
        let response_finished = !self.session_capabilities().supports(Capability::ResponseEvents)
            || (session.response_status == Some(ResponseStatus::Completed)
                && !session.needs_generation
                && session.generated_input_position == session.input_position);
        response_finished
            && (self.current.options.mode == RunMode::Task || session.input_closed)
    }''')
replace("crates/zhir-core/src/run/validation.rs", "        self.state.validate()?;", '''        self.state.validate()?;
        if self.active.session.binding.as_ref().is_some_and(|binding| binding.adapter.is_empty()) {
            return Err(Error::Invalid("empty model binding adapter".into()));
        }
        if matches!(self.state, State::Completed { .. })
            && self.active.session.capabilities.as_ref().is_some_and(|caps| {
                caps.supports(crate::model::Capability::ResponseEvents)
            })
            && (self.active.session.response_status != Some(crate::model::ResponseStatus::Completed)
                || self.active.session.needs_generation
                || self.active.session.generated_input_position != self.active.session.input_position)
        {
            return Err(Error::Invalid("completed run lacks current response coverage".into()));
        }''')

# Preserve binding through transparent sender decorators (not through unrelated Models).
forwarded = []
for root in [ROOT / "crates/zhir-models", ROOT / "crates/zhir-testing"]:
    for p in root.rglob("*.rs"):
        text = p.read_text()
        def forward(m):
            block = m.group(0)
            if "self.inner.capabilities()" not in block or "fn binding(" in block:
                return block
            forwarded.append(str(p.relative_to(ROOT)))
            return block.replace("{\n", '''{
    fn binding(&self) -> Option<zhir_core::model::ModelBinding> {
        self.inner.binding()
    }
''', 1)
        edited = re.sub(r"^impl (?:[A-Za-z_][A-Za-z0-9_]*::)*SessionSender for [^{]+\{.*?^}", forward, text, flags=re.M | re.S)
        if edited != text:
            p.write_text(edited)
assert len(forwarded) >= 3, forwarded
print("Binding forwarding:", forwarded)

path = "crates/zhir-models/src/decorators.rs"
replace(path, "struct BoundEvents {", '''#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FallbackBinding {
    candidate: String,
    inner: Option<ModelBinding>,
}
struct BoundInput {
    candidate: String,
    inner: Arc<dyn SessionSender>,
}
impl SessionSender for BoundInput {
    fn binding(&self) -> Option<ModelBinding> {
        Some(ModelBinding {
            adapter: "zhir.fallback".into(),
            data: serde_json::json!({"candidate": self.candidate, "inner": self.inner.binding()}),
        })
    }
    fn capabilities(&self) -> &CapabilitySet { self.inner.capabilities() }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn send(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        self.inner.send(command)
    }
}
struct BoundEvents {''')
replace(path, '''fn bind(mut session: ModelSession, candidate: &str) -> ModelSession {
    session.output''', '''fn bind(mut session: ModelSession, candidate: &str) -> ModelSession {
    session.input = Arc::new(BoundInput { candidate: candidate.into(), inner: session.input });
    session.output''')
p = ROOT / path
text = p.read_text()
start = text.index("            if let Some(reference) = open.recovery.take() {")
end = text.index('            let mut error = Error::Invalid("no model satisfies request".into());', start)
text = text[:start] + '''            let binding: Option<FallbackBinding> = open.binding.take().map(|binding| {
                if binding.adapter != "zhir.fallback" {
                    return Err(Error::Invalid("binding does not belong to this fallback adapter".into()));
                }
                serde_json::from_value(binding.data).map_err(|error| Error::Invalid(error.to_string()))
            }).transpose()?;
            let recovery: Option<FallbackRecovery> = open.recovery.take().map(|reference| {
                if reference.adapter != "zhir.fallback" {
                    return Err(Error::Invalid("recovery does not belong to this fallback adapter".into()));
                }
                serde_json::from_value(reference.data).map_err(|error| Error::Invalid(error.to_string()))
            }).transpose()?;
            if binding.is_some() || recovery.is_some() {
                if let (Some(binding), Some(recovery)) = (&binding, &recovery)
                    && binding.candidate != recovery.candidate
                {
                    return Err(Error::Invalid("recovery differs from the bound candidate".into()));
                }
                let id = binding.as_ref().map(|saved| &saved.candidate)
                    .or_else(|| recovery.as_ref().map(|saved| &saved.candidate))
                    .expect("binding or recovery");
                let candidate = self.models.iter().find(|candidate| &candidate.id == id)
                    .ok_or_else(|| Error::Invalid("original fallback candidate is unavailable".into()))?;
                open.binding = binding.and_then(|saved| saved.inner);
                open.recovery = recovery.map(|saved| saved.reference);
                return candidate.model.open_session(open).await.map(|session| bind(session, &candidate.id));
            }
''' + text[end:]
p.write_text(text)

# Native factories have no intrinsic binding layer; wrappers must unwrap before I/O.
for path in ["crates/zhir-models/src/websocket.rs", "crates/zhir-models/src/webrtc.rs"]:
    p = ROOT / path
    text = p.read_text()
    start = text.index("fn open_session(")
    body = text.index("Box::pin(async move {", start) + len("Box::pin(async move {")
    text = text[:body] + '''
            if open.binding.is_some() {
                return Err(Error::Invalid("binding does not belong to this transport adapter".into()));
            }
''' + text[body:]
    p.write_text(text)

# Schemas are a development-time contract dependency, never a production requirement.
p = ROOT / "crates/zhir-testing/Cargo.toml"
text = p.read_text()
a, b = text.split("[dev-dependencies]", 1)
assert "zhir-core.workspace = true" in b
b = b.replace("zhir-core.workspace = true", 'zhir-core = { workspace = true, features = ["schema"] }', 1)
p.write_text(a + "[dev-dependencies]" + b)

(ROOT / "crates/zhir-models/src/session.rs").write_text((ROOT / ".github/v4-review/session.rs").read_text())
(ROOT / "crates/zhir-testing/tests/v4_review.rs").write_text((ROOT / ".github/v4-review/review_tests.rs").read_text())
p = ROOT / "contracts/v4/behavior/runtime.md"
p.write_text(p.read_text() + '''
36. Response-capable sessions may complete only when the latest verified response
    covers the current input position and no generation remains required. This applies
    equally to explicit and automatic generation; a submitted-input acknowledgement
    cannot settle that input. Checkpoint validation enforces the same invariant.
37. ModelBinding persists adapter selection separately from RecoveryRef. Local
    projection reconstruction must reopen that binding, even after fallback candidates
    recover or change order. Missing or conflicting bindings fail rather than select
    another model. Transparent sender decorators preserve the binding unchanged.
38. Exchange command acknowledgements, generated deltas and final output are polled
    by one scheduler. A blocked emission cannot stop the generation or cancellation
    needed to release its capacity. Control staging is bounded; terminal settlement
    uses an independent port and never waits for output consumption.
''')
# Temporary preparation files do not belong in the resulting source tree. The
# workflow itself is removed through the connected GitHub writer after CI completes.
shutil.rmtree(ROOT / ".github/v4-review")
print("Applied v4 review fixes and regression tests. Regenerate schemas and run Rust checks next.")
