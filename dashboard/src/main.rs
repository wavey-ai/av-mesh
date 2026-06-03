use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

use gloo_net::http::Request;
use gloo_timers::callback::Interval;
use leptos::{mount::mount_to_body, prelude::*};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use wasm_bindgen_futures::spawn_local;
use web_sys::{Event, EventSource, MessageEvent};

const DEFAULT_MESH_API: &str = "https://local.bitneedle.com:19444/api/mesh";
const DEFAULT_CONTRIB_API: &str = "https://local.bitneedle.com:19443/api/status";
const DASHBOARD_FEED_MISSING_GRACE_MS: u64 = 5_000;
const DASHBOARD_SNAPSHOT_STALE_MS: u64 = 10_000;

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(App);
}

#[component]
fn App() -> impl IntoView {
    let dashboard_started_unix_ms = now_unix_ms();
    let (mesh_api, set_mesh_api) = signal(endpoint_from_query("mesh", DEFAULT_MESH_API));
    let (contrib_api, set_contrib_api) =
        signal(endpoint_from_query("contrib", DEFAULT_CONTRIB_API));
    let (mesh, set_mesh) = signal(None::<MeshApiSnapshot>);
    let (contrib, set_contrib) = signal(None::<ContribStatus>);
    let (mesh_rates, set_mesh_rates) = signal(MeshRateSnapshot::default());
    let (contrib_rates, set_contrib_rates) = signal(ContribRateSnapshot::default());
    let (playback_probes, set_playback_probes) = signal(PlaybackProbeState::default());
    let (last_mesh_sample, set_last_mesh_sample) = signal(None::<MeshRateSample>);
    let (last_contrib_sample, set_last_contrib_sample) = signal(None::<ContribRateSample>);
    let (status, set_status) = signal(String::from("starting"));
    let (mesh_feed, set_mesh_feed) = signal(String::from("mesh feed starting"));
    let (mesh_events_active, set_mesh_events_active) = signal(false);
    let (mesh_feed_diag, set_mesh_feed_diag) = signal(FeedDiagnostics::new("mesh"));
    let (contrib_feed, set_contrib_feed) = signal(String::from("contrib feed starting"));
    let (contrib_events_active, set_contrib_events_active) = signal(false);
    let (contrib_feed_diag, set_contrib_feed_diag) = signal(FeedDiagnostics::new("contrib"));
    let (control_status, set_control_status) = signal(String::from("idle"));
    let (stream_id, set_stream_id) = signal(String::from("1"));
    let (region, set_region) = signal(String::new());
    let (node_id, set_node_id) = signal(String::new());
    let mesh_event_handle = Rc::new(RefCell::new(None::<DashboardEventHandle>));
    let contrib_event_handle = Rc::new(RefCell::new(None::<DashboardEventHandle>));

    let refresh = move || {
        let mesh_url = mesh_api.get();
        let contrib_url = contrib_api.get();
        let poll_mesh = !mesh_events_active.get();
        let poll_contrib = !contrib_events_active.get();
        let mesh_feed_mode = if poll_mesh { "polling" } else { "events" };
        let contrib_feed_mode = if poll_contrib { "polling" } else { "events" };
        set_status.set(format!(
            "refreshing / mesh {mesh_feed_mode} / contrib {contrib_feed_mode}"
        ));
        if poll_mesh {
            set_mesh_feed_diag.update(|diag| diag.record_polling(&mesh_url));
        }
        if poll_contrib {
            set_contrib_feed_diag.update(|diag| diag.record_polling(&contrib_url));
        }
        spawn_local(async move {
            let mesh_result = if poll_mesh {
                Some(fetch_json::<MeshApiSnapshot>(&mesh_url).await)
            } else {
                None
            };
            let contrib_result = if poll_contrib {
                Some(fetch_json::<ContribStatus>(&contrib_url).await)
            } else {
                None
            };

            let mut errors = Vec::new();
            match mesh_result {
                Some(Ok(mesh_snapshot)) => {
                    accept_mesh_snapshot(
                        mesh_snapshot,
                        last_mesh_sample,
                        set_last_mesh_sample,
                        set_mesh_rates,
                        set_mesh,
                    );
                    set_mesh_feed.set(format!("mesh polling {}", short_clock()));
                    set_mesh_feed_diag.update(FeedDiagnostics::record_poll_ok);
                }
                Some(Err(error)) => {
                    set_mesh_feed_diag.update(|diag| diag.record_poll_error(error.as_str()));
                    errors.push(format!("mesh: {error}"));
                }
                None => {}
            }
            match contrib_result {
                Some(Ok(contrib_status)) => {
                    accept_contrib_snapshot(
                        contrib_status,
                        last_contrib_sample,
                        set_last_contrib_sample,
                        set_contrib_rates,
                        set_contrib,
                    );
                    set_contrib_feed.set(format!("contrib polling {}", short_clock()));
                    set_contrib_feed_diag.update(FeedDiagnostics::record_poll_ok);
                }
                Some(Err(error)) => {
                    set_contrib_feed_diag.update(|diag| diag.record_poll_error(error.as_str()));
                    errors.push(format!("contrib: {error}"));
                }
                None => {}
            }

            if errors.is_empty() {
                set_status.set(format!(
                    "ok {} / mesh {} / contrib {}",
                    short_clock(),
                    if poll_mesh { "polling" } else { "events" },
                    if poll_contrib { "polling" } else { "events" }
                ));
            } else {
                set_status.set(errors.join(" | "));
            }
            let probe_targets = playlist_probe_targets(
                mesh.get().as_ref(),
                contrib.get().as_ref(),
                &mesh_url,
                &contrib_url,
            );
            schedule_playlist_probes(probe_targets, set_playback_probes);
        });
    };

    let connect_mesh_events = {
        let mesh_event_handle = mesh_event_handle.clone();
        Rc::new(move || {
            if let Some(handle) = mesh_event_handle.borrow_mut().take() {
                handle.close();
            }

            let events_url = mesh_events_url(&mesh_api.get());
            set_last_mesh_sample.set(None);
            set_mesh_rates.set(MeshRateSnapshot::default());
            set_mesh_events_active.set(false);
            set_mesh_feed.set(format!("mesh events connecting {}", short_clock()));
            set_mesh_feed_diag.update(|diag| diag.record_event_connecting(&events_url));

            let source = match EventSource::new(&events_url) {
                Ok(source) => source,
                Err(error) => {
                    let error = js_error_text(error);
                    set_mesh_feed_diag.update(|diag| diag.record_event_error(&error));
                    set_mesh_feed.set(format!("mesh polling: {error}"));
                    return;
                }
            };

            let event_url = events_url.clone();
            let onmesh =
                Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |event: MessageEvent| {
                    let Some(data) = event.data().as_string() else {
                        set_mesh_events_active.set(false);
                        set_mesh_feed.set("mesh events: non-text payload".to_owned());
                        set_mesh_feed_diag
                            .update(|diag| diag.record_parse_error("non-text event payload"));
                        return;
                    };
                    match serde_json::from_str::<MeshApiSnapshot>(&data) {
                        Ok(snapshot) => {
                            accept_mesh_snapshot(
                                snapshot,
                                last_mesh_sample,
                                set_last_mesh_sample,
                                set_mesh_rates,
                                set_mesh,
                            );
                            set_mesh_events_active.set(true);
                            set_mesh_feed.set(format!("mesh events {}", short_clock()));
                            set_status.set(format!("ok {} / mesh events", short_clock()));
                            set_mesh_feed_diag.update(FeedDiagnostics::record_event_ok);
                        }
                        Err(error) => {
                            set_mesh_events_active.set(false);
                            set_mesh_feed.set(format!("mesh events parse error: {error}"));
                            set_mesh_feed_diag
                                .update(|diag| diag.record_parse_error(error.to_string()));
                        }
                    }
                }));

            if let Err(error) =
                source.add_event_listener_with_callback("mesh", onmesh.as_ref().unchecked_ref())
            {
                source.close();
                let error = js_error_text(error);
                set_mesh_feed_diag.update(|diag| diag.record_event_error(&error));
                set_mesh_feed.set(format!("mesh polling: {error}"));
                return;
            }

            let onerror = Closure::<dyn FnMut(Event)>::wrap(Box::new(move |_event: Event| {
                set_mesh_events_active.set(false);
                set_mesh_feed_diag.update(|diag| diag.record_event_reconnect(&event_url));
                set_mesh_feed.set(format!(
                    "mesh events reconnecting {} ({event_url})",
                    short_clock()
                ));
            }));
            source.set_onerror(Some(onerror.as_ref().unchecked_ref()));

            *mesh_event_handle.borrow_mut() = Some(DashboardEventHandle {
                source,
                _onmessage: onmesh,
                _onerror: onerror,
            });
        })
    };

    let connect_contrib_events = {
        let contrib_event_handle = contrib_event_handle.clone();
        Rc::new(move || {
            if let Some(handle) = contrib_event_handle.borrow_mut().take() {
                handle.close();
            }

            let events_url = contrib_events_url(&contrib_api.get());
            set_last_contrib_sample.set(None);
            set_contrib_rates.set(ContribRateSnapshot::default());
            set_contrib_events_active.set(false);
            set_contrib_feed.set(format!("contrib events connecting {}", short_clock()));
            set_contrib_feed_diag.update(|diag| diag.record_event_connecting(&events_url));

            let source = match EventSource::new(&events_url) {
                Ok(source) => source,
                Err(error) => {
                    let error = js_error_text(error);
                    set_contrib_feed_diag.update(|diag| diag.record_event_error(&error));
                    set_contrib_feed.set(format!("contrib polling: {error}"));
                    return;
                }
            };

            let event_url = events_url.clone();
            let oncontrib =
                Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |event: MessageEvent| {
                    let Some(data) = event.data().as_string() else {
                        set_contrib_events_active.set(false);
                        set_contrib_feed.set("contrib events: non-text payload".to_owned());
                        set_contrib_feed_diag
                            .update(|diag| diag.record_parse_error("non-text event payload"));
                        return;
                    };
                    match serde_json::from_str::<ContribStatus>(&data) {
                        Ok(snapshot) => {
                            accept_contrib_snapshot(
                                snapshot,
                                last_contrib_sample,
                                set_last_contrib_sample,
                                set_contrib_rates,
                                set_contrib,
                            );
                            set_contrib_events_active.set(true);
                            set_contrib_feed.set(format!("contrib events {}", short_clock()));
                            set_status.set(format!("ok {} / contrib events", short_clock()));
                            set_contrib_feed_diag.update(FeedDiagnostics::record_event_ok);
                        }
                        Err(error) => {
                            set_contrib_events_active.set(false);
                            set_contrib_feed.set(format!("contrib events parse error: {error}"));
                            set_contrib_feed_diag
                                .update(|diag| diag.record_parse_error(error.to_string()));
                        }
                    }
                }));

            if let Err(error) = source
                .add_event_listener_with_callback("contrib", oncontrib.as_ref().unchecked_ref())
            {
                source.close();
                let error = js_error_text(error);
                set_contrib_feed_diag.update(|diag| diag.record_event_error(&error));
                set_contrib_feed.set(format!("contrib polling: {error}"));
                return;
            }

            let onerror = Closure::<dyn FnMut(Event)>::wrap(Box::new(move |_event: Event| {
                set_contrib_events_active.set(false);
                set_contrib_feed_diag.update(|diag| diag.record_event_reconnect(&event_url));
                set_contrib_feed.set(format!(
                    "contrib events reconnecting {} ({event_url})",
                    short_clock()
                ));
            }));
            source.set_onerror(Some(onerror.as_ref().unchecked_ref()));

            *contrib_event_handle.borrow_mut() = Some(DashboardEventHandle {
                source,
                _onmessage: oncontrib,
                _onerror: onerror,
            });
        })
    };

    connect_mesh_events();
    connect_contrib_events();
    refresh();
    Interval::new(2_000, refresh).forget();

    let send_control = move |action: &'static str, body: Value| {
        let mesh_url = mesh_api.get();
        set_control_status.set(format!("{action} pending"));
        spawn_local(async move {
            let endpoint = mesh_control_url(&mesh_url, action);
            match post_json::<ControlCommand>(&endpoint, &body).await {
                Ok(command) => {
                    let status = control_status_text(action, &command);
                    set_mesh.update(move |snapshot| {
                        if let Some(snapshot) = snapshot {
                            upsert_recent_command(&mut snapshot.recent_commands, command.clone());
                        }
                    });
                    set_control_status.set(status);
                }
                Err(error) => set_control_status.set(format!("{action} failed: {error}")),
            }
        });
    };

    view! {
        <div class="shell">
            <header class="topbar">
                <div class="brand">
                    <img class="brand-icon" src="/assets/wavey-goose.png" alt="" />
                    <div>
                        <h1>"av mission control"</h1>
                        <p>{move || format!("{} / {} / {}", status.get(), mesh_feed.get(), contrib_feed.get())}</p>
                    </div>
                </div>
                <div class="endpoint-grid">
                    <label>
                        <span>"mesh"</span>
                        <input
                            prop:value=move || mesh_api.get()
                            on:input=move |event| set_mesh_api.set(event_target_value(&event))
                        />
                    </label>
                    <label>
                        <span>"contrib"</span>
                        <input
                            prop:value=move || contrib_api.get()
                            on:input=move |event| set_contrib_api.set(event_target_value(&event))
                        />
                    </label>
                    <button class="primary" on:click={
                        let connect_mesh_events = connect_mesh_events.clone();
                        let connect_contrib_events = connect_contrib_events.clone();
                        move |_| {
                            connect_mesh_events();
                            connect_contrib_events();
                            refresh();
                        }
                    }>"Refresh"</button>
                </div>
            </header>

            <main>
                <section class="band metrics">
                    <Metric label="nodes" value=move || mesh.get().map(|m| m.aggregate.node_count.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || mesh.get().map(|m| format!("{} links / {} alerts / local {}", m.aggregate.connection_count, m.alerts.len(), m.node.node_id)).unwrap_or_default() />
                    <Metric label="storage" value=move || mesh.get().map(|m| format_bytes(m.aggregate.used_storage_bytes)).unwrap_or_else(|| "-".to_owned()) detail=move || mesh.get().map(|m| format!("of {}", format_bytes(m.aggregate.total_storage_bytes))).unwrap_or_default() />
                    <Metric label="egress" value=move || mesh.get().map(|m| format_bps(m.aggregate.total_egress_capacity_bps)).unwrap_or_else(|| "-".to_owned()) detail=move || mesh.get().map(|m| format!("{} ingress / {} active", m.aggregate.contributor_streams, m.aggregate.active_streams)).unwrap_or_default() />
                    <Metric label="ingest" value=move || contrib.get().map(|c| c.runtime.fmp4.parts.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} listeners / {} publish errors / {}", enabled_listener_count(&c), c.runtime.fmp4.publish_errors, c.health.state)).unwrap_or_default() />
                    <Metric label="mesh rx" value=move || mesh_rates.get().byte_rate_text() detail=move || mesh_rates.get().detail_text() />
                    <Metric label="contrib out" value=move || contrib_rates.get().output_rate_text() detail=move || contrib_rates.get().detail_text() />
                    <Metric label="playback" value=move || playback_probes.get().summary_text() detail=move || playback_probes.get().detail_text() />
                    <Metric
                        label="incidents"
                        value=move || {
                            let mesh_snapshot = mesh.get();
                            let contrib_status = contrib.get();
                            let probes = playback_probes.get();
                            let feed = DashboardFeedHealth::new(
                                dashboard_started_unix_ms,
                                mesh_events_active.get(),
                                contrib_events_active.get(),
                            );
                            incident_count_text(&mesh_snapshot, &contrib_status, &probes, feed)
                        }
                        detail=move || {
                            let mesh_snapshot = mesh.get();
                            let contrib_status = contrib.get();
                            let probes = playback_probes.get();
                            let feed = DashboardFeedHealth::new(
                                dashboard_started_unix_ms,
                                mesh_events_active.get(),
                                contrib_events_active.get(),
                            );
                            incident_detail_text(&mesh_snapshot, &contrib_status, &probes, feed)
                        }
                    />
                </section>

                <IncidentRollup
                    mesh
                    contrib
                    probes=playback_probes
                    started_unix_ms=dashboard_started_unix_ms
                    mesh_events_active
                    contrib_events_active
                />

                <PipelineReadiness
                    mesh
                    contrib
                    probes=playback_probes
                    started_unix_ms=dashboard_started_unix_ms
                    mesh_events_active
                    contrib_events_active
                />

                <DataHoseDiagnostics
                    mesh_diag=mesh_feed_diag
                    contrib_diag=contrib_feed_diag
                    mesh_events_active
                    contrib_events_active
                />

                <div class="workspace">
                    <section class="panel map-panel">
                        <div class="panel-head">
                            <h2>"Topology"</h2>
                            <span>{move || mesh.get().map(|m| format!("updated {} / {} peers / {} links / {} alerts", age_text(m.updated_unix_ms), m.peers.len(), m.connections.len(), m.alerts.len())).unwrap_or_else(|| "waiting".to_owned())}</span>
                        </div>
                        <MeshAlertList mesh />
                        <TopologyTelemetryHealth mesh />
                        <TopologyGraph mesh />
                        <div class="node-map">
                            <For
                                each=move || mesh.get().map(|m| m.nodes).unwrap_or_default()
                                key=|node| node.node_id.clone()
                                let(node)
                            >
                                <NodeTile node rates=mesh_rates />
                            </For>
                        </div>
                        <PeerList mesh />
                        <ConnectionList mesh />
                    </section>

                    <section class="panel">
                        <div class="panel-head">
                            <h2>"Contributor"</h2>
                            <span>{move || contrib.get().map(|c| c.status).unwrap_or_else(|| "waiting".to_owned())}</span>
                        </div>
                        <ContribView contrib rates=contrib_rates />
                    </section>
                </div>

                <div class="workspace lower">
                    <section class="panel">
                        <div class="panel-head">
                            <h2>"Streams"</h2>
                            <span>{move || mesh.get().map(|m| format!("{} observed / {} planned", m.streams.len(), m.planned_replicas.len())).unwrap_or_else(|| "0 observed".to_owned())}</span>
                        </div>
                        <PlaybackProbeList probes=playback_probes />
                        <LocalStream mesh />
                        <StreamTable mesh rates=mesh_rates />
                        <ReplicaPlan mesh />
                    </section>

                    <section class="panel">
                        <div class="panel-head">
                            <h2>"Controls"</h2>
                            <span>{move || control_status.get()}</span>
                        </div>
                        <OrchestrationView mesh rates=mesh_rates />
                        <div class="control-grid">
                            <label>
                                <span>"stream"</span>
                                <input prop:value=move || stream_id.get() on:input=move |event| set_stream_id.set(event_target_value(&event)) />
                            </label>
                            <label>
                                <span>"region"</span>
                                <input prop:value=move || region.get() on:input=move |event| set_region.set(event_target_value(&event)) />
                            </label>
                            <label>
                                <span>"node"</span>
                                <input prop:value=move || node_id.get() on:input=move |event| set_node_id.set(event_target_value(&event)) />
                            </label>
                            <button on:click=move |_| {
                                send_control("warm-stream", json!({
                                    "stream_id": stream_id.get(),
                                    "region": optional_text(region.get())
                                }));
                            }>"Warm stream"</button>
                            <button on:click=move |_| {
                                send_control("provision-node", json!({
                                    "node_id": optional_text(node_id.get()),
                                    "region": optional_text(region.get())
                                }));
                            }>"Provision"</button>
                            <button class="danger" on:click=move |_| {
                                send_control("close-node", json!({
                                    "node_id": optional_text(node_id.get()),
                                    "region": optional_text(region.get())
                                }));
                            }>"Close node"</button>
                        </div>
                        <ControlTargetPreview mesh stream_id region node_id />
                        <CommandList mesh />
                    </section>
                </div>

                <div class="workspace activity-workspace">
                    <section class="panel">
                        <div class="panel-head">
                            <h2>"Mesh Activity"</h2>
                            <span>{move || mesh.get().map(|m| format!("{} events", m.activity.len())).unwrap_or_else(|| "0 events".to_owned())}</span>
                        </div>
                        <MeshActivityList mesh />
                    </section>

                    <section class="panel">
                        <div class="panel-head">
                            <h2>"Contrib Activity"</h2>
                            <span>{move || contrib.get().map(|c| format!("{} events", c.activity.len())).unwrap_or_else(|| "0 events".to_owned())}</span>
                        </div>
                        <ContribActivityList contrib />
                    </section>
                </div>

                <section class="panel">
                    <div class="panel-head">
                        <h2>"Edges"</h2>
                        <span>{move || mesh.get().map(|m| format!("{} services", m.edge_services.len())).unwrap_or_else(|| "0 services".to_owned())}</span>
                    </div>
                    <EdgeGrid mesh rates=mesh_rates />
                </section>
            </main>
        </div>
    }
}

#[component]
fn IncidentRollup(
    mesh: ReadSignal<Option<MeshApiSnapshot>>,
    contrib: ReadSignal<Option<ContribStatus>>,
    probes: ReadSignal<PlaybackProbeState>,
    started_unix_ms: u64,
    mesh_events_active: ReadSignal<bool>,
    contrib_events_active: ReadSignal<bool>,
) -> impl IntoView {
    view! {
        <section class="band incident-rollup">
            <div class="incident-head">
                <h2>"Incidents"</h2>
                <span>{move || {
                    let mesh_snapshot = mesh.get();
                    let contrib_status = contrib.get();
                    let probes = probes.get();
                    let feed = DashboardFeedHealth::new(
                        started_unix_ms,
                        mesh_events_active.get(),
                        contrib_events_active.get(),
                    );
                    incident_detail_text(&mesh_snapshot, &contrib_status, &probes, feed)
                }}</span>
            </div>
            <div class="incident-list">
                <For
                    each=move || {
                        let mesh_snapshot = mesh.get();
                        let contrib_status = contrib.get();
                        let probes = probes.get();
                        let feed = DashboardFeedHealth::new(
                            started_unix_ms,
                            mesh_events_active.get(),
                            contrib_events_active.get(),
                        );
                        build_incidents(&mesh_snapshot, &contrib_status, &probes, feed)
                            .into_iter()
                            .take(12)
                            .collect::<Vec<_>>()
                    }
                    key=|incident| incident.key()
                    let(incident)
                >
                    <div class=incident.class_name()>
                        <strong>{incident.source.clone()}</strong>
                        <span>{incident.code.clone()}</span>
                        <p>{incident.message.clone()}</p>
                        <small>{incident.meta_text()}</small>
                    </div>
                </For>
            </div>
            <Show when=move || {
                let mesh_snapshot = mesh.get();
                let contrib_status = contrib.get();
                let probes = probes.get();
                let feed = DashboardFeedHealth::new(
                    started_unix_ms,
                    mesh_events_active.get(),
                    contrib_events_active.get(),
                );
                build_incidents(&mesh_snapshot, &contrib_status, &probes, feed).is_empty()
            }>
                <p class="incident-empty">{move || {
                    let mesh_snapshot = mesh.get();
                    let contrib_status = contrib.get();
                    let probes = probes.get();
                    let feed = DashboardFeedHealth::new(
                        started_unix_ms,
                        mesh_events_active.get(),
                        contrib_events_active.get(),
                    );
                    incident_empty_text(&mesh_snapshot, &contrib_status, &probes, feed)
                }}</p>
            </Show>
        </section>
    }
}

#[component]
fn PipelineReadiness(
    mesh: ReadSignal<Option<MeshApiSnapshot>>,
    contrib: ReadSignal<Option<ContribStatus>>,
    probes: ReadSignal<PlaybackProbeState>,
    started_unix_ms: u64,
    mesh_events_active: ReadSignal<bool>,
    contrib_events_active: ReadSignal<bool>,
) -> impl IntoView {
    view! {
        <section class="band pipeline">
            <div class="pipeline-head">
                <h2>"Pipeline"</h2>
                <span>{move || {
                    let mesh_snapshot = mesh.get();
                    let contrib_status = contrib.get();
                    let probes = probes.get();
                    let feed = DashboardFeedHealth::new(
                        started_unix_ms,
                        mesh_events_active.get(),
                        contrib_events_active.get(),
                    );
                    pipeline_summary_text(&build_pipeline_stages(
                        &mesh_snapshot,
                        &contrib_status,
                        &probes,
                        feed,
                    ))
                }}</span>
            </div>
            <div class="pipeline-grid">
                <For
                    each=move || {
                        let mesh_snapshot = mesh.get();
                        let contrib_status = contrib.get();
                        let probes = probes.get();
                        let feed = DashboardFeedHealth::new(
                            started_unix_ms,
                            mesh_events_active.get(),
                            contrib_events_active.get(),
                        );
                        build_pipeline_stages(&mesh_snapshot, &contrib_status, &probes, feed)
                    }
                    key=|stage| stage.key
                    let(stage)
                >
                    {
                        let class_name = stage.class_name();
                        let title = stage.title;
                        let status = stage.status;
                        let detail = stage.detail;
                        view! {
                            <div class=class_name>
                                <strong>{title}</strong>
                                <span>{status}</span>
                                <small>{detail}</small>
                            </div>
                        }
                    }
                </For>
            </div>
        </section>
    }
}

#[component]
fn DataHoseDiagnostics(
    mesh_diag: ReadSignal<FeedDiagnostics>,
    contrib_diag: ReadSignal<FeedDiagnostics>,
    mesh_events_active: ReadSignal<bool>,
    contrib_events_active: ReadSignal<bool>,
) -> impl IntoView {
    view! {
        <section class="band dashboard-hoses">
            <div class="dashboard-hoses-head">
                <h2>"Dashboard Data Hoses"</h2>
                <span>{move || {
                    let mesh = mesh_diag.get();
                    let contrib = contrib_diag.get();
                    format!(
                        "{} / {}",
                        mesh.summary_text(mesh_events_active.get()),
                        contrib.summary_text(contrib_events_active.get())
                    )
                }}</span>
            </div>
            <div class="dashboard-hose-grid">
                <FeedDiagnosticCard diag=mesh_diag active=mesh_events_active />
                <FeedDiagnosticCard diag=contrib_diag active=contrib_events_active />
            </div>
        </section>
    }
}

#[component]
fn FeedDiagnosticCard(
    diag: ReadSignal<FeedDiagnostics>,
    active: ReadSignal<bool>,
) -> impl IntoView {
    view! {
        <div class=move || diag.get().class_name(active.get())>
            <strong>{move || diag.get().source}</strong>
            <span>{move || diag.get().summary_text(active.get())}</span>
            <small>{move || diag.get().detail_text()}</small>
        </div>
    }
}

#[component]
fn Metric<V, D>(label: &'static str, value: V, detail: D) -> impl IntoView
where
    V: Fn() -> String + Send + Sync + 'static,
    D: Fn() -> String + Send + Sync + 'static,
{
    view! {
        <article class="metric">
            <span>{label}</span>
            <strong>{value}</strong>
            <em>{detail}</em>
        </article>
    }
}

#[component]
fn NodeTile(node: MeshNode, rates: ReadSignal<MeshRateSnapshot>) -> impl IntoView {
    let storage_pct = percent(node.used_storage_bytes, node.total_storage_bytes);
    let class = if node.draining {
        "node draining"
    } else {
        "node"
    };
    let rate_node_id = node.node_id.clone();
    let active_streams = node.active_streams;
    view! {
        <article class=class>
            <div>
                <strong>{node.node_id.clone()}</strong>
                <span>{format!("{} / {}", node.region, node.continent)}</span>
            </div>
            <div class="node-stats">
                <span>{format!("{} active", node.active_streams)}</span>
                <span>{format!("{} ingress", node.contributor_streams)}</span>
                <span>{format_bps(node.egress_capacity_bps)}</span>
            </div>
            <div class="node-stats node-rate-stats">
                <span>{move || {
                    let rate = rates.get().nodes.get(&rate_node_id).copied();
                    node_rate_text(rate, active_streams)
                }}</span>
            </div>
            <div class="bar"><i style=format!("width: {:.1}%", storage_pct)></i></div>
            <small>{format!("{} used", format_bytes(node.used_storage_bytes))}</small>
        </article>
    }
}

fn node_rate_text(rate: Option<NodeRateSnapshot>, active_streams: u64) -> String {
    match rate {
        Some(rate) if rate.ready => format!(
            "{} / {} / {} streams / {}",
            format_bytes_per_sec(true, rate.bytes_per_sec),
            format_count_per_sec(rate.datagrams_per_sec, "datagrams"),
            rate.streams,
            format_rate_window(rate.window_ms)
        ),
        _ if active_streams == 0 => "no stream traffic".to_owned(),
        _ => "stream rate waiting".to_owned(),
    }
}

#[component]
fn ContribView(
    contrib: ReadSignal<Option<ContribStatus>>,
    rates: ReadSignal<ContribRateSnapshot>,
) -> impl IntoView {
    view! {
        <div class="contrib">
            <div class="kv">
                <span>"advertised hls"</span>
                <strong>{move || contrib.get().map(|c| format!("{} (stream {})", c.advertised_hls_path, c.advertised_hls_stream_id)).unwrap_or_else(|| "-".to_owned())}</strong>
            </div>
            <div class="kv">
                <span>"byte fec"</span>
                <strong>{move || contrib.get().map(|c| c.mesh.byte_fec_target).unwrap_or_else(|| "-".to_owned())}</strong>
            </div>
            <div class="kv">
                <span>"media fec"</span>
                <strong>{move || contrib.get().map(|c| c.mesh.media_fec_target).unwrap_or_else(|| "-".to_owned())}</strong>
            </div>
            <div class="contrib-config-grid">
                <RuntimeCell
                    label="ll-hls target"
                    value=move || contrib.get().map(|c| c.hls.part_target_text()).unwrap_or_else(|| "-".to_owned())
                    detail=move || contrib.get().map(|c| c.hls.playlist_detail_text()).unwrap_or_else(|| "configured LL-HLS cadence".to_owned())
                />
                <RuntimeCell
                    label="fec target"
                    value=move || contrib.get().map(|c| c.fec.repair_text()).unwrap_or_else(|| "-".to_owned())
                    detail=move || contrib.get().map(|c| c.fec.detail_text()).unwrap_or_else(|| "configured mesh FEC".to_owned())
                />
            </div>
            <div class="runtime-grid">
                <RuntimeCell label="health" value=move || contrib.get().map(|c| c.health.state).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| c.health.detail_text()).unwrap_or_default() />
                <RuntimeCell label="raw http" value=move || contrib.get().map(|c| c.runtime.raw_http.requests.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} / {} datagrams / {}", format_bytes(c.runtime.raw_http.bytes), c.runtime.raw_http.datagrams, optional_age(c.runtime.raw_http.last_seen_age_ms))).unwrap_or_default() />
                <RuntimeCell label="media au" value=move || contrib.get().map(|c| c.runtime.media_access_units.requests.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} / {} datagrams / {}", format_bytes(c.runtime.media_access_units.payload_bytes), c.runtime.media_access_units.datagrams, optional_age(c.runtime.media_access_units.last_seen_age_ms))).unwrap_or_default() />
                <RuntimeCell label="mesh tx" value=move || contrib.get().map(|c| c.runtime.mesh_forward.payloads().to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| c.runtime.mesh_forward.detail_text()).unwrap_or_default() />
                <RuntimeCell label="mpeg-ts" value=move || contrib.get().map(|c| c.runtime.mpeg_ts.slots.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| c.runtime.mpeg_ts.detail_text()).unwrap_or_default() />
                <RuntimeCell label="rtmp" value=move || contrib.get().map(|c| c.runtime.rtmp.access_units.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} / {}", format_bytes(c.runtime.rtmp.bytes), optional_age(c.runtime.rtmp.last_seen_age_ms))).unwrap_or_default() />
                <RuntimeCell label="fmp4" value=move || contrib.get().map(|c| c.runtime.fmp4.parts.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} media / {} init / {}", format_bytes(c.runtime.fmp4.bytes), format_bytes(c.runtime.fmp4.init_bytes), optional_age(c.runtime.fmp4.last_publish_age_ms))).unwrap_or_default() />
                <RuntimeCell label="tracks" value=move || contrib.get().map(|c| c.runtime.fmp4.track_summary()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| c.runtime.fmp4.track_detail()).unwrap_or_default() />
                <RuntimeCell label="hls" value=move || contrib.get().map(|c| c.runtime.hls.responses_total.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} errors / {} 404s / {}", c.runtime.hls.response_errors, c.runtime.hls.response_not_found, optional_age(c.runtime.hls.last_response_age_ms))).unwrap_or_default() />
                <RuntimeCell label="sessions" value=move || contrib.get().map(|c| c.runtime.ingest_sessions.active.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} started / {} ended", c.runtime.ingest_sessions.started, c.runtime.ingest_sessions.ended)).unwrap_or_default() />
                <RuntimeCell label="errors" value=move || contrib.get().map(|c| c.runtime.fmp4.publish_errors.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} alerts", c.alerts.len())).unwrap_or_default() />
            </div>
            <ContribHlsResponses contrib />
            <ContribStreamRuntime contrib />
            <ContribIngestSessions contrib />
            <ContribProtocolRuntime contrib rates />
            <div class="listener-list">
                <For
                    each=move || contrib.get().map(|c| c.listeners).unwrap_or_default()
                    key=|listener| listener.protocol.clone()
                    let(listener)
                >
                    <div class=if listener.enabled { "listener on" } else { "listener" }>
                        <strong>{listener.protocol}</strong>
                        <span>{listener.bind.unwrap_or_else(|| "disabled".to_owned())}</span>
                        <small>{format!("stream {}", listener.output_stream_id)}</small>
                    </div>
                </For>
            </div>
            <div class="alert-list">
                <For
                    each=move || contrib.get().map(|c| c.alerts).unwrap_or_default()
                    key=|alert| format!("{}:{}", alert.code, alert.message)
                    let(alert)
                >
                    <div class=format!("alert {}", alert.level)>
                        <strong>{alert.code}</strong>
                        <span>{alert.message}</span>
                        <small>{format!("{} seen / {}", alert.count, optional_unix_age(alert.last_seen_unix_ms))}</small>
                    </div>
                </For>
            </div>
        </div>
    }
}

#[component]
fn ContribProtocolRuntime(
    contrib: ReadSignal<Option<ContribStatus>>,
    rates: ReadSignal<ContribRateSnapshot>,
) -> impl IntoView {
    view! {
        <div class="protocol-list">
            <For
                each=move || contrib.get().map(|c| c.runtime.protocols).unwrap_or_default()
                key=|protocol| protocol.protocol.clone()
                let(protocol)
            >
                {
                    let class_name = protocol.class_name();
                    let protocol_label = protocol.protocol.clone();
                    let summary_protocol = protocol.clone();
                    let meta_protocol = protocol.clone();
                    view! {
                        <div class=class_name>
                            <strong>{protocol_label}</strong>
                            <span>{move || {
                                let rate = rates.get().protocols.get(&summary_protocol.protocol).copied();
                                format!(
                                    "{} / {}",
                                    summary_protocol.summary_text(),
                                    summary_protocol.rate_text(rate)
                                )
                            }}</span>
                            <small>{move || {
                                let rate = rates.get().protocols.get(&meta_protocol.protocol).copied();
                                meta_protocol.meta_text(rate)
                            }}</small>
                        </div>
                    }
                }
            </For>
        </div>
    }
}

#[component]
fn ContribStreamRuntime(contrib: ReadSignal<Option<ContribStatus>>) -> impl IntoView {
    view! {
        <Show when=move || {
            contrib
                .get()
                .map(|status| !status.runtime.streams.is_empty())
                .unwrap_or(false)
        }>
            <div class="table compact contrib-stream-table">
                <div class="table-head contrib-stream-row">
                    <span>"stream"</span><span>"state"</span><span>"input"</span><span>"mesh"</span><span>"fmp4"</span><span>"seen"</span>
                </div>
                <For
                    each=move || contrib.get().map(|c| c.runtime.streams).unwrap_or_default()
                    key=|stream| stream.stream_id_text.clone()
                    let(stream)
                >
                    <div class=stream.class_name()>
                        <span>{stream.display_stream_id()}</span>
                        <span>{stream.state.clone()}</span>
                        <span>{stream.input_text()}</span>
                        <span>{stream.mesh_text()}</span>
                        <span>{stream.fmp4_text()}</span>
                        <span>{stream.age_text()}</span>
                    </div>
                </For>
            </div>
        </Show>
    }
}

#[component]
fn ContribIngestSessions(contrib: ReadSignal<Option<ContribStatus>>) -> impl IntoView {
    view! {
        <div class="ingest-session-list">
            <For
                each=move || contrib.get().map(|c| c.runtime.ingest_sessions.recent).unwrap_or_default()
                key=|session| session.key()
                let(session)
            >
                <div class=session.class_name()>
                    <strong>{format!("{} {}", session.protocol, session.state)}</strong>
                    <span>{session.title_text()}</span>
                    <small>{session.meta_text()}</small>
                </div>
            </For>
        </div>
    }
}

#[component]
fn ContribHlsResponses(contrib: ReadSignal<Option<ContribStatus>>) -> impl IntoView {
    view! {
        <div class="hls-response-list">
            <For
                each=move || contrib.get().map(|c| c.runtime.hls.recent_responses).unwrap_or_default()
                key=|response| response.key()
                let(response)
            >
                <div class=response.class_name()>
                    <strong>{format!("{} {}", response.method, response.status)}</strong>
                    <span>{response.path_text()}</span>
                    <small>{response.meta_text()}</small>
                </div>
            </For>
        </div>
    }
}

#[component]
fn RuntimeCell<V, D>(label: &'static str, value: V, detail: D) -> impl IntoView
where
    V: Fn() -> String + Send + Sync + 'static,
    D: Fn() -> String + Send + Sync + 'static,
{
    view! {
        <div class="runtime-cell">
            <span>{label}</span>
            <strong>{value}</strong>
            <small>{detail}</small>
        </div>
    }
}

#[component]
fn PeerList(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="mini-list">
            <For
                each=move || mesh.get().map(|m| m.peers).unwrap_or_default()
                key=|peer| peer.addr.clone()
                let(peer)
            >
                <span>{format!("{} {}", peer.addr, peer.state)}</span>
            </For>
        </div>
    }
}

#[component]
fn MeshAlertList(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="alert-list mesh-alerts">
            <For
                each=move || mesh.get().map(|m| m.alerts).unwrap_or_default()
                key=|alert| format!("{}:{}", alert.code, alert.message)
                let(alert)
            >
                <div class=format!("alert {}", alert.level)>
                    <strong>{alert.code}</strong>
                    <span>{alert.message}</span>
                    <small>{format!("{} seen / {}", alert.count, optional_unix_age(alert.last_seen_unix_ms))}</small>
                </div>
            </For>
        </div>
    }
}

#[component]
fn TopologyTelemetryHealth(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="topology-health">
            <div class=move || {
                mesh.get()
                    .map(|snapshot| snapshot.telemetry.class_name())
                    .unwrap_or("topology-health-card waiting")
            }>
                <strong>"telemetry"</strong>
                <span>{move || mesh.get().map(|snapshot| snapshot.telemetry.summary_text()).unwrap_or_else(|| "waiting".to_owned())}</span>
                <small>{move || mesh.get().map(|snapshot| snapshot.telemetry.detail_text()).unwrap_or_else(|| "mesh topology telemetry unavailable".to_owned())}</small>
            </div>
            <For
                each=move || mesh.get().map(|snapshot| snapshot.telemetry.stale_nodes).unwrap_or_default()
                key=|node| node.node_id.clone()
                let(node)
            >
                <div class="topology-health-card warn">
                    <strong>{node.node_id.clone()}</strong>
                    <span>"stale"</span>
                    <small>{node.detail_text()}</small>
                </div>
            </For>
        </div>
    }
}

#[component]
fn TopologyGraph(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="topology-graph">
            <svg viewBox="0 0 720 260" role="img" aria-label="mesh topology graph">
                <For
                    each=move || build_topology_graph(mesh.get()).links
                    key=|link| link.key.clone()
                    let(link)
                >
                    <line
                        class=link.class_name()
                        x1=format!("{:.1}", link.x1)
                        y1=format!("{:.1}", link.y1)
                        x2=format!("{:.1}", link.x2)
                        y2=format!("{:.1}", link.y2)
                    />
                </For>
                <For
                    each=move || build_topology_graph(mesh.get()).nodes
                    key=|node| node.node_id.clone()
                    let(node)
                >
                    <g class=node.class_name() transform=format!("translate({:.1} {:.1})", node.x, node.y)>
                        <circle r="23"></circle>
                        <text class="topology-node-label" y="4">{node.short_label()}</text>
                        <text class="topology-node-detail" y="39">{node.detail_text()}</text>
                    </g>
                </For>
            </svg>
            <Show when=move || mesh.get().map(|m| m.nodes.is_empty()).unwrap_or(true)>
                <div class="topology-empty">"waiting for mesh topology"</div>
            </Show>
        </div>
    }
}

#[component]
fn MeshActivityList(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="activity-list">
            <For
                each=move || mesh.get().map(|m| m.activity).unwrap_or_default()
                key=|activity| activity.key()
                let(activity)
            >
                <ActivityRow activity />
            </For>
        </div>
    }
}

#[component]
fn ContribActivityList(contrib: ReadSignal<Option<ContribStatus>>) -> impl IntoView {
    view! {
        <div class="activity-list">
            <For
                each=move || contrib.get().map(|c| c.activity).unwrap_or_default()
                key=|activity| activity.key()
                let(activity)
            >
                <ActivityRow activity />
            </For>
        </div>
    }
}

#[component]
fn ActivityRow(activity: ActivityItem) -> impl IntoView {
    view! {
        <div class=format!("activity {}", activity.level.clone())>
            <strong>{activity.code.clone()}</strong>
            <span>{activity.message.clone()}</span>
            <small>{activity.meta_text()}</small>
        </div>
    }
}

#[component]
fn ConnectionList(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="table compact">
            <div class="table-head connection-row">
                <span>"source"</span><span>"target"</span><span>"state"</span>
            </div>
            <For
                each=move || mesh.get().map(|m| m.connections).unwrap_or_default()
                key=|connection| format!("{}:{}", connection.source_node_id, connection.target_addr)
                let(connection)
            >
                <div class="connection-row">
                    <span>{connection.source_node_id}</span>
                    <span>{connection.target_node_id.unwrap_or(connection.target_addr)}</span>
                    <span>{connection.state}</span>
                </div>
            </For>
        </div>
    }
}

#[component]
fn PlaybackProbeList(probes: ReadSignal<PlaybackProbeState>) -> impl IntoView {
    view! {
        <div class="playback-probe-list">
            <For
                each=move || probes.get().probes
                key=|probe| probe.url.clone()
                let(probe)
            >
                <div class=probe.class_name()>
                    <strong>{probe.label.clone()}</strong>
                    <span>{probe.status_text()}</span>
                    <small>{probe.meta_text()}</small>
                </div>
            </For>
        </div>
    }
}

#[component]
fn LocalStream(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="local-stream">
            <span>{move || mesh.get().map(|m| format!("local stream {}", m.stream.stream_id_text)).unwrap_or_else(|| "local stream -".to_owned())}</span>
            <strong>{move || mesh.get().map(|m| format_bytes(m.stream.bytes_received)).unwrap_or_else(|| "-".to_owned())}</strong>
            <em>{move || mesh.get().map(|m| format!(
                "local {} / mesh {} / {} datagrams / ingest {} / part {} / snapshot {}",
                optional_u64(m.stream.latest_local_part),
                optional_u64(m.stream.latest_mesh_part),
                m.stream.datagrams_received,
                optional_age(m.stream.last_ingest_age_ms),
                optional_age(m.stream.latest_local_part_age_ms),
                age_text(m.updated_unix_ms)
            )).unwrap_or_default()}</em>
        </div>
    }
}

#[component]
fn StreamTable(
    mesh: ReadSignal<Option<MeshApiSnapshot>>,
    rates: ReadSignal<MeshRateSnapshot>,
) -> impl IntoView {
    view! {
        <div class="table">
            <div class="table-head stream-row">
                <span>"stream"</span><span>"node"</span><span>"state"</span><span>"local"</span><span>"mesh"</span><span>"age"</span><span>"bytes"</span><span>"rate"</span>
            </div>
            <For
                each=move || mesh.get().map(|m| m.streams).unwrap_or_default()
                key=|stream| format!("{}:{}", stream.node_id, stream.stream_id_text)
                let(stream)
            >
                {
                    let class_name = stream.class_name();
                    let rate_key = stream.rate_key();
                    let rate_stream = stream.clone();
                    view! {
                        <div class=class_name>
                            <span>{stream.display_stream_id()}</span>
                            <span>{stream.node_id.clone()}</span>
                            <span>{stream.status_text()}</span>
                            <span>{optional_u64(stream.latest_local_part)}</span>
                            <span>{optional_u64(stream.latest_mesh_part)}</span>
                            <span>{stream.age_text()}</span>
                            <span>{format_bytes(stream.bytes_received)}</span>
                            <span>{move || {
                                let rate = rates.get().streams.get(&rate_key).copied();
                                rate_stream.rate_text(rate)
                            }}</span>
                        </div>
                    }
                }
            </For>
        </div>
    }
}

#[component]
fn ReplicaPlan(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="table compact">
            <div class="table-head plan-row">
                <span>"planned stream"</span><span>"target"</span><span>"score"</span>
            </div>
            <For
                each=move || mesh.get().map(|m| m.planned_replicas).unwrap_or_default()
                key=|replica| format!("{}:{}", replica.stream_id_text, replica.target_node_id)
                let(replica)
            >
                <div class="plan-row">
                    <span>{replica.stream_id_text}</span>
                    <span>{replica.target_node_id}</span>
                    <span>{format!("{:.2}", replica.score)}</span>
                </div>
            </For>
        </div>
    }
}

#[component]
fn OrchestrationView(
    mesh: ReadSignal<Option<MeshApiSnapshot>>,
    rates: ReadSignal<MeshRateSnapshot>,
) -> impl IntoView {
    view! {
        <div class="orchestration-grid">
            <RuntimeCell
                label="control bus"
                value=move || mesh.get().map(|m| {
                    if m.orchestration.control_dispatch_ready { "connected" } else { "local-only" }.to_owned()
                }).unwrap_or_else(|| "-".to_owned())
                detail=move || "AVMC command dispatch".to_owned()
            />
            <RuntimeCell
                label="provision"
                value=move || mesh.get().map(|m| {
                    if m.orchestration.provision.enabled { "enabled" } else { "disabled" }.to_owned()
                }).unwrap_or_else(|| "-".to_owned())
                detail=move || mesh.get().map(|m| {
                    if m.orchestration.provision.backends.is_empty() {
                        "no backend configured".to_owned()
                    } else {
                        m.orchestration.provision.backends.join(", ")
                    }
                }).unwrap_or_default()
            />
            <RuntimeCell
                label="timeout"
                value=move || mesh.get().map(|m| format!("{}ms", m.orchestration.provision.timeout_ms)).unwrap_or_else(|| "-".to_owned())
                detail=move || "provision command budget".to_owned()
            />
            <RuntimeCell
                label="data hoses"
                value=move || mesh.get().map(|m| {
                    let total = m.orchestration.telemetry_peers.len();
                    let connected = m.orchestration.telemetry_peers.iter().filter(|peer| peer.state == "connected").count();
                    format!("{connected}/{total}")
                }).unwrap_or_else(|| "-".to_owned())
                detail=move || mesh.get().map(|m| {
                    data_hose_detail_text(&m.orchestration.telemetry_peers, &rates.get())
                }).unwrap_or_else(|| "tcp-changes telemetry peers".to_owned())
            />
        </div>
        <ProvisionBackendList mesh />
        <ControlCommandHealth mesh />
        <TelemetryPeerList mesh rates />
    }
}

#[component]
fn ProvisionBackendList(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="provision-backend-list">
            <For
                each=move || mesh.get().map(|m| m.orchestration.provision.backend_statuses).unwrap_or_default()
                key=|backend| backend.name.clone()
                let(backend)
            >
                <div class=backend.class_name()>
                    <strong>{backend.name.clone()}</strong>
                    <span>{backend.state.clone()}</span>
                    <small>{backend.details.join(" / ")}</small>
                </div>
            </For>
        </div>
    }
}

#[component]
fn ControlCommandHealth(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="command-health-grid">
            <RuntimeCell
                label="commands"
                value=move || mesh.get().map(|m| m.recent_commands.len().to_string()).unwrap_or_else(|| "-".to_owned())
                detail=move || mesh.get().map(|m| command_health_detail(&m.recent_commands)).unwrap_or_else(|| "recent control actions".to_owned())
            />
            <RuntimeCell
                label="provision"
                value=move || mesh.get().map(|m| latest_command_status(&m.recent_commands, "provision_node")).unwrap_or_else(|| "-".to_owned())
                detail=move || mesh.get().map(|m| latest_command_meta(&m.recent_commands, "provision_node")).unwrap_or_else(|| "no provision commands".to_owned())
            />
        </div>
    }
}

#[component]
fn ControlTargetPreview(
    mesh: ReadSignal<Option<MeshApiSnapshot>>,
    stream_id: ReadSignal<String>,
    region: ReadSignal<String>,
    node_id: ReadSignal<String>,
) -> impl IntoView {
    view! {
        <div class="control-preview">
            <ControlPreviewCard
                title="warm"
                preview=move || {
                    let snapshot = mesh.get();
                    control_warm_preview(
                        snapshot.as_ref(),
                        &stream_id.get(),
                        &region.get(),
                        &node_id.get(),
                    )
                }
            />
            <ControlPreviewCard
                title="close"
                preview=move || {
                    let snapshot = mesh.get();
                    control_close_preview(snapshot.as_ref(), &region.get(), &node_id.get())
                }
            />
            <ControlPreviewCard
                title="provision"
                preview=move || {
                    let snapshot = mesh.get();
                    control_provision_preview(snapshot.as_ref(), &region.get(), &node_id.get())
                }
            />
        </div>
    }
}

#[component]
fn ControlPreviewCard<P>(title: &'static str, preview: P) -> impl IntoView
where
    P: Fn() -> ControlPreview + Send + Sync + 'static,
{
    let preview = Arc::new(preview);
    let class_preview = preview.clone();
    let summary_preview = preview.clone();
    let detail_preview = preview;
    view! {
        <div class=move || class_preview().class_name()>
            <strong>{title}</strong>
            <span>{move || summary_preview().summary}</span>
            <small>{move || detail_preview().detail}</small>
        </div>
    }
}

#[component]
fn TelemetryPeerList(
    mesh: ReadSignal<Option<MeshApiSnapshot>>,
    rates: ReadSignal<MeshRateSnapshot>,
) -> impl IntoView {
    view! {
        <div class="hose-list">
            <For
                each=move || mesh.get().map(|m| m.orchestration.telemetry_peers).unwrap_or_default()
                key=|peer| peer.peer.clone()
                let(peer)
            >
                {
                    let class_name = peer.class_name();
                    let peer_label = peer.peer.clone();
                    let peer_state = peer.state.clone();
                    let peer_key = peer.peer.clone();
                    let peer_for_meta = peer.clone();
                    view! {
                        <div class=class_name>
                            <strong>{peer_label}</strong>
                            <span>{peer_state}</span>
                            <small>{move || {
                                let rate = rates.get().telemetry_peers.get(&peer_key).copied();
                                peer_for_meta.meta_text(rate)
                            }}</small>
                        </div>
                    }
                }
            </For>
        </div>
    }
}

#[component]
fn CommandList(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="commands">
            <For
                each=move || mesh.get().map(|m| m.recent_commands).unwrap_or_default()
                key=|command| command.id
                let(command)
            >
                <div class=command.class_name()>
                    <strong>{command.kind_label()}</strong>
                    <span>{command.status_text()}</span>
                    <small>{command.meta_text()}</small>
                </div>
            </For>
        </div>
    }
}

#[component]
fn EdgeGrid(
    mesh: ReadSignal<Option<MeshApiSnapshot>>,
    rates: ReadSignal<MeshRateSnapshot>,
) -> impl IntoView {
    view! {
        <div class="edge-grid">
            <For
                each=move || mesh.get().map(|m| m.edge_services).unwrap_or_default()
                key=|edge| edge.node_id.clone()
                let(edge)
            >
                <EdgeCard edge rates />
            </For>
        </div>
    }
}

#[component]
fn EdgeCard(edge: EdgeServiceSnapshot, rates: ReadSignal<MeshRateSnapshot>) -> impl IntoView {
    let class = if edge.draining {
        "edge draining"
    } else if edge.response_errors > 0 {
        "edge warn"
    } else {
        "edge"
    };
    let node_id = edge.node_id.clone();
    let region = edge.region.clone();
    let continent = edge.continent.clone();
    let playback_base_url = edge
        .playback_base_url
        .clone()
        .unwrap_or_else(|| "no playback url".to_owned());
    let recent_responses = edge.recent_responses.clone();
    let traffic_rate_node_id = edge.node_id.clone();
    let tail_rate_node_id = edge.node_id.clone();
    let response_rate_node_id = edge.node_id.clone();
    let window_rate_node_id = edge.node_id.clone();
    view! {
        <article class=class>
            <div>
                <strong>{node_id}</strong>
                <span>{format!("{region} / {continent}")}</span>
            </div>
            <p>{playback_base_url}</p>
            <div class="edge-stats">
                <span>{format!("{} readers", edge.active_readers)}</span>
                <span>{format!("{} tail reads", edge.requests_served)}</span>
                <span>{format!("{} tails", edge.llhls_tail_requests)}</span>
                <span>{format!("{} served", format_bytes(edge.bytes_served))}</span>
            </div>
            <div class="edge-stats edge-rate-stats">
                <span>{move || {
                    let rate = rates.get().edges.get(&traffic_rate_node_id).copied();
                    edge_rate_traffic_text(rate)
                }}</span>
                <span>{move || {
                    let rate = rates.get().edges.get(&tail_rate_node_id).copied();
                    edge_rate_tail_text(rate)
                }}</span>
                <span>{move || {
                    let rate = rates.get().edges.get(&response_rate_node_id).copied();
                    edge_rate_response_text(rate)
                }}</span>
                <span>{move || {
                    let rate = rates.get().edges.get(&window_rate_node_id).copied();
                    edge_rate_window_text(rate)
                }}</span>
            </div>
            <div class="edge-stats edge-http-stats">
                <span>{format!("{} responses", edge.responses_total)}</span>
                <span>{format!("{} errors", edge.response_errors)}</span>
                <span>{format!("{} 404s", edge.response_not_found)}</span>
                <span>{format!("last {}", optional_unix_age(edge.last_response_unix_ms))}</span>
            </div>
            <div class="edge-response-list">
                <For
                    each=move || recent_responses.clone()
                    key=|response| response.key()
                    let(response)
                >
                    <div class=response.class_name()>
                        <strong>{format!("{} {}", response.method, response.status)}</strong>
                        <span>{response.path_text()}</span>
                        <small>{response.meta_text()}</small>
                    </div>
                </For>
            </div>
        </article>
    }
}

fn edge_rate_traffic_text(rate: Option<EdgeRateSnapshot>) -> String {
    match rate {
        Some(rate) if rate.ready => rate.traffic_text(),
        _ => "rate waiting".to_owned(),
    }
}

fn edge_rate_tail_text(rate: Option<EdgeRateSnapshot>) -> String {
    match rate {
        Some(rate) if rate.ready => rate.tail_text(),
        _ => "tails waiting".to_owned(),
    }
}

fn edge_rate_response_text(rate: Option<EdgeRateSnapshot>) -> String {
    match rate {
        Some(rate) if rate.ready => rate.response_text(),
        _ => "responses waiting".to_owned(),
    }
}

fn edge_rate_window_text(rate: Option<EdgeRateSnapshot>) -> String {
    match rate {
        Some(rate) if rate.ready => format_rate_window(rate.window_ms),
        _ => "window waiting".to_owned(),
    }
}

async fn fetch_json<T>(url: &str) -> Result<T, String>
where
    T: DeserializeOwned,
{
    let response = Request::get(url)
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.ok() {
        return Err(format!("HTTP {}", response.status()));
    }
    response
        .json::<T>()
        .await
        .map_err(|error| error.to_string())
}

async fn probe_playlist(target: PlaylistProbeTarget) -> PlaylistProbe {
    let started = js_sys::Date::now() as u64;
    let response = Request::get(&target.url)
        .header("Accept", "application/vnd.apple.mpegurl, */*")
        .header("Range", "bytes=0-0")
        .send()
        .await;
    let elapsed_ms = (js_sys::Date::now() as u64).saturating_sub(started);

    match response {
        Ok(response) => {
            let status = response.status();
            let headers = response.headers();
            PlaylistProbe {
                label: target.label,
                url: target.url,
                status: Some(status),
                ok: status == 200 || status == 206,
                elapsed_ms,
                content_length: headers
                    .get("content-length")
                    .and_then(|value| value.parse::<u64>().ok()),
                content_type: headers.get("content-type"),
                error: None,
            }
        }
        Err(error) => PlaylistProbe {
            label: target.label,
            url: target.url,
            status: None,
            ok: false,
            elapsed_ms,
            content_length: None,
            content_type: None,
            error: Some(error.to_string()),
        },
    }
}

fn schedule_playlist_probes(
    targets: Vec<PlaylistProbeTarget>,
    set_probes: WriteSignal<PlaybackProbeState>,
) {
    if targets.is_empty() {
        set_probes.set(PlaybackProbeState::default());
        return;
    }
    spawn_local(async move {
        let mut probes = Vec::with_capacity(targets.len());
        for target in targets {
            probes.push(probe_playlist(target).await);
        }
        set_probes.set(PlaybackProbeState {
            updated_unix_ms: js_sys::Date::now() as u64,
            probes,
        });
    });
}

async fn post_json<T>(url: &str, body: &Value) -> Result<T, String>
where
    T: DeserializeOwned,
{
    let response = Request::post(url)
        .header("Accept", "application/json")
        .json(body)
        .map_err(|error| error.to_string())?
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.ok() {
        return Err(format!("HTTP {}", response.status()));
    }
    response
        .json::<T>()
        .await
        .map_err(|error| error.to_string())
}

fn control_status_text(action: &str, command: &ControlCommand) -> String {
    let command_status = if command.status.is_empty() {
        "accepted"
    } else {
        command.status.as_str()
    };
    format!("{action} {command_status} {}", short_clock())
}

fn upsert_recent_command(commands: &mut Vec<ControlCommand>, command: ControlCommand) {
    if let Some(existing) = commands
        .iter_mut()
        .find(|existing| existing.id == command.id && command.id != 0)
    {
        *existing = command;
    } else {
        commands.insert(0, command);
    }
    commands.truncate(16);
}

struct DashboardEventHandle {
    source: EventSource,
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
    _onerror: Closure<dyn FnMut(Event)>,
}

impl DashboardEventHandle {
    fn close(self) {
        self.source.close();
    }
}

fn accept_mesh_snapshot(
    snapshot: MeshApiSnapshot,
    last_sample: ReadSignal<Option<MeshRateSample>>,
    set_last_sample: WriteSignal<Option<MeshRateSample>>,
    set_rates: WriteSignal<MeshRateSnapshot>,
    set_mesh: WriteSignal<Option<MeshApiSnapshot>>,
) {
    let sample = MeshRateSample::from_snapshot(&snapshot);
    if let Some(previous) = last_sample.get() {
        set_rates.set(MeshRateSnapshot::from_delta(previous, sample.clone()));
    }
    set_last_sample.set(Some(sample));
    set_mesh.set(Some(snapshot));
}

fn accept_contrib_snapshot(
    snapshot: ContribStatus,
    last_sample: ReadSignal<Option<ContribRateSample>>,
    set_last_sample: WriteSignal<Option<ContribRateSample>>,
    set_rates: WriteSignal<ContribRateSnapshot>,
    set_contrib: WriteSignal<Option<ContribStatus>>,
) {
    let sample = ContribRateSample::from_snapshot(&snapshot);
    if let Some(previous) = last_sample.get() {
        set_rates.set(ContribRateSnapshot::from_delta(previous, sample.clone()));
    }
    set_last_sample.set(Some(sample));
    set_contrib.set(Some(snapshot));
}

fn mesh_events_url(mesh_api: &str) -> String {
    let base = mesh_api
        .split_once("/api/mesh")
        .map(|(base, _)| base)
        .unwrap_or(mesh_api.trim_end_matches('/'));
    format!("{base}/api/mesh/events")
}

fn contrib_events_url(contrib_api: &str) -> String {
    let base = contrib_api
        .split_once("/api/status")
        .map(|(base, _)| base)
        .unwrap_or(contrib_api.trim_end_matches('/'));
    format!("{base}/api/status/events")
}

fn mesh_control_url(mesh_api: &str, action: &str) -> String {
    let base = mesh_api
        .split_once("/api/mesh")
        .map(|(base, _)| base)
        .unwrap_or(mesh_api.trim_end_matches('/'));
    format!("{base}/api/control/{action}")
}

fn playlist_probe_targets(
    mesh: Option<&MeshApiSnapshot>,
    contrib: Option<&ContribStatus>,
    mesh_api: &str,
    contrib_api: &str,
) -> Vec<PlaylistProbeTarget> {
    let mut seen = HashSet::new();
    let mut targets = Vec::new();

    if let Some(contrib) = contrib {
        if !contrib.advertised_hls_path.is_empty() {
            push_playlist_probe_target(
                &mut targets,
                &mut seen,
                "contrib".to_owned(),
                join_url_path(
                    &api_base(contrib_api, "/api/status"),
                    &contrib.advertised_hls_path,
                ),
            );
        }
    }

    if let Some(mesh) = mesh {
        let stream_id = if mesh.stream.stream_id_text.is_empty() {
            "1"
        } else {
            mesh.stream.stream_id_text.as_str()
        };
        for edge in &mesh.edge_services {
            if let Some(base_url) = &edge.playback_base_url {
                push_playlist_probe_target(
                    &mut targets,
                    &mut seen,
                    format!("mesh {}", edge.node_id),
                    join_url_path(base_url, &format!("{stream_id}/stream.m3u8")),
                );
            }
        }
        if !targets
            .iter()
            .any(|target| target.label.starts_with("mesh "))
        {
            push_playlist_probe_target(
                &mut targets,
                &mut seen,
                "mesh local".to_owned(),
                join_url_path(
                    &join_url_path(&api_base(mesh_api, "/api/mesh"), "live"),
                    &format!("{stream_id}/stream.m3u8"),
                ),
            );
        }
    }

    targets
}

fn push_playlist_probe_target(
    targets: &mut Vec<PlaylistProbeTarget>,
    seen: &mut HashSet<String>,
    label: String,
    url: String,
) {
    if seen.insert(url.clone()) {
        targets.push(PlaylistProbeTarget { label, url });
    }
}

fn api_base(api_url: &str, marker: &str) -> String {
    api_url
        .split_once(marker)
        .map(|(base, _)| base)
        .unwrap_or(api_url.trim_end_matches('/'))
        .to_owned()
}

fn join_url_path(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    let path = path.trim_start_matches('/');
    format!("{base}/{path}")
}

fn endpoint_from_query(key: &str, fallback: &str) -> String {
    let Some(window) = web_sys::window() else {
        return fallback.to_owned();
    };
    let Ok(search) = window.location().search() else {
        return fallback.to_owned();
    };
    let prefix = format!("{key}=");
    search
        .trim_start_matches('?')
        .split('&')
        .filter_map(|part| part.split_once('='))
        .find_map(|(name, value)| {
            if name == key {
                Some(value.replace("%3A", ":").replace("%2F", "/"))
            } else {
                None
            }
        })
        .or_else(|| {
            search
                .trim_start_matches('?')
                .split('&')
                .find_map(|part| part.strip_prefix(&prefix).map(ToOwned::to_owned))
        })
        .unwrap_or_else(|| fallback.to_owned())
}

fn js_error_text(value: JsValue) -> String {
    value
        .as_string()
        .unwrap_or_else(|| format!("JavaScript error: {value:?}"))
}

fn enabled_listener_count(contrib: &ContribStatus) -> usize {
    contrib
        .listeners
        .iter()
        .filter(|listener| listener.enabled)
        .count()
}

fn optional_text(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

fn nonzero_u64(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

fn optional_age(value: Option<u64>) -> String {
    value
        .map(format_duration_ms)
        .unwrap_or_else(|| "never".to_owned())
}

fn optional_unix_age(value: Option<u64>) -> String {
    value.map(age_text).unwrap_or_else(|| "config".to_owned())
}

fn youngest_age(values: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    values.into_iter().flatten().min()
}

fn format_duration_ms(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms}ms ago")
    } else if ms < 60_000 {
        format!("{}s ago", ms / 1_000)
    } else {
        format!("{}m ago", ms / 60_000)
    }
}

fn format_duration_ms_plain(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        format!("{}s", ms / 1_000)
    } else {
        format!("{}m", ms / 60_000)
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn format_bps(bps: u64) -> String {
    if bps >= 1_000_000_000 {
        format!("{:.1} Gbps", bps as f64 / 1_000_000_000.0)
    } else if bps >= 1_000_000 {
        format!("{:.1} Mbps", bps as f64 / 1_000_000.0)
    } else if bps >= 1_000 {
        format!("{:.1} Kbps", bps as f64 / 1_000.0)
    } else {
        format!("{bps} bps")
    }
}

fn percent(used: u64, total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    ((used as f64 / total as f64) * 100.0).clamp(0.0, 100.0)
}

fn age_text(unix_ms: u64) -> String {
    let now = js_sys::Date::now() as u64;
    let age = now.saturating_sub(unix_ms);
    if age < 1_000 {
        "now".to_owned()
    } else if age < 60_000 {
        format!("{}s ago", age / 1_000)
    } else {
        format!("{}m ago", age / 60_000)
    }
}

fn short_clock() -> String {
    js_sys::Date::new_0()
        .to_locale_time_string("en-GB")
        .as_string()
        .unwrap_or_else(|| "now".to_owned())
}

#[derive(Clone, Debug)]
struct PlaylistProbeTarget {
    label: String,
    url: String,
}

#[derive(Clone, Debug, Default)]
struct PlaybackProbeState {
    updated_unix_ms: u64,
    probes: Vec<PlaylistProbe>,
}

impl PlaybackProbeState {
    fn ready_count(&self) -> usize {
        self.probes.iter().filter(|probe| probe.ok).count()
    }

    fn summary_text(&self) -> String {
        if self.probes.is_empty() {
            "-".to_owned()
        } else {
            format!("{}/{}", self.ready_count(), self.probes.len())
        }
    }

    fn detail_text(&self) -> String {
        if self.probes.is_empty() {
            "waiting for playlist targets".to_owned()
        } else {
            let failures = self.probes.len().saturating_sub(self.ready_count());
            format!(
                "{failures} failing / probed {}",
                age_text(self.updated_unix_ms)
            )
        }
    }
}

#[derive(Clone, Debug, Default)]
struct PlaylistProbe {
    label: String,
    url: String,
    status: Option<u16>,
    ok: bool,
    elapsed_ms: u64,
    content_length: Option<u64>,
    content_type: Option<String>,
    error: Option<String>,
}

impl PlaylistProbe {
    fn class_name(&self) -> &'static str {
        if self.ok {
            "playback-probe ok"
        } else {
            "playback-probe error"
        }
    }

    fn status_text(&self) -> String {
        match (self.status, &self.error) {
            (Some(status), _) => format!("HTTP {status}"),
            (None, Some(error)) => format!("error: {error}"),
            (None, None) => "pending".to_owned(),
        }
    }

    fn meta_text(&self) -> String {
        let mut parts = vec![self.url.clone(), format!("{}ms", self.elapsed_ms)];
        if let Some(length) = self.content_length {
            parts.push(format_bytes(length));
        }
        if let Some(content_type) = &self.content_type {
            parts.push(content_type.clone());
        }
        parts.join(" / ")
    }
}

#[derive(Clone, Copy, Debug)]
struct DashboardFeedHealth {
    started_unix_ms: u64,
    mesh_events_active: bool,
    contrib_events_active: bool,
}

impl DashboardFeedHealth {
    fn new(started_unix_ms: u64, mesh_events_active: bool, contrib_events_active: bool) -> Self {
        Self {
            started_unix_ms,
            mesh_events_active,
            contrib_events_active,
        }
    }

    fn within_startup_grace(self) -> bool {
        now_unix_ms().saturating_sub(self.started_unix_ms) < DASHBOARD_FEED_MISSING_GRACE_MS
    }
}

#[derive(Clone, Debug)]
struct FeedDiagnostics {
    source: String,
    endpoint: String,
    mode: &'static str,
    event_messages: u64,
    poll_successes: u64,
    reconnects: u64,
    errors: u64,
    parse_errors: u64,
    last_event_unix_ms: Option<u64>,
    last_poll_unix_ms: Option<u64>,
    last_error: Option<String>,
}

impl FeedDiagnostics {
    fn new(source: &'static str) -> Self {
        Self {
            source: source.to_owned(),
            endpoint: String::new(),
            mode: "starting",
            event_messages: 0,
            poll_successes: 0,
            reconnects: 0,
            errors: 0,
            parse_errors: 0,
            last_event_unix_ms: None,
            last_poll_unix_ms: None,
            last_error: None,
        }
    }

    fn record_polling(&mut self, endpoint: &str) {
        self.endpoint = endpoint.to_owned();
        self.mode = "polling";
    }

    fn record_poll_ok(&mut self) {
        self.mode = "polling";
        self.poll_successes = self.poll_successes.saturating_add(1);
        self.last_poll_unix_ms = Some(now_unix_ms());
        self.last_error = None;
    }

    fn record_poll_error(&mut self, error: &str) {
        self.mode = "polling";
        self.errors = self.errors.saturating_add(1);
        self.last_error = Some(error.to_owned());
    }

    fn record_event_connecting(&mut self, endpoint: &str) {
        self.endpoint = endpoint.to_owned();
        self.mode = "connecting";
        self.last_error = None;
    }

    fn record_event_ok(&mut self) {
        self.mode = "events";
        self.event_messages = self.event_messages.saturating_add(1);
        self.last_event_unix_ms = Some(now_unix_ms());
        self.last_error = None;
    }

    fn record_event_reconnect(&mut self, endpoint: &str) {
        self.endpoint = endpoint.to_owned();
        self.mode = "reconnecting";
        self.reconnects = self.reconnects.saturating_add(1);
        self.last_error = Some("EventSource reconnecting".to_owned());
    }

    fn record_event_error(&mut self, error: &str) {
        self.mode = "event error";
        self.errors = self.errors.saturating_add(1);
        self.last_error = Some(error.to_owned());
    }

    fn record_parse_error(&mut self, error: impl Into<String>) {
        self.mode = "parse error";
        self.errors = self.errors.saturating_add(1);
        self.parse_errors = self.parse_errors.saturating_add(1);
        self.last_error = Some(error.into());
    }

    fn class_name(&self, active: bool) -> &'static str {
        if self.last_error.is_some() {
            "dashboard-hose-card error"
        } else if active {
            "dashboard-hose-card ready"
        } else if self.poll_successes > 0 {
            "dashboard-hose-card warn"
        } else {
            "dashboard-hose-card waiting"
        }
    }

    fn summary_text(&self, active: bool) -> String {
        if active {
            "events active".to_owned()
        } else if self.mode == "polling" && self.poll_successes > 0 {
            "polling fallback".to_owned()
        } else {
            self.mode.to_owned()
        }
    }

    fn detail_text(&self) -> String {
        let mut parts = vec![
            self.endpoint_text(),
            format!("{} events", self.event_messages),
            format!("{} polls", self.poll_successes),
            format!("{} reconnects", self.reconnects),
            format!("{} errors", self.errors),
        ];
        if self.parse_errors > 0 {
            parts.push(format!("{} parse", self.parse_errors));
        }
        if let Some(last_event) = self.last_event_unix_ms {
            parts.push(format!("event {}", optional_unix_age(Some(last_event))));
        }
        if let Some(last_poll) = self.last_poll_unix_ms {
            parts.push(format!("poll {}", optional_unix_age(Some(last_poll))));
        }
        if let Some(error) = &self.last_error {
            parts.push(error.clone());
        }
        parts.join(" / ")
    }

    fn endpoint_text(&self) -> String {
        if self.endpoint.is_empty() {
            "endpoint pending".to_owned()
        } else {
            self.endpoint.clone()
        }
    }
}

#[derive(Clone, Debug)]
struct PipelineStage {
    key: &'static str,
    title: &'static str,
    level: &'static str,
    status: String,
    detail: String,
}

impl PipelineStage {
    fn new(
        key: &'static str,
        title: &'static str,
        level: &'static str,
        status: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            key,
            title,
            level,
            status: status.into(),
            detail: detail.into(),
        }
    }

    fn class_name(&self) -> String {
        format!("pipeline-stage {}", self.level)
    }
}

fn build_pipeline_stages(
    mesh: &Option<MeshApiSnapshot>,
    contrib: &Option<ContribStatus>,
    probes: &PlaybackProbeState,
    feed: DashboardFeedHealth,
) -> Vec<PipelineStage> {
    vec![
        pipeline_ingest_stage(contrib.as_ref()),
        pipeline_fmp4_stage(contrib.as_ref()),
        pipeline_mesh_stage(mesh.as_ref(), contrib.as_ref()),
        pipeline_playback_stage(probes),
        pipeline_control_stage(mesh.as_ref(), contrib.as_ref(), feed),
    ]
}

fn pipeline_summary_text(stages: &[PipelineStage]) -> String {
    let ready = stages.iter().filter(|stage| stage.level == "ready").count();
    let errors = stages.iter().filter(|stage| stage.level == "error").count();
    let warnings = stages.iter().filter(|stage| stage.level == "warn").count();
    let waiting = stages
        .iter()
        .filter(|stage| stage.level == "waiting")
        .count();
    format!(
        "{ready}/{} ready / {errors} errors / {warnings} warnings / {waiting} waiting",
        stages.len()
    )
}

fn pipeline_ingest_stage(contrib: Option<&ContribStatus>) -> PipelineStage {
    let Some(contrib) = contrib else {
        return PipelineStage::new(
            "ingest",
            "ingest",
            "waiting",
            "waiting",
            "contrib status unavailable",
        );
    };

    let active_protocols = contrib
        .runtime
        .protocols
        .iter()
        .filter(|protocol| protocol.active_sessions > 0)
        .map(|protocol| protocol.protocol.clone())
        .collect::<Vec<_>>();
    let listener_count = enabled_listener_count(contrib);
    let detail = if active_protocols.is_empty() {
        format!(
            "{listener_count} listener(s) enabled / {}",
            contrib.health.detail_text()
        )
    } else {
        format!(
            "active {} / {}",
            active_protocols.join(", "),
            contrib.health.detail_text()
        )
    };

    if contrib_health_age_is_fresh(
        contrib.health.input_seen,
        contrib.health.last_input_age_ms,
        &contrib.health,
    ) {
        PipelineStage::new("ingest", "ingest", "ready", "receiving bytes", detail)
    } else if contrib.health.input_seen {
        PipelineStage::new("ingest", "ingest", "warn", "input stale", detail)
    } else if listener_count > 0 {
        PipelineStage::new("ingest", "ingest", "waiting", "listening", detail)
    } else {
        PipelineStage::new("ingest", "ingest", "warn", "no listeners", detail)
    }
}

fn pipeline_fmp4_stage(contrib: Option<&ContribStatus>) -> PipelineStage {
    let Some(contrib) = contrib else {
        return PipelineStage::new(
            "fmp4",
            "fMP4",
            "waiting",
            "waiting",
            "contrib status unavailable",
        );
    };

    let runtime = &contrib.runtime.fmp4;
    let detail = format!(
        "{} parts / {} media / {} init / {} / {}",
        runtime.parts,
        format_bytes(runtime.bytes),
        format_bytes(runtime.init_bytes),
        runtime.track_summary(),
        contrib.health.detail_text()
    );
    if runtime.publish_errors > 0 {
        PipelineStage::new(
            "fmp4",
            "fMP4",
            "error",
            format!("{} publish errors", runtime.publish_errors),
            detail,
        )
    } else if contrib_health_age_is_fresh(
        contrib.health.output_seen,
        contrib.health.last_output_age_ms,
        &contrib.health,
    ) {
        PipelineStage::new("fmp4", "fMP4", "ready", "publishing parts", detail)
    } else if contrib.health.fmp4_input_seen {
        PipelineStage::new("fmp4", "fMP4", "warn", "input without fresh output", detail)
    } else if contrib.health.input_seen {
        PipelineStage::new(
            "fmp4",
            "fMP4",
            "waiting",
            "waiting for boxed output",
            detail,
        )
    } else {
        PipelineStage::new("fmp4", "fMP4", "waiting", "waiting for input", detail)
    }
}

fn pipeline_mesh_stage(
    mesh: Option<&MeshApiSnapshot>,
    contrib: Option<&ContribStatus>,
) -> PipelineStage {
    let forward_errors = contrib
        .map(|contrib| contrib.runtime.mesh_forward.errors())
        .unwrap_or_default();
    let forward_payloads = contrib
        .map(|contrib| contrib.runtime.mesh_forward.payloads())
        .unwrap_or_default();
    let forward_detail = contrib
        .map(|contrib| contrib.runtime.mesh_forward.detail_text())
        .unwrap_or_else(|| "contrib status unavailable".to_owned());
    let mesh_active = mesh
        .map(|mesh| {
            mesh.aggregate.active_streams > 0
                || mesh.stream.latest_mesh_part.is_some()
                || mesh.stream.latest_local_part.is_some()
                || mesh.streams.iter().any(StreamTelemetry::active)
        })
        .unwrap_or(false);
    let mesh_detail = mesh
        .map(|mesh| {
            format!(
                "{} active stream(s) / {} node(s) / local part {} / mesh part {}",
                mesh.aggregate.active_streams,
                mesh.aggregate.node_count,
                optional_u64(mesh.stream.latest_local_part),
                optional_u64(mesh.stream.latest_mesh_part)
            )
        })
        .unwrap_or_else(|| "mesh status unavailable".to_owned());
    let detail = format!("{forward_detail} / {mesh_detail}");

    if forward_errors > 0 {
        PipelineStage::new(
            "mesh",
            "mesh",
            "error",
            format!("{forward_errors} forward errors"),
            detail,
        )
    } else if mesh_active {
        PipelineStage::new("mesh", "mesh", "ready", "stream visible", detail)
    } else if forward_payloads > 0 {
        PipelineStage::new("mesh", "mesh", "warn", "forwarding but not visible", detail)
    } else {
        PipelineStage::new(
            "mesh",
            "mesh",
            "waiting",
            "waiting for mesh payloads",
            detail,
        )
    }
}

fn pipeline_playback_stage(probes: &PlaybackProbeState) -> PipelineStage {
    if probes.probes.is_empty() {
        return PipelineStage::new(
            "playback",
            "playback",
            "waiting",
            "waiting",
            "no playlist probe targets yet",
        );
    }

    let ready = probes.ready_count();
    let total = probes.probes.len();
    let failures = total.saturating_sub(ready);
    let detail = probes
        .probes
        .iter()
        .map(|probe| format!("{} {}", probe.label, probe.status_text()))
        .collect::<Vec<_>>()
        .join(" / ");
    if failures == 0 {
        PipelineStage::new(
            "playback",
            "playback",
            "ready",
            format!("{ready}/{total} playlists"),
            detail,
        )
    } else if ready > 0 {
        PipelineStage::new(
            "playback",
            "playback",
            "warn",
            format!("{failures}/{total} failing"),
            detail,
        )
    } else {
        PipelineStage::new(
            "playback",
            "playback",
            "error",
            format!("{failures}/{total} failing"),
            detail,
        )
    }
}

fn pipeline_control_stage(
    mesh: Option<&MeshApiSnapshot>,
    contrib: Option<&ContribStatus>,
    feed: DashboardFeedHealth,
) -> PipelineStage {
    let Some(mesh) = mesh else {
        return PipelineStage::new(
            "control",
            "control",
            "waiting",
            "waiting",
            "mesh status unavailable",
        );
    };

    let telemetry_total = mesh.orchestration.telemetry_peers.len();
    let telemetry_connected = mesh
        .orchestration
        .telemetry_peers
        .iter()
        .filter(|peer| peer.state == "connected")
        .count();
    let mesh_feed = if feed.mesh_events_active {
        "mesh SSE"
    } else {
        "mesh polling"
    };
    let contrib_feed = if feed.contrib_events_active {
        "contrib SSE"
    } else if contrib.is_some() {
        "contrib polling"
    } else {
        "contrib waiting"
    };
    let control = if mesh.orchestration.control_dispatch_ready {
        "control bus connected"
    } else {
        "control bus local-only"
    };
    let detail =
        format!("{mesh_feed} / {contrib_feed} / {telemetry_connected}/{telemetry_total} telemetry / {control}");

    if feed.within_startup_grace() && (!feed.mesh_events_active || !feed.contrib_events_active) {
        PipelineStage::new("control", "control", "waiting", "connecting hoses", detail)
    } else if !feed.mesh_events_active || !feed.contrib_events_active {
        PipelineStage::new("control", "control", "warn", "polling fallback", detail)
    } else if telemetry_total > 0 && telemetry_connected == 0 {
        PipelineStage::new(
            "control",
            "control",
            "warn",
            "telemetry disconnected",
            detail,
        )
    } else if mesh.orchestration.control_dispatch_ready {
        PipelineStage::new("control", "control", "ready", "data hoses up", detail)
    } else {
        PipelineStage::new("control", "control", "warn", "local control only", detail)
    }
}

fn contrib_health_age_is_fresh(seen: bool, age_ms: Option<u64>, health: &ContribHealth) -> bool {
    seen && age_ms.is_some_and(|age_ms| age_ms <= health.stale_threshold_ms.max(1))
}

#[derive(Clone, Debug)]
struct Incident {
    level: String,
    source: String,
    code: String,
    message: String,
    detail: String,
    count: u64,
    last_seen_unix_ms: Option<u64>,
}

impl Incident {
    fn class_name(&self) -> String {
        format!("incident {}", self.level)
    }

    fn key(&self) -> String {
        format!("{}:{}:{}", self.source, self.code, self.message)
    }

    fn meta_text(&self) -> String {
        let mut parts = vec![format!("{} seen", self.count)];
        parts.push(optional_unix_age(self.last_seen_unix_ms));
        if !self.detail.is_empty() {
            parts.push(self.detail.clone());
        }
        parts.join(" / ")
    }
}

fn build_incidents(
    mesh: &Option<MeshApiSnapshot>,
    contrib: &Option<ContribStatus>,
    probes: &PlaybackProbeState,
    feed: DashboardFeedHealth,
) -> Vec<Incident> {
    let mut incidents = Vec::new();

    push_dashboard_feed_incidents(
        &mut incidents,
        "mesh",
        mesh.as_ref().map(|snapshot| snapshot.updated_unix_ms),
        feed.mesh_events_active,
        feed.started_unix_ms,
    );
    push_dashboard_feed_incidents(
        &mut incidents,
        "contrib",
        contrib.as_ref().map(|snapshot| snapshot.updated_unix_ms),
        feed.contrib_events_active,
        feed.started_unix_ms,
    );

    if let Some(mesh) = mesh {
        incidents.extend(mesh.alerts.iter().map(|alert| Incident {
            level: normalize_incident_level(&alert.level),
            source: "mesh".to_owned(),
            code: alert.code.clone(),
            message: alert.message.clone(),
            detail: format!("local {}", mesh.node.node_id),
            count: alert.count.max(1),
            last_seen_unix_ms: alert.last_seen_unix_ms,
        }));
    }

    if let Some(contrib) = contrib {
        incidents.extend(contrib.alerts.iter().map(|alert| Incident {
            level: normalize_incident_level(&alert.level),
            source: "contrib".to_owned(),
            code: alert.code.clone(),
            message: alert.message.clone(),
            detail: format!("stream {}", contrib.advertised_hls_stream_id),
            count: alert.count.max(1),
            last_seen_unix_ms: alert.last_seen_unix_ms,
        }));
    }

    incidents.extend(
        probes
            .probes
            .iter()
            .filter(|probe| !probe.ok)
            .map(|probe| Incident {
                level: "error".to_owned(),
                source: "playback".to_owned(),
                code: "playlist_probe_failed".to_owned(),
                message: format!("{} {}", probe.label, probe.status_text()),
                detail: probe.meta_text(),
                count: 1,
                last_seen_unix_ms: nonzero_u64(probes.updated_unix_ms),
            }),
    );

    incidents.sort_by(|left, right| {
        incident_level_rank(&left.level)
            .cmp(&incident_level_rank(&right.level))
            .then_with(|| {
                right
                    .last_seen_unix_ms
                    .unwrap_or_default()
                    .cmp(&left.last_seen_unix_ms.unwrap_or_default())
            })
            .then_with(|| left.source.cmp(&right.source))
            .then_with(|| left.code.cmp(&right.code))
    });
    incidents
}

fn incident_count_text(
    mesh: &Option<MeshApiSnapshot>,
    contrib: &Option<ContribStatus>,
    probes: &PlaybackProbeState,
    feed: DashboardFeedHealth,
) -> String {
    build_incidents(mesh, contrib, probes, feed)
        .len()
        .to_string()
}

fn incident_detail_text(
    mesh: &Option<MeshApiSnapshot>,
    contrib: &Option<ContribStatus>,
    probes: &PlaybackProbeState,
    feed: DashboardFeedHealth,
) -> String {
    let incidents = build_incidents(mesh, contrib, probes, feed);
    if incidents.is_empty() {
        return incident_empty_text(mesh, contrib, probes, feed);
    }
    let errors = incidents
        .iter()
        .filter(|incident| incident.level == "error")
        .count();
    let warnings = incidents
        .iter()
        .filter(|incident| incident.level == "warn")
        .count();
    let info = incidents
        .len()
        .saturating_sub(errors)
        .saturating_sub(warnings);
    format!("{errors} errors / {warnings} warnings / {info} info")
}

fn incident_empty_text(
    mesh: &Option<MeshApiSnapshot>,
    contrib: &Option<ContribStatus>,
    probes: &PlaybackProbeState,
    feed: DashboardFeedHealth,
) -> String {
    let waiting_for_grace =
        now_unix_ms().saturating_sub(feed.started_unix_ms) < DASHBOARD_FEED_MISSING_GRACE_MS;
    if waiting_for_grace && mesh.is_none() && contrib.is_none() && probes.probes.is_empty() {
        "waiting for feeds".to_owned()
    } else {
        "no active incidents".to_owned()
    }
}

fn push_dashboard_feed_incidents(
    incidents: &mut Vec<Incident>,
    source: &'static str,
    updated_unix_ms: Option<u64>,
    events_active: bool,
    started_unix_ms: u64,
) {
    let now = now_unix_ms();
    let since_start = now.saturating_sub(started_unix_ms);
    match nonzero_u64(updated_unix_ms.unwrap_or_default()) {
        Some(updated) => {
            let age_ms = now.saturating_sub(updated);
            if age_ms > DASHBOARD_SNAPSHOT_STALE_MS {
                incidents.push(Incident {
                    level: "error".to_owned(),
                    source: source.to_owned(),
                    code: format!("{source}_feed_stale"),
                    message: format!(
                        "{source} status data has not updated for {}.",
                        format_duration_ms_plain(age_ms)
                    ),
                    detail: format!("last snapshot {}", age_text(updated)),
                    count: 1,
                    last_seen_unix_ms: Some(updated),
                });
            } else if !events_active && since_start > DASHBOARD_FEED_MISSING_GRACE_MS {
                incidents.push(Incident {
                    level: "warn".to_owned(),
                    source: source.to_owned(),
                    code: format!("{source}_events_inactive"),
                    message: format!(
                        "{source} SSE data hose is inactive; dashboard is relying on HTTP polling."
                    ),
                    detail: format!("last snapshot {}", age_text(updated)),
                    count: 1,
                    last_seen_unix_ms: Some(now),
                });
            }
        }
        None if since_start > DASHBOARD_FEED_MISSING_GRACE_MS => {
            incidents.push(Incident {
                level: "error".to_owned(),
                source: source.to_owned(),
                code: format!("{source}_feed_missing"),
                message: format!("{source} status feed has not delivered an initial snapshot."),
                detail: format!(
                    "dashboard waiting {}",
                    format_duration_ms_plain(since_start)
                ),
                count: 1,
                last_seen_unix_ms: Some(started_unix_ms),
            });
        }
        None => {}
    }
}

fn normalize_incident_level(level: &str) -> String {
    match level {
        "error" | "warn" | "info" => level.to_owned(),
        "warning" => "warn".to_owned(),
        _ => "info".to_owned(),
    }
}

fn incident_level_rank(level: &str) -> u8 {
    match level {
        "error" => 0,
        "warn" => 1,
        _ => 2,
    }
}

#[derive(Clone, Debug, Default)]
struct TopologyGraphData {
    nodes: Vec<TopologyGraphNode>,
    links: Vec<TopologyGraphLink>,
}

#[derive(Clone, Debug)]
struct TopologyGraphNode {
    node_id: String,
    region: String,
    active_streams: u64,
    severity: TopologyNodeSeverity,
    x: f64,
    y: f64,
}

impl TopologyGraphNode {
    fn short_label(&self) -> String {
        if self.node_id.chars().count() <= 10 {
            self.node_id.clone()
        } else {
            format!("{}...", self.node_id.chars().take(10).collect::<String>())
        }
    }

    fn detail_text(&self) -> String {
        format!("{} / {} active", self.region, self.active_streams)
    }

    fn class_name(&self) -> String {
        format!("topology-node {}", self.severity.class_name())
    }
}

#[derive(Clone, Copy, Debug)]
enum TopologyNodeSeverity {
    Idle,
    Active,
    Warn,
    Error,
}

impl TopologyNodeSeverity {
    fn class_name(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Active => "active",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Debug)]
struct TopologyGraphLink {
    key: String,
    resolved: bool,
    x1: f64,
    y1: f64,
    x2: f64,
    y2: f64,
}

impl TopologyGraphLink {
    fn class_name(&self) -> &'static str {
        if self.resolved {
            "topology-link"
        } else {
            "topology-link unresolved"
        }
    }
}

fn build_topology_graph(snapshot: Option<MeshApiSnapshot>) -> TopologyGraphData {
    let Some(snapshot) = snapshot else {
        return TopologyGraphData::default();
    };
    let mut nodes = snapshot.nodes;
    nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));

    let count = nodes.len().max(1);
    let center_x = 360.0;
    let center_y = 124.0;
    let radius_x = 270.0;
    let radius_y = 78.0;
    let mut graph_nodes = Vec::with_capacity(nodes.len());
    let mut positions = HashMap::with_capacity(nodes.len());

    for (index, node) in nodes.into_iter().enumerate() {
        let (x, y) = if count == 1 {
            (center_x, center_y)
        } else {
            let angle = -std::f64::consts::FRAC_PI_2
                + (index as f64 * std::f64::consts::TAU / count as f64);
            (
                center_x + angle.cos() * radius_x,
                center_y + angle.sin() * radius_y,
            )
        };
        let severity = topology_node_severity(&node);
        positions.insert(node.node_id.clone(), (x, y));
        graph_nodes.push(TopologyGraphNode {
            node_id: node.node_id,
            region: node.region,
            active_streams: node.active_streams,
            severity,
            x,
            y,
        });
    }

    let links = snapshot
        .connections
        .into_iter()
        .filter_map(|connection| {
            let (x1, y1) = *positions.get(&connection.source_node_id)?;
            let (x2, y2, resolved) = if let Some(target_node_id) = &connection.target_node_id {
                let (x2, y2) = *positions.get(target_node_id)?;
                (x2, y2, true)
            } else {
                let dx = x1 - center_x;
                let dy = y1 - center_y;
                let len = (dx * dx + dy * dy).sqrt().max(1.0);
                (x1 + (dx / len * 58.0), y1 + (dy / len * 38.0), false)
            };
            Some(TopologyGraphLink {
                key: format!(
                    "{}:{}:{}",
                    connection.source_node_id,
                    connection
                        .target_node_id
                        .as_deref()
                        .unwrap_or(&connection.target_addr),
                    connection.state
                ),
                resolved,
                x1,
                y1,
                x2,
                y2,
            })
        })
        .collect();

    TopologyGraphData {
        nodes: graph_nodes,
        links,
    }
}

fn topology_node_severity(node: &MeshNode) -> TopologyNodeSeverity {
    let storage = percent(node.used_storage_bytes, node.total_storage_bytes);
    if node.draining || storage >= 95.0 {
        TopologyNodeSeverity::Error
    } else if storage >= 85.0 {
        TopologyNodeSeverity::Warn
    } else if node.active_streams > 0 || node.contributor_streams > 0 {
        TopologyNodeSeverity::Active
    } else {
        TopologyNodeSeverity::Idle
    }
}

fn now_unix_ms() -> u64 {
    js_sys::Date::now() as u64
}

#[derive(Clone, Debug, Default)]
struct MeshRateSample {
    sampled_unix_ms: u64,
    bytes_received: u64,
    datagrams_received: u64,
    nodes: HashMap<String, NodeRateCounters>,
    streams: HashMap<String, StreamRateCounters>,
    edges: HashMap<String, EdgeRateCounters>,
    telemetry_peers: HashMap<String, TelemetryPeerRateCounters>,
}

impl MeshRateSample {
    fn from_snapshot(snapshot: &MeshApiSnapshot) -> Self {
        let stream_bytes = snapshot
            .streams
            .iter()
            .map(|stream| stream.bytes_received)
            .sum::<u64>();
        let stream_datagrams = snapshot
            .streams
            .iter()
            .map(|stream| stream.datagrams_received)
            .sum::<u64>();
        Self {
            sampled_unix_ms: nonzero_u64(snapshot.updated_unix_ms).unwrap_or_else(now_unix_ms),
            bytes_received: stream_bytes.max(snapshot.stream.bytes_received),
            datagrams_received: stream_datagrams.max(snapshot.stream.datagrams_received),
            nodes: node_rate_counters(&snapshot.streams),
            streams: snapshot
                .streams
                .iter()
                .map(|stream| {
                    (
                        stream.rate_key(),
                        StreamRateCounters {
                            bytes_received: stream.bytes_received,
                            datagrams_received: stream.datagrams_received,
                        },
                    )
                })
                .collect(),
            edges: snapshot
                .edge_services
                .iter()
                .map(|edge| {
                    (
                        edge.node_id.clone(),
                        EdgeRateCounters {
                            requests_served: edge.requests_served,
                            bytes_served: edge.bytes_served,
                            llhls_tail_requests: edge.llhls_tail_requests,
                            responses_total: edge.responses_total,
                            response_errors: edge.response_errors,
                            response_not_found: edge.response_not_found,
                        },
                    )
                })
                .collect(),
            telemetry_peers: snapshot
                .orchestration
                .telemetry_peers
                .iter()
                .map(|peer| {
                    (
                        peer.peer.clone(),
                        TelemetryPeerRateCounters {
                            payloads: peer.payloads,
                            bytes: peer.bytes,
                            connect_attempts: peer.connect_attempts,
                            disconnects: peer.disconnects,
                        },
                    )
                })
                .collect(),
        }
    }
}

fn node_rate_counters(streams: &[StreamTelemetry]) -> HashMap<String, NodeRateCounters> {
    let mut nodes = HashMap::<String, NodeRateCounters>::new();
    for stream in streams {
        let node = nodes.entry(stream.node_id.clone()).or_default();
        node.streams = node.streams.saturating_add(1);
        node.bytes_received = node.bytes_received.saturating_add(stream.bytes_received);
        node.datagrams_received = node
            .datagrams_received
            .saturating_add(stream.datagrams_received);
    }
    nodes
}

#[derive(Clone, Copy, Debug, Default)]
struct NodeRateCounters {
    streams: usize,
    bytes_received: u64,
    datagrams_received: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct NodeRateSnapshot {
    ready: bool,
    window_ms: u64,
    streams: usize,
    bytes_per_sec: f64,
    datagrams_per_sec: f64,
}

impl NodeRateSnapshot {
    fn from_delta(previous: NodeRateCounters, current: NodeRateCounters, window_ms: u64) -> Self {
        Self {
            ready: window_ms >= 250,
            window_ms,
            streams: current.streams,
            bytes_per_sec: counter_rate(previous.bytes_received, current.bytes_received, window_ms),
            datagrams_per_sec: counter_rate(
                previous.datagrams_received,
                current.datagrams_received,
                window_ms,
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct StreamRateCounters {
    bytes_received: u64,
    datagrams_received: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct StreamRateSnapshot {
    ready: bool,
    window_ms: u64,
    bytes_per_sec: f64,
    datagrams_per_sec: f64,
}

impl StreamRateSnapshot {
    fn from_delta(
        previous: StreamRateCounters,
        current: StreamRateCounters,
        window_ms: u64,
    ) -> Self {
        Self {
            ready: window_ms >= 250,
            window_ms,
            bytes_per_sec: counter_rate(previous.bytes_received, current.bytes_received, window_ms),
            datagrams_per_sec: counter_rate(
                previous.datagrams_received,
                current.datagrams_received,
                window_ms,
            ),
        }
    }

    fn text(&self) -> String {
        if !self.ready {
            return "rate waiting".to_owned();
        }
        format!(
            "{} / {} / {}",
            format_bytes_per_sec(true, self.bytes_per_sec),
            format_count_per_sec(self.datagrams_per_sec, "datagrams"),
            format_rate_window(self.window_ms)
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct EdgeRateCounters {
    requests_served: u64,
    bytes_served: u64,
    llhls_tail_requests: u64,
    responses_total: u64,
    response_errors: u64,
    response_not_found: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct EdgeRateSnapshot {
    ready: bool,
    window_ms: u64,
    requests_per_sec: f64,
    bytes_per_sec: f64,
    tail_requests_per_sec: f64,
    responses_per_sec: f64,
    errors_per_sec: f64,
    not_found_per_sec: f64,
}

impl EdgeRateSnapshot {
    fn from_delta(previous: EdgeRateCounters, current: EdgeRateCounters, window_ms: u64) -> Self {
        Self {
            ready: window_ms >= 250,
            window_ms,
            requests_per_sec: counter_rate(
                previous.requests_served,
                current.requests_served,
                window_ms,
            ),
            bytes_per_sec: counter_rate(previous.bytes_served, current.bytes_served, window_ms),
            tail_requests_per_sec: counter_rate(
                previous.llhls_tail_requests,
                current.llhls_tail_requests,
                window_ms,
            ),
            responses_per_sec: counter_rate(
                previous.responses_total,
                current.responses_total,
                window_ms,
            ),
            errors_per_sec: counter_rate(
                previous.response_errors,
                current.response_errors,
                window_ms,
            ),
            not_found_per_sec: counter_rate(
                previous.response_not_found,
                current.response_not_found,
                window_ms,
            ),
        }
    }

    fn traffic_text(&self) -> String {
        if !self.ready {
            return "rate waiting".to_owned();
        }
        format!(
            "{} / {}",
            format_bytes_per_sec(true, self.bytes_per_sec),
            format_count_per_sec(self.requests_per_sec, "reads")
        )
    }

    fn response_text(&self) -> String {
        if !self.ready {
            return "rate waiting".to_owned();
        }
        format!(
            "{} / {} / {}",
            format_count_per_sec(self.responses_per_sec, "responses"),
            format_count_per_sec(self.errors_per_sec, "errors"),
            format_count_per_sec(self.not_found_per_sec, "404s")
        )
    }

    fn tail_text(&self) -> String {
        if !self.ready {
            return "tail rate waiting".to_owned();
        }
        format_count_per_sec(self.tail_requests_per_sec, "tails")
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TelemetryPeerRateCounters {
    payloads: u64,
    bytes: u64,
    connect_attempts: u64,
    disconnects: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct TelemetryPeerRateSnapshot {
    ready: bool,
    window_ms: u64,
    payloads_per_sec: f64,
    bytes_per_sec: f64,
    connect_attempts_per_sec: f64,
    disconnects_per_sec: f64,
}

impl TelemetryPeerRateSnapshot {
    fn from_delta(
        previous: TelemetryPeerRateCounters,
        current: TelemetryPeerRateCounters,
        window_ms: u64,
    ) -> Self {
        Self {
            ready: window_ms >= 250,
            window_ms,
            payloads_per_sec: counter_rate(previous.payloads, current.payloads, window_ms),
            bytes_per_sec: counter_rate(previous.bytes, current.bytes, window_ms),
            connect_attempts_per_sec: counter_rate(
                previous.connect_attempts,
                current.connect_attempts,
                window_ms,
            ),
            disconnects_per_sec: counter_rate(previous.disconnects, current.disconnects, window_ms),
        }
    }

    fn traffic_text(&self) -> String {
        if !self.ready {
            return "rate waiting".to_owned();
        }
        format!(
            "{} / {}",
            format_bytes_per_sec(true, self.bytes_per_sec),
            format_count_per_sec(self.payloads_per_sec, "payloads")
        )
    }

    fn churn_text(&self) -> String {
        if !self.ready {
            return "churn waiting".to_owned();
        }
        format!(
            "{} / {}",
            format_count_per_sec(self.connect_attempts_per_sec, "connects"),
            format_count_per_sec(self.disconnects_per_sec, "disconnects")
        )
    }
}

#[derive(Clone, Debug, Default)]
struct MeshRateSnapshot {
    ready: bool,
    window_ms: u64,
    bytes_per_sec: f64,
    datagrams_per_sec: f64,
    nodes: HashMap<String, NodeRateSnapshot>,
    streams: HashMap<String, StreamRateSnapshot>,
    edges: HashMap<String, EdgeRateSnapshot>,
    telemetry_peers: HashMap<String, TelemetryPeerRateSnapshot>,
}

impl MeshRateSnapshot {
    fn from_delta(previous: MeshRateSample, current: MeshRateSample) -> Self {
        let window_ms = current
            .sampled_unix_ms
            .saturating_sub(previous.sampled_unix_ms);
        Self {
            ready: window_ms >= 250,
            window_ms,
            bytes_per_sec: counter_rate(previous.bytes_received, current.bytes_received, window_ms),
            datagrams_per_sec: counter_rate(
                previous.datagrams_received,
                current.datagrams_received,
                window_ms,
            ),
            nodes: current
                .nodes
                .iter()
                .map(|(node_id, current)| {
                    let previous = previous.nodes.get(node_id).copied().unwrap_or_default();
                    (
                        node_id.clone(),
                        NodeRateSnapshot::from_delta(previous, *current, window_ms),
                    )
                })
                .collect(),
            streams: current
                .streams
                .iter()
                .map(|(stream, current)| {
                    let previous = previous.streams.get(stream).copied().unwrap_or_default();
                    (
                        stream.clone(),
                        StreamRateSnapshot::from_delta(previous, *current, window_ms),
                    )
                })
                .collect(),
            edges: current
                .edges
                .iter()
                .map(|(node_id, current)| {
                    let previous = previous.edges.get(node_id).copied().unwrap_or_default();
                    (
                        node_id.clone(),
                        EdgeRateSnapshot::from_delta(previous, *current, window_ms),
                    )
                })
                .collect(),
            telemetry_peers: current
                .telemetry_peers
                .iter()
                .map(|(peer, current)| {
                    let previous = previous
                        .telemetry_peers
                        .get(peer)
                        .copied()
                        .unwrap_or_default();
                    (
                        peer.clone(),
                        TelemetryPeerRateSnapshot::from_delta(previous, *current, window_ms),
                    )
                })
                .collect(),
        }
    }

    fn byte_rate_text(&self) -> String {
        format_bytes_per_sec(self.ready, self.bytes_per_sec)
    }

    fn detail_text(&self) -> String {
        if !self.ready {
            return "waiting for second sample".to_owned();
        }
        format!(
            "{} / {}",
            format_count_per_sec(self.datagrams_per_sec, "datagrams"),
            format_rate_window(self.window_ms)
        )
    }
}

#[derive(Clone, Debug, Default)]
struct ContribRateSample {
    sampled_unix_ms: u64,
    input_bytes: u64,
    input_datagrams: u64,
    output_bytes: u64,
    output_parts: u64,
    protocols: HashMap<String, ProtocolRateCounters>,
}

impl ContribRateSample {
    fn from_snapshot(snapshot: &ContribStatus) -> Self {
        Self {
            sampled_unix_ms: nonzero_u64(snapshot.updated_unix_ms).unwrap_or_else(now_unix_ms),
            input_bytes: snapshot
                .runtime
                .raw_http
                .bytes
                .saturating_add(snapshot.runtime.media_access_units.payload_bytes)
                .saturating_add(snapshot.runtime.mpeg_ts.bytes)
                .saturating_add(snapshot.runtime.rtmp.bytes),
            input_datagrams: snapshot
                .runtime
                .raw_http
                .datagrams
                .saturating_add(snapshot.runtime.media_access_units.datagrams),
            output_bytes: snapshot.runtime.fmp4.bytes,
            output_parts: snapshot.runtime.fmp4.parts,
            protocols: snapshot
                .runtime
                .protocols
                .iter()
                .map(|protocol| {
                    (
                        protocol.protocol.clone(),
                        ProtocolRateCounters {
                            units: protocol.units,
                            bytes: protocol.bytes,
                        },
                    )
                })
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ProtocolRateCounters {
    units: u64,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct ProtocolRateSnapshot {
    ready: bool,
    window_ms: u64,
    units_per_sec: f64,
    bytes_per_sec: f64,
}

impl ProtocolRateSnapshot {
    fn from_delta(
        previous: ProtocolRateCounters,
        current: ProtocolRateCounters,
        window_ms: u64,
    ) -> Self {
        Self {
            ready: window_ms >= 250,
            window_ms,
            units_per_sec: counter_rate(previous.units, current.units, window_ms),
            bytes_per_sec: counter_rate(previous.bytes, current.bytes, window_ms),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ContribRateSnapshot {
    ready: bool,
    window_ms: u64,
    input_bytes_per_sec: f64,
    input_datagrams_per_sec: f64,
    output_bytes_per_sec: f64,
    output_parts_per_sec: f64,
    protocols: HashMap<String, ProtocolRateSnapshot>,
}

impl ContribRateSnapshot {
    fn from_delta(previous: ContribRateSample, current: ContribRateSample) -> Self {
        let window_ms = current
            .sampled_unix_ms
            .saturating_sub(previous.sampled_unix_ms);
        Self {
            ready: window_ms >= 250,
            window_ms,
            input_bytes_per_sec: counter_rate(previous.input_bytes, current.input_bytes, window_ms),
            input_datagrams_per_sec: counter_rate(
                previous.input_datagrams,
                current.input_datagrams,
                window_ms,
            ),
            output_bytes_per_sec: counter_rate(
                previous.output_bytes,
                current.output_bytes,
                window_ms,
            ),
            output_parts_per_sec: counter_rate(
                previous.output_parts,
                current.output_parts,
                window_ms,
            ),
            protocols: current
                .protocols
                .iter()
                .map(|(protocol, current)| {
                    let previous = previous
                        .protocols
                        .get(protocol)
                        .copied()
                        .unwrap_or_default();
                    (
                        protocol.clone(),
                        ProtocolRateSnapshot::from_delta(previous, *current, window_ms),
                    )
                })
                .collect(),
        }
    }

    fn output_rate_text(&self) -> String {
        format_bytes_per_sec(self.ready, self.output_bytes_per_sec)
    }

    fn detail_text(&self) -> String {
        if !self.ready {
            return "waiting for second sample".to_owned();
        }
        format!(
            "input {} / {} / {} / {}",
            format_bytes_per_sec(true, self.input_bytes_per_sec),
            format_count_per_sec(self.output_parts_per_sec, "parts"),
            format_count_per_sec(self.input_datagrams_per_sec, "datagrams"),
            format_rate_window(self.window_ms)
        )
    }
}

fn counter_rate(previous: u64, current: u64, window_ms: u64) -> f64 {
    if window_ms == 0 || current < previous {
        return 0.0;
    }
    (current - previous) as f64 / (window_ms as f64 / 1_000.0)
}

fn format_bytes_per_sec(ready: bool, bytes_per_sec: f64) -> String {
    if !ready {
        "-".to_owned()
    } else {
        format!("{}/s", format_bytes(bytes_per_sec.max(0.0).round() as u64))
    }
}

fn format_count_per_sec(count_per_sec: f64, unit: &str) -> String {
    if count_per_sec >= 1_000.0 {
        format!("{:.1}k {unit}/s", count_per_sec / 1_000.0)
    } else {
        format!("{count_per_sec:.1} {unit}/s")
    }
}

fn format_rate_window(window_ms: u64) -> String {
    if window_ms < 1_000 {
        format!("{window_ms}ms window")
    } else {
        format!("{:.1}s window", window_ms as f64 / 1_000.0)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MeshApiSnapshot {
    #[serde(default)]
    updated_unix_ms: u64,
    #[serde(default)]
    node: MeshNode,
    #[serde(default)]
    peers: Vec<PeerSnapshot>,
    #[serde(default)]
    stream: StatsSnapshot,
    #[serde(default)]
    recent_commands: Vec<ControlCommand>,
    #[serde(default)]
    planned_replicas: Vec<ReplicaPlacementSnapshot>,
    #[serde(default)]
    aggregate: AggregateMetrics,
    #[serde(default)]
    alerts: Vec<MeshAlert>,
    #[serde(default)]
    activity: Vec<ActivityItem>,
    #[serde(default)]
    telemetry: TelemetryHealth,
    #[serde(default)]
    orchestration: OrchestrationStatus,
    #[serde(default)]
    nodes: Vec<MeshNode>,
    #[serde(default)]
    edge_services: Vec<EdgeServiceSnapshot>,
    #[serde(default)]
    connections: Vec<ConnectionSnapshot>,
    #[serde(default)]
    streams: Vec<StreamTelemetry>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MeshNode {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    continent: String,
    #[serde(default)]
    total_storage_bytes: u64,
    #[serde(default)]
    used_storage_bytes: u64,
    #[serde(default)]
    egress_capacity_bps: u64,
    #[serde(default)]
    contributor_streams: u64,
    #[serde(default)]
    active_streams: u64,
    #[serde(default)]
    draining: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PeerSnapshot {
    #[serde(default)]
    addr: String,
    #[serde(default)]
    state: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct StatsSnapshot {
    #[serde(default)]
    stream_id_text: String,
    #[serde(default)]
    latest_local_part: Option<u64>,
    #[serde(default)]
    latest_mesh_part: Option<u64>,
    #[serde(default)]
    bytes_received: u64,
    #[serde(default)]
    datagrams_received: u64,
    #[serde(default)]
    latest_local_part_age_ms: Option<u64>,
    #[serde(default)]
    last_ingest_age_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct AggregateMetrics {
    #[serde(default)]
    node_count: usize,
    #[serde(default)]
    connection_count: usize,
    #[serde(default)]
    total_storage_bytes: u64,
    #[serde(default)]
    used_storage_bytes: u64,
    #[serde(default)]
    total_egress_capacity_bps: u64,
    #[serde(default)]
    contributor_streams: u64,
    #[serde(default)]
    active_streams: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TelemetryHealth {
    #[serde(default)]
    stale_after_ms: u64,
    #[serde(default)]
    fresh_remote_count: usize,
    #[serde(default)]
    stale_remote_count: usize,
    #[serde(default)]
    stale_nodes: Vec<TelemetryNodeHealth>,
}

impl TelemetryHealth {
    fn class_name(&self) -> &'static str {
        if self.stale_remote_count > 0 {
            "topology-health-card warn"
        } else {
            "topology-health-card ready"
        }
    }

    fn summary_text(&self) -> String {
        format!(
            "{} fresh / {} stale",
            self.fresh_remote_count, self.stale_remote_count
        )
    }

    fn detail_text(&self) -> String {
        format!(
            "remote telemetry snapshots / stale after {}",
            format_duration_ms_plain(self.stale_after_ms)
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TelemetryNodeHealth {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    updated_unix_ms: u64,
    #[serde(default)]
    age_ms: u64,
}

impl TelemetryNodeHealth {
    fn detail_text(&self) -> String {
        format!(
            "{} / age {} / last {}",
            self.region,
            format_duration_ms_plain(self.age_ms),
            optional_unix_age(nonzero_u64(self.updated_unix_ms))
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct OrchestrationStatus {
    #[serde(default)]
    control_dispatch_ready: bool,
    #[serde(default)]
    provision: ProvisionStatus,
    #[serde(default)]
    telemetry_peers: Vec<TelemetryPeerStatus>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ProvisionStatus {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    backends: Vec<String>,
    #[serde(default)]
    timeout_ms: u64,
    #[serde(default)]
    backend_statuses: Vec<ProvisionBackendStatus>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ProvisionBackendStatus {
    #[serde(default)]
    name: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    details: Vec<String>,
}

impl ProvisionBackendStatus {
    fn class_name(&self) -> &'static str {
        match self.state.as_str() {
            "ready" => "provision-backend ready",
            "blocked" | "error" => "provision-backend blocked",
            _ => "provision-backend warn",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TelemetryPeerStatus {
    #[serde(default)]
    peer: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    connect_attempts: u64,
    #[serde(default)]
    disconnects: u64,
    #[serde(default)]
    payloads: u64,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    last_connected_unix_ms: Option<u64>,
    #[serde(default)]
    last_payload_unix_ms: Option<u64>,
    #[serde(default)]
    last_error: Option<String>,
}

impl TelemetryPeerStatus {
    fn class_name(&self) -> &'static str {
        match self.state.as_str() {
            "connected" => "hose connected",
            "connecting" | "configured" => "hose warn",
            "error" => "hose error",
            _ => "hose warn",
        }
    }

    fn meta_text(&self, rate: Option<TelemetryPeerRateSnapshot>) -> String {
        let mut parts = vec![
            format!("{} attempts", self.connect_attempts),
            format!("{} disconnects", self.disconnects),
            format!("{} payloads", self.payloads),
            format_bytes(self.bytes),
        ];
        if let Some(rate) = rate.filter(|rate| rate.ready) {
            parts.push(rate.traffic_text());
            parts.push(rate.churn_text());
            parts.push(format_rate_window(rate.window_ms));
        }
        if self.last_payload_unix_ms.is_some() {
            parts.push(format!(
                "last payload {}",
                optional_unix_age(self.last_payload_unix_ms)
            ));
        } else if self.last_connected_unix_ms.is_some() {
            parts.push(format!(
                "connected {}",
                optional_unix_age(self.last_connected_unix_ms)
            ));
        }
        if let Some(error) = &self.last_error {
            parts.push(error.clone());
        }
        parts.join(" / ")
    }
}

fn data_hose_detail_text(peers: &[TelemetryPeerStatus], rates: &MeshRateSnapshot) -> String {
    let payloads = peers.iter().map(|peer| peer.payloads).sum::<u64>();
    let bytes = peers.iter().map(|peer| peer.bytes).sum::<u64>();
    if !rates.ready {
        return format!(
            "{} tcp-changes payloads / {} / waiting for rates",
            payloads,
            format_bytes(bytes)
        );
    }

    let mut payloads_per_sec = 0.0;
    let mut bytes_per_sec = 0.0;
    let mut disconnects_per_sec = 0.0;
    for rate in rates.telemetry_peers.values().filter(|rate| rate.ready) {
        payloads_per_sec += rate.payloads_per_sec;
        bytes_per_sec += rate.bytes_per_sec;
        disconnects_per_sec += rate.disconnects_per_sec;
    }

    format!(
        "{} tcp-changes payloads / {} / {} / {} / {}",
        payloads,
        format_bytes(bytes),
        format_bytes_per_sec(true, bytes_per_sec),
        format_count_per_sec(payloads_per_sec, "payloads"),
        format_count_per_sec(disconnects_per_sec, "disconnects")
    )
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MeshAlert {
    #[serde(default)]
    level: String,
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    count: u64,
    #[serde(default)]
    last_seen_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ActivityItem {
    #[serde(default)]
    level: String,
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    count: u64,
    #[serde(default)]
    seen_unix_ms: u64,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    stream_id_text: Option<String>,
    #[serde(default)]
    bytes: Option<u64>,
    #[serde(default)]
    datagrams: Option<u64>,
    #[serde(default)]
    sequence: Option<u64>,
}

impl ActivityItem {
    fn key(&self) -> String {
        format!("{}:{}:{}", self.seen_unix_ms, self.code, self.message)
    }

    fn meta_text(&self) -> String {
        let mut parts = Vec::new();
        parts.push(optional_unix_age(nonzero_u64(self.seen_unix_ms)));
        if let Some(node_id) = &self.node_id {
            parts.push(format!("node {node_id}"));
        }
        if let Some(stream_id) = &self.stream_id_text {
            parts.push(format!("stream {stream_id}"));
        }
        if let Some(bytes) = self.bytes {
            parts.push(format_bytes(bytes));
        }
        if let Some(datagrams) = self.datagrams {
            parts.push(format!("{datagrams} datagrams"));
        }
        if let Some(sequence) = self.sequence {
            parts.push(format!("seq {sequence}"));
        }
        if self.count > 1 {
            parts.push(format!("{} seen", self.count));
        }
        parts.join(" / ")
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ConnectionSnapshot {
    #[serde(default)]
    source_node_id: String,
    #[serde(default)]
    target_addr: String,
    #[serde(default)]
    target_node_id: Option<String>,
    #[serde(default)]
    state: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct EdgeServiceSnapshot {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    continent: String,
    #[serde(default)]
    playback_base_url: Option<String>,
    #[serde(default)]
    active_readers: u64,
    #[serde(default)]
    requests_served: u64,
    #[serde(default)]
    bytes_served: u64,
    #[serde(default)]
    llhls_tail_requests: u64,
    #[serde(default)]
    responses_total: u64,
    #[serde(default)]
    response_errors: u64,
    #[serde(default)]
    response_not_found: u64,
    #[serde(default)]
    last_response_unix_ms: Option<u64>,
    #[serde(default)]
    recent_responses: Vec<EdgeResponseSnapshot>,
    #[serde(default)]
    draining: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct EdgeResponseSnapshot {
    #[serde(default)]
    unix_ms: u64,
    #[serde(default)]
    method: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    content_type: Option<String>,
}

impl EdgeResponseSnapshot {
    fn key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.unix_ms, self.method, self.path, self.status
        )
    }

    fn class_name(&self) -> &'static str {
        if self.status >= 500 {
            "edge-response error"
        } else if self.status >= 400 {
            "edge-response warn"
        } else {
            "edge-response"
        }
    }

    fn path_text(&self) -> String {
        match &self.query {
            Some(query) if !query.is_empty() => format!("{}?{}", self.path, query),
            _ => self.path.clone(),
        }
    }

    fn meta_text(&self) -> String {
        let mut parts = vec![optional_unix_age(nonzero_u64(self.unix_ms))];
        if self.bytes > 0 {
            parts.push(format_bytes(self.bytes));
        }
        if let Some(content_type) = &self.content_type {
            parts.push(content_type.clone());
        }
        parts.join(" / ")
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct StreamTelemetry {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    stream_id_text: String,
    #[serde(default)]
    latest_local_part: Option<u64>,
    #[serde(default)]
    latest_local_part_bytes: Option<usize>,
    #[serde(default)]
    latest_local_part_duration_ms: Option<u64>,
    #[serde(default)]
    latest_local_part_age_ms: Option<u64>,
    #[serde(default)]
    latest_mesh_part: Option<u64>,
    #[serde(default)]
    bytes_received: u64,
    #[serde(default)]
    datagrams_received: u64,
    #[serde(default)]
    last_ingest_age_ms: Option<u64>,
    #[serde(default)]
    stale_threshold_ms: Option<u64>,
}

impl StreamTelemetry {
    fn rate_key(&self) -> String {
        format!("{}:{}", self.node_id, self.stream_id_text)
    }

    fn display_stream_id(&self) -> String {
        if self.stream_id_text.is_empty() {
            "-".to_owned()
        } else {
            self.stream_id_text.clone()
        }
    }

    fn active(&self) -> bool {
        self.latest_local_part.is_some() || self.latest_mesh_part.is_some()
    }

    fn stale(&self) -> bool {
        self.active()
            && self
                .last_ingest_age_ms
                .is_some_and(|age_ms| age_ms > self.stale_threshold_ms.unwrap_or(5_000))
    }

    fn status_text(&self) -> &'static str {
        if self.stale() {
            "stale"
        } else if self.last_ingest_age_ms.is_some() {
            "active"
        } else if self.latest_mesh_part.is_some() {
            "mirrored"
        } else {
            "waiting"
        }
    }

    fn class_name(&self) -> &'static str {
        match self.status_text() {
            "stale" => "stream-row stale",
            "active" => "stream-row active",
            "mirrored" => "stream-row mirrored",
            _ => "stream-row",
        }
    }

    fn age_text(&self) -> String {
        let ingest = self
            .last_ingest_age_ms
            .or(self.latest_local_part_age_ms)
            .map(format_duration_ms)
            .unwrap_or_else(|| "never".to_owned());
        let mut detail = vec![format!("ingest {ingest}")];
        if let Some(duration_ms) = self.latest_local_part_duration_ms {
            detail.push(format!("part {}", format_duration_ms_plain(duration_ms)));
        }
        if let Some(bytes) = self.latest_local_part_bytes {
            detail.push(format!("part {}", format_bytes(bytes as u64)));
        }
        detail.join(" / ")
    }

    fn rate_text(&self, rate: Option<StreamRateSnapshot>) -> String {
        match rate {
            Some(rate) if rate.ready => rate.text(),
            _ if self.active() => "rate waiting".to_owned(),
            _ => "idle".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ReplicaPlacementSnapshot {
    #[serde(default)]
    stream_id_text: String,
    #[serde(default)]
    target_node_id: String,
    #[serde(default)]
    score: f64,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ControlCommand {
    #[serde(default)]
    id: u64,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    stream_id_text: Option<String>,
    #[serde(default)]
    status: String,
}

impl ControlCommand {
    fn status_kind(&self) -> CommandStatusKind {
        CommandStatusKind::from_status(&self.status)
    }

    fn class_name(&self) -> &'static str {
        match self.status_kind() {
            CommandStatusKind::Running => "command running",
            CommandStatusKind::Ok => "command ok",
            CommandStatusKind::Warn => "command warn",
            CommandStatusKind::Error => "command error",
        }
    }

    fn kind_label(&self) -> String {
        command_kind_label(&self.kind)
    }

    fn status_text(&self) -> String {
        if self.status.is_empty() {
            "pending".to_owned()
        } else {
            self.status.clone()
        }
    }

    fn target_text(&self) -> String {
        let target = [
            self.node_id.as_deref(),
            self.region.as_deref(),
            self.stream_id_text.as_deref(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" / ");
        if target.is_empty() {
            "all local targets".to_owned()
        } else {
            target
        }
    }

    fn meta_text(&self) -> String {
        let mut parts = Vec::new();
        if self.id != 0 {
            parts.push(format!("id {}", self.id));
            parts.push(optional_unix_age(Some(self.id)));
        }
        parts.push(self.target_text());
        parts.join(" / ")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommandStatusKind {
    Running,
    Ok,
    Warn,
    Error,
}

impl CommandStatusKind {
    fn from_status(status: &str) -> Self {
        let status = status.to_ascii_lowercase();
        if status.contains("failed") || status.contains("timed out") || status.contains("error") {
            Self::Error
        } else if status.contains("skipped") {
            Self::Warn
        } else if status.contains("accepted")
            || status.contains("running")
            || status.contains("dispatch")
        {
            Self::Running
        } else {
            Self::Ok
        }
    }
}

fn command_health_detail(commands: &[ControlCommand]) -> String {
    let failures = commands
        .iter()
        .filter(|command| command.status_kind() == CommandStatusKind::Error)
        .count();
    let warnings = commands
        .iter()
        .filter(|command| command.status_kind() == CommandStatusKind::Warn)
        .count();
    let running = commands
        .iter()
        .filter(|command| command.status_kind() == CommandStatusKind::Running)
        .count();
    format!("{failures} failed / {warnings} skipped / {running} running")
}

fn latest_command_status(commands: &[ControlCommand], kind: &str) -> String {
    commands
        .iter()
        .find(|command| command_kind_matches(&command.kind, kind))
        .map(|command| match command.status_kind() {
            CommandStatusKind::Running => "running",
            CommandStatusKind::Ok => "ok",
            CommandStatusKind::Warn => "skipped",
            CommandStatusKind::Error => "failed",
        })
        .unwrap_or("none")
        .to_owned()
}

fn latest_command_meta(commands: &[ControlCommand], kind: &str) -> String {
    commands
        .iter()
        .find(|command| command_kind_matches(&command.kind, kind))
        .map(|command| command.meta_text())
        .unwrap_or_else(|| "no provision commands".to_owned())
}

#[derive(Clone, Debug)]
struct ControlPreview {
    level: &'static str,
    summary: String,
    detail: String,
}

impl ControlPreview {
    fn new(level: &'static str, summary: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            level,
            summary: summary.into(),
            detail: detail.into(),
        }
    }

    fn class_name(&self) -> String {
        format!("control-preview-card {}", self.level)
    }
}

#[derive(Clone, Debug)]
struct ControlTargetScope {
    summary: String,
    detail: String,
    visible_count: usize,
    global: bool,
}

fn control_warm_preview(
    mesh: Option<&MeshApiSnapshot>,
    stream_id: &str,
    region: &str,
    node_id: &str,
) -> ControlPreview {
    let target = control_target_scope(mesh, region, node_id);
    let stream_id = stream_id.trim();
    if !valid_stream_id_text(stream_id) {
        return ControlPreview::new(
            "warn",
            "stream missing",
            format!("{} / stream id must be decimal", target.detail),
        );
    }
    let level = if target.global { "warn" } else { "ready" };
    ControlPreview::new(
        level,
        target.summary,
        format!("stream {stream_id} / {}", target.detail),
    )
}

fn control_close_preview(
    mesh: Option<&MeshApiSnapshot>,
    region: &str,
    node_id: &str,
) -> ControlPreview {
    let target = control_target_scope(mesh, region, node_id);
    let level = if target.global {
        "danger"
    } else if target.visible_count == 0 {
        "warn"
    } else {
        "warn"
    };
    let summary = if target.global {
        "global close".to_owned()
    } else {
        target.summary
    };
    ControlPreview::new(level, summary, target.detail)
}

fn control_provision_preview(
    mesh: Option<&MeshApiSnapshot>,
    region: &str,
    node_id: &str,
) -> ControlPreview {
    let Some(mesh) = mesh else {
        return ControlPreview::new("warn", "waiting", "mesh status unavailable");
    };
    let provision = &mesh.orchestration.provision;
    let request = provision_request_text(region, node_id);
    if !provision.enabled {
        return ControlPreview::new(
            "warn",
            "disabled",
            format!("no backend configured / {request}"),
        );
    }

    let ready = provision
        .backend_statuses
        .iter()
        .filter(|backend| backend.state == "ready")
        .count();
    let blocked = provision
        .backend_statuses
        .iter()
        .filter(|backend| backend.state == "blocked" || backend.state == "error")
        .count();
    let backends = if provision.backends.is_empty() {
        "no backend".to_owned()
    } else {
        provision.backends.join(", ")
    };

    if ready > 0 {
        ControlPreview::new(
            "ready",
            format!("{ready} ready"),
            format!("{backends} / local executor / {request}"),
        )
    } else if blocked > 0 {
        ControlPreview::new(
            "danger",
            format!("{blocked} blocked"),
            format!("{backends} / {request}"),
        )
    } else {
        ControlPreview::new("warn", "not ready", format!("{backends} / {request}"))
    }
}

fn control_target_scope(
    mesh: Option<&MeshApiSnapshot>,
    region: &str,
    node_id: &str,
) -> ControlTargetScope {
    let node_id = node_id.trim();
    let region = region.trim();
    if !node_id.is_empty() {
        let observed = mesh
            .map(|mesh| mesh.nodes.iter().any(|node| node.node_id == node_id))
            .unwrap_or(false);
        return ControlTargetScope {
            summary: format!("node {node_id}"),
            detail: if observed {
                "explicit node target observed".to_owned()
            } else {
                "explicit node target not observed in telemetry".to_owned()
            },
            visible_count: usize::from(observed),
            global: false,
        };
    }

    if !region.is_empty() {
        let nodes = mesh
            .map(|mesh| {
                mesh.nodes
                    .iter()
                    .filter(|node| node.region == region)
                    .map(|node| node.node_id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        return ControlTargetScope {
            summary: format!("{} regional nodes", nodes.len()),
            detail: if nodes.is_empty() {
                format!("region {region} has no visible telemetry nodes")
            } else {
                format!("region {region}: {}", nodes.join(", "))
            },
            visible_count: nodes.len(),
            global: false,
        };
    }

    let visible = mesh.map(|mesh| mesh.nodes.len()).unwrap_or_default();
    ControlTargetScope {
        summary: "global".to_owned(),
        detail: format!("{visible} visible nodes; empty target is accepted by AVMC subscribers"),
        visible_count: visible,
        global: true,
    }
}

fn provision_request_text(region: &str, node_id: &str) -> String {
    let mut parts = Vec::new();
    let node_id = node_id.trim();
    let region = region.trim();
    if !node_id.is_empty() {
        parts.push(format!("node {node_id}"));
    }
    if !region.is_empty() {
        parts.push(format!("region {region}"));
    }
    if parts.is_empty() {
        "default request".to_owned()
    } else {
        parts.join(" / ")
    }
}

fn valid_stream_id_text(stream_id: &str) -> bool {
    !stream_id.is_empty() && stream_id.bytes().all(|byte| byte.is_ascii_digit())
}

fn command_kind_matches(left: &str, right: &str) -> bool {
    command_kind_key(left) == command_kind_key(right)
}

fn command_kind_key(value: &str) -> String {
    value
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-' && !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

fn command_kind_label(value: &str) -> String {
    if value.is_empty() {
        return "command".to_owned();
    }
    if value.contains('_') || value.contains('-') {
        return value
            .split(['_', '-'])
            .filter(|part| !part.is_empty())
            .map(capitalize_ascii)
            .collect::<Vec<_>>()
            .join(" ");
    }
    let mut out = String::with_capacity(value.len() + 4);
    for (idx, ch) in value.chars().enumerate() {
        if idx > 0 && ch.is_ascii_uppercase() {
            out.push(' ');
        }
        out.push(ch);
    }
    out
}

fn capitalize_ascii(value: &str) -> String {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::with_capacity(value.len());
    out.extend(first.to_uppercase());
    out.extend(chars);
    out
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ContribStatus {
    #[serde(default)]
    updated_unix_ms: u64,
    #[serde(default)]
    status: String,
    #[serde(default)]
    advertised_hls_stream_id: String,
    #[serde(default)]
    advertised_hls_path: String,
    #[serde(default)]
    mesh: ContribMeshStatus,
    #[serde(default)]
    hls: ContribHlsConfig,
    #[serde(default)]
    fec: ContribFecConfig,
    #[serde(default)]
    listeners: Vec<ListenerStatus>,
    #[serde(default)]
    runtime: ContribRuntimeStatus,
    #[serde(default)]
    health: ContribHealth,
    #[serde(default)]
    alerts: Vec<ContribAlert>,
    #[serde(default)]
    activity: Vec<ActivityItem>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ContribHealth {
    #[serde(default)]
    state: String,
    #[serde(default)]
    stale_threshold_ms: u64,
    #[serde(default)]
    input_seen: bool,
    #[serde(default)]
    fmp4_input_seen: bool,
    #[serde(default)]
    output_seen: bool,
    #[serde(default)]
    last_input_age_ms: Option<u64>,
    #[serde(default)]
    last_fmp4_input_age_ms: Option<u64>,
    #[serde(default)]
    last_output_age_ms: Option<u64>,
}

impl ContribHealth {
    fn detail_text(&self) -> String {
        let input = if self.input_seen {
            optional_age(self.last_input_age_ms)
        } else {
            "no input".to_owned()
        };
        let output = if self.output_seen {
            optional_age(self.last_output_age_ms)
        } else {
            "no output".to_owned()
        };
        let fmp4_input = if self.fmp4_input_seen {
            optional_age(self.last_fmp4_input_age_ms)
        } else {
            "no fmp4 input".to_owned()
        };
        format!(
            "input {input} / fmp4 input {fmp4_input} / output {output} / stale {}",
            format_duration_ms_plain(self.stale_threshold_ms)
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ContribMeshStatus {
    #[serde(default)]
    byte_fec_target: String,
    #[serde(default)]
    media_fec_target: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ContribHlsConfig {
    #[serde(default)]
    part_target_ms: u64,
    #[serde(default)]
    segment_target_ms: u64,
    #[serde(default)]
    playlist_target_duration_ms: u64,
    #[serde(default)]
    playlist_count: usize,
    #[serde(default)]
    playlist_buffer_kb: usize,
}

impl ContribHlsConfig {
    fn part_target_text(&self) -> String {
        if self.part_target_ms == 0 {
            "-".to_owned()
        } else {
            format_duration_ms_plain(self.part_target_ms)
        }
    }

    fn playlist_detail_text(&self) -> String {
        format!(
            "segment {} / playlist target {} / {} playlist(s) / {} KB buffer",
            format_duration_ms_plain(self.segment_target_ms),
            format_duration_ms_plain(self.playlist_target_duration_ms),
            self.playlist_count,
            self.playlist_buffer_kb
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ContribFecConfig {
    #[serde(default)]
    repair_symbols: usize,
    #[serde(default)]
    symbol_size: usize,
}

impl ContribFecConfig {
    fn repair_text(&self) -> String {
        format!("{} repair", self.repair_symbols)
    }

    fn detail_text(&self) -> String {
        format!("{} byte symbols", self.symbol_size)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ListenerStatus {
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    bind: Option<String>,
    #[serde(default)]
    output_stream_id: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ContribRuntimeStatus {
    #[serde(default)]
    raw_http: RawHttpRuntime,
    #[serde(default)]
    media_access_units: MediaRuntime,
    #[serde(default)]
    mesh_forward: MeshForwardRuntime,
    #[serde(default)]
    mpeg_ts: MpegTsRuntime,
    #[serde(default)]
    rtmp: RtmpRuntime,
    #[serde(default)]
    fmp4: Fmp4Runtime,
    #[serde(default)]
    hls: HlsRuntime,
    #[serde(default)]
    ingest_sessions: IngestSessionsRuntime,
    #[serde(default)]
    streams: Vec<ContribStreamRuntime>,
    #[serde(default)]
    protocols: Vec<ProtocolRuntime>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct RawHttpRuntime {
    #[serde(default)]
    requests: u64,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    datagrams: u64,
    #[serde(default)]
    last_seen_age_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MediaRuntime {
    #[serde(default)]
    requests: u64,
    #[serde(default)]
    payload_bytes: u64,
    #[serde(default)]
    datagrams: u64,
    #[serde(default)]
    last_seen_age_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MeshForwardRuntime {
    #[serde(default)]
    stream_payloads: u64,
    #[serde(default)]
    stream_payload_bytes: u64,
    #[serde(default)]
    stream_datagrams: u64,
    #[serde(default)]
    stream_datagram_bytes: u64,
    #[serde(default)]
    stream_errors: u64,
    #[serde(default)]
    stream_last_age_ms: Option<u64>,
    #[serde(default)]
    media_payloads: u64,
    #[serde(default)]
    media_payload_bytes: u64,
    #[serde(default)]
    media_datagrams: u64,
    #[serde(default)]
    media_datagram_bytes: u64,
    #[serde(default)]
    media_errors: u64,
    #[serde(default)]
    media_last_age_ms: Option<u64>,
}

impl MeshForwardRuntime {
    fn payloads(&self) -> u64 {
        self.stream_payloads.saturating_add(self.media_payloads)
    }

    fn datagrams(&self) -> u64 {
        self.stream_datagrams.saturating_add(self.media_datagrams)
    }

    fn errors(&self) -> u64 {
        self.stream_errors.saturating_add(self.media_errors)
    }

    fn payload_bytes(&self) -> u64 {
        self.stream_payload_bytes
            .saturating_add(self.media_payload_bytes)
    }

    fn datagram_bytes(&self) -> u64 {
        self.stream_datagram_bytes
            .saturating_add(self.media_datagram_bytes)
    }

    fn last_age_ms(&self) -> Option<u64> {
        youngest_age([self.stream_last_age_ms, self.media_last_age_ms])
    }

    fn detail_text(&self) -> String {
        format!(
            "{} payload / {} wire / {} datagrams / {} errors / {}",
            format_bytes(self.payload_bytes()),
            format_bytes(self.datagram_bytes()),
            self.datagrams(),
            self.errors(),
            optional_age(self.last_age_ms())
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MpegTsRuntime {
    #[serde(default)]
    slots: u64,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    last_seen_age_ms: Option<u64>,
    #[serde(default)]
    continuity_errors: u64,
    #[serde(default)]
    continuity_dropped_bytes: u64,
    #[serde(default)]
    payload_drops: u64,
    #[serde(default)]
    payload_drop_bytes: u64,
    #[serde(default)]
    last_error_age_ms: Option<u64>,
}

impl MpegTsRuntime {
    fn detail_text(&self) -> String {
        format!(
            "{} / {} continuity / {} drops / {} damaged / last error {} / seen {}",
            format_bytes(self.bytes),
            self.continuity_errors,
            self.payload_drops,
            format_bytes(
                self.continuity_dropped_bytes
                    .saturating_add(self.payload_drop_bytes)
            ),
            optional_age(self.last_error_age_ms),
            optional_age(self.last_seen_age_ms)
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct RtmpRuntime {
    #[serde(default)]
    access_units: u64,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    last_seen_age_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct Fmp4Runtime {
    #[serde(default)]
    parts: u64,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    init_bytes: u64,
    #[serde(default)]
    publish_errors: u64,
    #[serde(default)]
    last_publish_age_ms: Option<u64>,
    #[serde(default)]
    video_codec: Option<String>,
    #[serde(default)]
    video_width: Option<u16>,
    #[serde(default)]
    video_height: Option<u16>,
    #[serde(default)]
    video_parts: u64,
    #[serde(default)]
    video_access_units: u64,
    #[serde(default)]
    audio_codec: Option<String>,
    #[serde(default)]
    audio_parts: u64,
    #[serde(default)]
    audio_access_units: u64,
}

impl Fmp4Runtime {
    fn track_summary(&self) -> String {
        let video = match (&self.video_codec, self.video_width, self.video_height) {
            (Some(codec), Some(width), Some(height)) => format!("{codec} {width}x{height}"),
            (Some(codec), _, _) => codec.clone(),
            _ => "no video".to_owned(),
        };
        let audio = self.audio_codec.as_deref().unwrap_or("no audio");
        format!("{video} / {audio}")
    }

    fn track_detail(&self) -> String {
        format!(
            "{} video parts / {} video AUs / {} audio parts / {} audio AUs",
            self.video_parts, self.video_access_units, self.audio_parts, self.audio_access_units
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HlsRuntime {
    #[serde(default)]
    responses_total: u64,
    #[serde(default)]
    response_errors: u64,
    #[serde(default)]
    response_not_found: u64,
    #[serde(default)]
    last_response_age_ms: Option<u64>,
    #[serde(default)]
    recent_responses: Vec<ContribHlsResponse>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct IngestSessionsRuntime {
    #[serde(default)]
    active: usize,
    #[serde(default)]
    started: u64,
    #[serde(default)]
    ended: u64,
    #[serde(default)]
    recent: Vec<IngestSession>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ContribStreamRuntime {
    #[serde(default)]
    stream_id_text: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    input_units: u64,
    #[serde(default)]
    input_bytes: u64,
    #[serde(default)]
    mesh_payloads: u64,
    #[serde(default)]
    mesh_payload_bytes: u64,
    #[serde(default)]
    mesh_datagrams: u64,
    #[serde(default)]
    mesh_datagram_bytes: u64,
    #[serde(default)]
    mesh_errors: u64,
    #[serde(default)]
    fmp4_parts: u64,
    #[serde(default)]
    fmp4_bytes: u64,
    #[serde(default)]
    fmp4_init_bytes: u64,
    #[serde(default)]
    fmp4_publish_errors: u64,
    #[serde(default)]
    latest_fmp4_sequence: Option<u64>,
    #[serde(default)]
    video_parts: u64,
    #[serde(default)]
    video_access_units: u64,
    #[serde(default)]
    audio_parts: u64,
    #[serde(default)]
    audio_access_units: u64,
    #[serde(default)]
    last_input_age_ms: Option<u64>,
    #[serde(default)]
    last_mesh_forward_age_ms: Option<u64>,
    #[serde(default)]
    last_fmp4_age_ms: Option<u64>,
}

impl ContribStreamRuntime {
    fn class_name(&self) -> &'static str {
        match self.state.as_str() {
            "degraded" => "contrib-stream-row degraded",
            "publishing" => "contrib-stream-row publishing",
            "forwarding" => "contrib-stream-row forwarding",
            "ingesting" => "contrib-stream-row ingesting",
            _ => "contrib-stream-row",
        }
    }

    fn display_stream_id(&self) -> String {
        if self.stream_id_text.is_empty() {
            "-".to_owned()
        } else {
            self.stream_id_text.clone()
        }
    }

    fn input_text(&self) -> String {
        format!(
            "{} units / {}",
            self.input_units,
            format_bytes(self.input_bytes)
        )
    }

    fn mesh_text(&self) -> String {
        let mut parts = vec![
            format!("{} payloads", self.mesh_payloads),
            format_bytes(self.mesh_payload_bytes),
            format!("{} datagrams", self.mesh_datagrams),
            format!("wire {}", format_bytes(self.mesh_datagram_bytes)),
        ];
        if self.mesh_errors > 0 {
            parts.push(format!("{} errors", self.mesh_errors));
        }
        parts.join(" / ")
    }

    fn fmp4_text(&self) -> String {
        let mut parts = vec![
            format!("{} parts", self.fmp4_parts),
            format_bytes(self.fmp4_bytes),
            format!("init {}", format_bytes(self.fmp4_init_bytes)),
        ];
        if let Some(sequence) = self.latest_fmp4_sequence {
            parts.push(format!("seq {sequence}"));
        }
        if self.video_parts > 0 || self.audio_parts > 0 {
            parts.push(format!(
                "{} video parts ({} AU) / {} audio parts ({} AU)",
                self.video_parts,
                self.video_access_units,
                self.audio_parts,
                self.audio_access_units
            ));
        }
        if self.fmp4_publish_errors > 0 {
            parts.push(format!("{} errors", self.fmp4_publish_errors));
        }
        parts.join(" / ")
    }

    fn age_text(&self) -> String {
        format!(
            "input {} / mesh {} / fmp4 {}",
            optional_age(self.last_input_age_ms),
            optional_age(self.last_mesh_forward_age_ms),
            optional_age(self.last_fmp4_age_ms)
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ProtocolRuntime {
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    units: u64,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    active_sessions: usize,
    #[serde(default)]
    ended_sessions: usize,
    #[serde(default)]
    last_seen_age_ms: Option<u64>,
}

impl ProtocolRuntime {
    fn class_name(&self) -> &'static str {
        if self.active_sessions > 0 {
            "protocol active"
        } else if self.units > 0 || self.ended_sessions > 0 {
            "protocol seen"
        } else {
            "protocol idle"
        }
    }

    fn summary_text(&self) -> String {
        format!("{} units / {}", self.units, format_bytes(self.bytes))
    }

    fn rate_text(&self, rate: Option<ProtocolRateSnapshot>) -> String {
        let Some(rate) = rate else {
            return "rate waiting".to_owned();
        };
        if !rate.ready {
            return "rate waiting".to_owned();
        }
        format!(
            "{} / {}",
            format_bytes_per_sec(true, rate.bytes_per_sec),
            format_count_per_sec(rate.units_per_sec, "units")
        )
    }

    fn meta_text(&self, rate: Option<ProtocolRateSnapshot>) -> String {
        let mut parts = vec![
            format!("{} active", self.active_sessions),
            format!("{} ended", self.ended_sessions),
            optional_age(self.last_seen_age_ms),
        ];
        if let Some(rate) = rate.filter(|rate| rate.ready) {
            parts.push(format_rate_window(rate.window_ms));
        }
        parts.join(" / ")
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct IngestSession {
    #[serde(default)]
    session_id: u64,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    stream_id_text: String,
    #[serde(default)]
    output_stream_id_text: Option<String>,
    #[serde(default)]
    output_stream_idx: Option<usize>,
    #[serde(default)]
    peer: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    state: String,
    #[serde(default)]
    started_unix_ms: u64,
    #[serde(default)]
    last_seen_unix_ms: u64,
    #[serde(default)]
    ended_unix_ms: Option<u64>,
    #[serde(default)]
    age_ms: u64,
    #[serde(default)]
    body_slots: u64,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    access_units: u64,
    #[serde(default)]
    end_reason: Option<String>,
}

impl IngestSession {
    fn key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.protocol, self.stream_id_text, self.session_id, self.last_seen_unix_ms, self.state
        )
    }

    fn class_name(&self) -> &'static str {
        if self.state == "active" {
            "ingest-session active"
        } else {
            "ingest-session ended"
        }
    }

    fn title_text(&self) -> String {
        let stream = if let Some(output) = &self.output_stream_id_text {
            format!("stream {} -> {}", self.stream_id_text, output)
        } else {
            format!("stream {}", self.stream_id_text)
        };
        match (&self.peer, &self.path) {
            (Some(peer), Some(path)) => format!("{stream} / {peer} / {path}"),
            (Some(peer), None) => format!("{stream} / {peer}"),
            (None, Some(path)) => format!("{stream} / {path}"),
            (None, None) => stream,
        }
    }

    fn meta_text(&self) -> String {
        let mut parts = vec![
            format!("{} body slots", self.body_slots),
            format!("{} access units", self.access_units),
            format_bytes(self.bytes),
            optional_age(Some(self.age_ms)),
        ];
        if let Some(idx) = self.output_stream_idx {
            parts.push(format!("idx {idx}"));
        }
        if self.started_unix_ms != 0 {
            parts.push(format!(
                "started {}",
                optional_unix_age(Some(self.started_unix_ms))
            ));
        }
        if let Some(ended) = self.ended_unix_ms {
            parts.push(format!("ended {}", optional_unix_age(Some(ended))));
        }
        if let Some(reason) = &self.end_reason {
            parts.push(reason.clone());
        }
        parts.join(" / ")
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ContribHlsResponse {
    #[serde(default)]
    unix_ms: u64,
    #[serde(default)]
    method: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    content_type: Option<String>,
}

impl ContribHlsResponse {
    fn key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.unix_ms, self.method, self.path, self.status
        )
    }

    fn class_name(&self) -> &'static str {
        if self.status >= 500 {
            "hls-response error"
        } else if self.status >= 400 {
            "hls-response warn"
        } else {
            "hls-response"
        }
    }

    fn path_text(&self) -> String {
        match &self.query {
            Some(query) if !query.is_empty() => format!("{}?{}", self.path, query),
            _ => self.path.clone(),
        }
    }

    fn meta_text(&self) -> String {
        let mut parts = vec![optional_unix_age(nonzero_u64(self.unix_ms))];
        if self.bytes > 0 {
            parts.push(format_bytes(self.bytes));
        }
        if let Some(content_type) = &self.content_type {
            parts.push(content_type.clone());
        }
        parts.join(" / ")
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ContribAlert {
    #[serde(default)]
    level: String,
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    count: u64,
    #[serde(default)]
    last_seen_unix_ms: Option<u64>,
}
