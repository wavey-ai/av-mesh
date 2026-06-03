use std::{cell::RefCell, rc::Rc};

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

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(App);
}

#[component]
fn App() -> impl IntoView {
    let (mesh_api, set_mesh_api) = signal(endpoint_from_query("mesh", DEFAULT_MESH_API));
    let (contrib_api, set_contrib_api) =
        signal(endpoint_from_query("contrib", DEFAULT_CONTRIB_API));
    let (mesh, set_mesh) = signal(None::<MeshApiSnapshot>);
    let (contrib, set_contrib) = signal(None::<ContribStatus>);
    let (mesh_rates, set_mesh_rates) = signal(MeshRateSnapshot::default());
    let (contrib_rates, set_contrib_rates) = signal(ContribRateSnapshot::default());
    let (last_mesh_sample, set_last_mesh_sample) = signal(None::<MeshRateSample>);
    let (last_contrib_sample, set_last_contrib_sample) = signal(None::<ContribRateSample>);
    let (status, set_status) = signal(String::from("starting"));
    let (mesh_feed, set_mesh_feed) = signal(String::from("mesh feed starting"));
    let (mesh_events_active, set_mesh_events_active) = signal(false);
    let (contrib_feed, set_contrib_feed) = signal(String::from("contrib feed starting"));
    let (contrib_events_active, set_contrib_events_active) = signal(false);
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
                }
                Some(Err(error)) => errors.push(format!("mesh: {error}")),
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
                }
                Some(Err(error)) => errors.push(format!("contrib: {error}")),
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

            let source = match EventSource::new(&events_url) {
                Ok(source) => source,
                Err(error) => {
                    set_mesh_feed.set(format!("mesh polling: {}", js_error_text(error)));
                    return;
                }
            };

            let event_url = events_url.clone();
            let onmesh =
                Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |event: MessageEvent| {
                    let Some(data) = event.data().as_string() else {
                        set_mesh_events_active.set(false);
                        set_mesh_feed.set("mesh events: non-text payload".to_owned());
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
                        }
                        Err(error) => {
                            set_mesh_events_active.set(false);
                            set_mesh_feed.set(format!("mesh events parse error: {error}"));
                        }
                    }
                }));

            if let Err(error) =
                source.add_event_listener_with_callback("mesh", onmesh.as_ref().unchecked_ref())
            {
                source.close();
                set_mesh_feed.set(format!("mesh polling: {}", js_error_text(error)));
                return;
            }

            let onerror = Closure::<dyn FnMut(Event)>::wrap(Box::new(move |_event: Event| {
                set_mesh_events_active.set(false);
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

            let source = match EventSource::new(&events_url) {
                Ok(source) => source,
                Err(error) => {
                    set_contrib_feed.set(format!("contrib polling: {}", js_error_text(error)));
                    return;
                }
            };

            let event_url = events_url.clone();
            let oncontrib =
                Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |event: MessageEvent| {
                    let Some(data) = event.data().as_string() else {
                        set_contrib_events_active.set(false);
                        set_contrib_feed.set("contrib events: non-text payload".to_owned());
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
                        }
                        Err(error) => {
                            set_contrib_events_active.set(false);
                            set_contrib_feed.set(format!("contrib events parse error: {error}"));
                        }
                    }
                }));

            if let Err(error) = source
                .add_event_listener_with_callback("contrib", oncontrib.as_ref().unchecked_ref())
            {
                source.close();
                set_contrib_feed.set(format!("contrib polling: {}", js_error_text(error)));
                return;
            }

            let onerror = Closure::<dyn FnMut(Event)>::wrap(Box::new(move |_event: Event| {
                set_contrib_events_active.set(false);
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
                </section>

                <div class="workspace">
                    <section class="panel map-panel">
                        <div class="panel-head">
                            <h2>"Topology"</h2>
                            <span>{move || mesh.get().map(|m| format!("updated {} / {} peers / {} links / {} alerts", age_text(m.updated_unix_ms), m.peers.len(), m.connections.len(), m.alerts.len())).unwrap_or_else(|| "waiting".to_owned())}</span>
                        </div>
                        <MeshAlertList mesh />
                        <div class="node-map">
                            <For
                                each=move || mesh.get().map(|m| m.nodes).unwrap_or_default()
                                key=|node| node.node_id.clone()
                                let(node)
                            >
                                <NodeTile node />
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
                        <ContribView contrib />
                    </section>
                </div>

                <div class="workspace lower">
                    <section class="panel">
                        <div class="panel-head">
                            <h2>"Streams"</h2>
                            <span>{move || mesh.get().map(|m| format!("{} observed / {} planned", m.streams.len(), m.planned_replicas.len())).unwrap_or_else(|| "0 observed".to_owned())}</span>
                        </div>
                        <LocalStream mesh />
                        <StreamTable mesh />
                        <ReplicaPlan mesh />
                    </section>

                    <section class="panel">
                        <div class="panel-head">
                            <h2>"Controls"</h2>
                            <span>{move || control_status.get()}</span>
                        </div>
                        <OrchestrationView mesh />
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
                    <EdgeGrid mesh />
                </section>
            </main>
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
fn NodeTile(node: MeshNode) -> impl IntoView {
    let storage_pct = percent(node.used_storage_bytes, node.total_storage_bytes);
    let class = if node.draining {
        "node draining"
    } else {
        "node"
    };
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
            <div class="bar"><i style=format!("width: {:.1}%", storage_pct)></i></div>
            <small>{format!("{} used", format_bytes(node.used_storage_bytes))}</small>
        </article>
    }
}

#[component]
fn ContribView(contrib: ReadSignal<Option<ContribStatus>>) -> impl IntoView {
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
            <div class="runtime-grid">
                <RuntimeCell label="health" value=move || contrib.get().map(|c| c.health.state).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| c.health.detail_text()).unwrap_or_default() />
                <RuntimeCell label="raw http" value=move || contrib.get().map(|c| c.runtime.raw_http.requests.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} / {} datagrams / {}", format_bytes(c.runtime.raw_http.bytes), c.runtime.raw_http.datagrams, optional_age(c.runtime.raw_http.last_seen_age_ms))).unwrap_or_default() />
                <RuntimeCell label="media au" value=move || contrib.get().map(|c| c.runtime.media_access_units.requests.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} / {} datagrams / {}", format_bytes(c.runtime.media_access_units.payload_bytes), c.runtime.media_access_units.datagrams, optional_age(c.runtime.media_access_units.last_seen_age_ms))).unwrap_or_default() />
                <RuntimeCell label="mpeg-ts" value=move || contrib.get().map(|c| c.runtime.mpeg_ts.slots.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} / {}", format_bytes(c.runtime.mpeg_ts.bytes), optional_age(c.runtime.mpeg_ts.last_seen_age_ms))).unwrap_or_default() />
                <RuntimeCell label="rtmp" value=move || contrib.get().map(|c| c.runtime.rtmp.access_units.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} / {}", format_bytes(c.runtime.rtmp.bytes), optional_age(c.runtime.rtmp.last_seen_age_ms))).unwrap_or_default() />
                <RuntimeCell label="fmp4" value=move || contrib.get().map(|c| c.runtime.fmp4.parts.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} media / {} init / {}", format_bytes(c.runtime.fmp4.bytes), format_bytes(c.runtime.fmp4.init_bytes), optional_age(c.runtime.fmp4.last_publish_age_ms))).unwrap_or_default() />
                <RuntimeCell label="errors" value=move || contrib.get().map(|c| c.runtime.fmp4.publish_errors.to_string()).unwrap_or_else(|| "-".to_owned()) detail=move || contrib.get().map(|c| format!("{} alerts", c.alerts.len())).unwrap_or_default() />
            </div>
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
fn LocalStream(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="local-stream">
            <span>{move || mesh.get().map(|m| format!("local stream {}", m.stream.stream_id_text)).unwrap_or_else(|| "local stream -".to_owned())}</span>
            <strong>{move || mesh.get().map(|m| format_bytes(m.stream.bytes_received)).unwrap_or_else(|| "-".to_owned())}</strong>
            <em>{move || mesh.get().map(|m| format!("local {} / mesh {} / {} datagrams / snapshot {}", optional_u64(m.stream.latest_local_part), optional_u64(m.stream.latest_mesh_part), m.stream.datagrams_received, age_text(m.updated_unix_ms))).unwrap_or_default()}</em>
        </div>
    }
}

#[component]
fn StreamTable(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="table">
            <div class="table-head stream-row">
                <span>"stream"</span><span>"node"</span><span>"local"</span><span>"mesh"</span><span>"bytes"</span>
            </div>
            <For
                each=move || mesh.get().map(|m| m.streams).unwrap_or_default()
                key=|stream| format!("{}:{}", stream.node_id, stream.stream_id_text)
                let(stream)
            >
                <div class="stream-row">
                    <span>{stream.display_stream_id()}</span>
                    <span>{stream.node_id}</span>
                    <span>{optional_u64(stream.latest_local_part)}</span>
                    <span>{optional_u64(stream.latest_mesh_part)}</span>
                    <span>{format_bytes(stream.bytes_received)}</span>
                </div>
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
fn OrchestrationView(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
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
                <div class="command">
                    <strong>{command.kind.clone()}</strong>
                    <span>{command.status.clone()}</span>
                    <small>{command.target_text()}</small>
                </div>
            </For>
        </div>
    }
}

#[component]
fn EdgeGrid(mesh: ReadSignal<Option<MeshApiSnapshot>>) -> impl IntoView {
    view! {
        <div class="edge-grid">
            <For
                each=move || mesh.get().map(|m| m.edge_services).unwrap_or_default()
                key=|edge| edge.node_id.clone()
                let(edge)
            >
                <article class=if edge.draining { "edge draining" } else { "edge" }>
                    <div>
                        <strong>{edge.node_id}</strong>
                        <span>{format!("{} / {}", edge.region, edge.continent)}</span>
                    </div>
                    <p>{edge.playback_base_url.unwrap_or_else(|| "no playback url".to_owned())}</p>
                    <div class="edge-stats">
                        <span>{format!("{} readers", edge.active_readers)}</span>
                        <span>{format!("{} served", format_bytes(edge.bytes_served))}</span>
                        <span>{format!("{} tails", edge.llhls_tail_requests)}</span>
                    </div>
                </article>
            </For>
        </div>
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
        set_rates.set(MeshRateSnapshot::from_delta(previous, sample));
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
        set_rates.set(ContribRateSnapshot::from_delta(previous, sample));
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

fn now_unix_ms() -> u64 {
    js_sys::Date::now() as u64
}

#[derive(Clone, Copy, Debug, Default)]
struct MeshRateSample {
    sampled_unix_ms: u64,
    bytes_received: u64,
    datagrams_received: u64,
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
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct MeshRateSnapshot {
    ready: bool,
    window_ms: u64,
    bytes_per_sec: f64,
    datagrams_per_sec: f64,
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

#[derive(Clone, Copy, Debug, Default)]
struct ContribRateSample {
    sampled_unix_ms: u64,
    input_bytes: u64,
    input_datagrams: u64,
    output_bytes: u64,
    output_parts: u64,
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
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ContribRateSnapshot {
    ready: bool,
    window_ms: u64,
    input_bytes_per_sec: f64,
    input_datagrams_per_sec: f64,
    output_bytes_per_sec: f64,
    output_parts_per_sec: f64,
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
struct OrchestrationStatus {
    #[serde(default)]
    control_dispatch_ready: bool,
    #[serde(default)]
    provision: ProvisionStatus,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ProvisionStatus {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    backends: Vec<String>,
    #[serde(default)]
    timeout_ms: u64,
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
    bytes_served: u64,
    #[serde(default)]
    llhls_tail_requests: u64,
    #[serde(default)]
    draining: bool,
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
    latest_mesh_part: Option<u64>,
    #[serde(default)]
    bytes_received: u64,
    #[serde(default)]
    datagrams_received: u64,
}

impl StreamTelemetry {
    fn display_stream_id(&self) -> String {
        if self.stream_id_text.is_empty() {
            "-".to_owned()
        } else {
            self.stream_id_text.clone()
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
    fn target_text(&self) -> String {
        [
            self.node_id.as_deref(),
            self.region.as_deref(),
            self.stream_id_text.as_deref(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" / ")
    }
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
    mpeg_ts: MpegTsRuntime,
    #[serde(default)]
    rtmp: RtmpRuntime,
    #[serde(default)]
    fmp4: Fmp4Runtime,
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
struct MpegTsRuntime {
    #[serde(default)]
    slots: u64,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    last_seen_age_ms: Option<u64>,
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
