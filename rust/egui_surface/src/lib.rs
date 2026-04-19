use eframe::egui;
use eframe::egui::{Align, Layout, RichText, ScrollArea};
use egui_commonmark::{CommonMarkCache, CommonMarkViewer};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DEFAULT_WIDTH_RATIO: f32 = 0.84;
const DEFAULT_HEIGHT_RATIO: f32 = 0.82;
const MIN_WINDOW_WIDTH: f32 = 1080.0;
const MIN_WINDOW_HEIGHT: f32 = 680.0;
const MAX_WINDOW_WIDTH: f32 = 1600.0;
const MAX_WINDOW_HEIGHT: f32 = 900.0;

#[pyclass]
#[derive(Clone)]
struct NativeEguiInfo {
    #[pyo3(get)]
    version: String,
}

#[pyfunction]
#[pyo3(signature = (title=None, home_provider=None))]
fn launch(title: Option<String>, home_provider: Option<Py<PyAny>>) -> PyResult<()> {
    let window_title = title.unwrap_or_else(|| "Data Engine egui Surface".to_string());
    let [default_width, default_height] = default_window_size();
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title(window_title.clone())
            .with_app_id("data-engine-egui")
            .with_inner_size([default_width, default_height])
            .with_min_inner_size([MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT]),
        persist_window: false,
        centered: true,
        ..Default::default()
    };
    eframe::run_native(
        &window_title,
        options,
        Box::new(move |_cc| Ok(Box::new(DataEngineEguiApp::new(home_provider)))),
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

#[pyfunction]
fn hello() -> String {
    "hello from data_engine.ui.egui".to_string()
}

#[pyfunction]
fn runtime_info() -> NativeEguiInfo {
    NativeEguiInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
    }
}

fn default_window_size() -> [f32; 2] {
    #[cfg(target_os = "windows")]
    {
        if let Some((work_width, work_height)) = windows_work_area_size() {
            return [
                clamp_dimension(work_width * DEFAULT_WIDTH_RATIO, MIN_WINDOW_WIDTH, MAX_WINDOW_WIDTH),
                clamp_dimension(work_height * DEFAULT_HEIGHT_RATIO, MIN_WINDOW_HEIGHT, MAX_WINDOW_HEIGHT),
            ];
        }
    }

    [
        clamp_dimension(1360.0, MIN_WINDOW_WIDTH, MAX_WINDOW_WIDTH),
        clamp_dimension(760.0, MIN_WINDOW_HEIGHT, MAX_WINDOW_HEIGHT),
    ]
}

fn clamp_dimension(value: f32, min: f32, max: f32) -> f32 {
    value.clamp(min, max)
}

#[cfg(target_os = "windows")]
fn windows_work_area_size() -> Option<(f32, f32)> {
    use winapi::shared::windef::RECT;
    use winapi::um::winuser::{SystemParametersInfoW, SPI_GETWORKAREA};

    let mut rect = RECT {
        left: 0,
        top: 0,
        right: 0,
        bottom: 0,
    };

    let success = unsafe {
        SystemParametersInfoW(
            SPI_GETWORKAREA,
            0,
            (&mut rect as *mut RECT).cast(),
            0,
        )
    };
    if success == 0 {
        return None;
    }

    let width = (rect.right - rect.left).max(0) as f32;
    let height = (rect.bottom - rect.top).max(0) as f32;
    if width <= 0.0 || height <= 0.0 {
        None
    } else {
        Some((width, height))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SurfaceView {
    Home,
    Debug,
    Docs,
    Settings,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeEngineState {
    label: String,
    enabled: bool,
    state: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeRequestControlState {
    label: String,
    enabled: bool,
    visible: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeRefreshState {
    enabled: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeFlowSummaryRow {
    label: String,
    value: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeFlowListItem {
    flow_name: String,
    title: String,
    secondary: String,
    state: String,
    group_name: String,
    valid: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeFlowGroup {
    group_name: String,
    title: String,
    secondary: String,
    flows: Vec<HomeFlowListItem>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeStepItem {
    number: usize,
    title: String,
    status: String,
    duration: String,
    inspectable: bool,
    active_count: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeLogItem {
    timestamp: String,
    label: String,
    duration: String,
    state: String,
    inspectable: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeFlowActions {
    flow_run_label: String,
    flow_run_enabled: bool,
    flow_config_enabled: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeFlowDetail {
    flow_name: String,
    title: String,
    description: String,
    error: String,
    group_name: String,
    summary_rows: Vec<HomeFlowSummaryRow>,
    steps: Vec<HomeStepItem>,
    logs: Vec<HomeLogItem>,
    actions: HomeFlowActions,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HomeViewState {
    workspace_ids: Vec<String>,
    selected_workspace_id: String,
    workspace_root: String,
    empty_message: String,
    engine: HomeEngineState,
    request_control: HomeRequestControlState,
    refresh: HomeRefreshState,
    flow_groups: Vec<HomeFlowGroup>,
    flows: std::collections::HashMap<String, HomeFlowDetail>,
    default_selected_flow_name: Option<String>,
}

#[derive(Clone)]
struct DebugArtifact {
    name: &'static str,
    kind: &'static str,
    source: &'static str,
    path: &'static str,
    metadata: Vec<(&'static str, &'static str)>,
}

struct DocPage {
    title: String,
    relative_path: String,
    markdown: String,
}

struct DataEngineEguiApp {
    current_view: SurfaceView,
    home_provider: Option<Py<PyAny>>,
    home_state: HomeViewState,
    selected_flow_name: Option<String>,
    selected_workspace_id: Option<String>,
    last_home_poll: Instant,
    selected_artifact: usize,
    selected_doc: usize,
    artifacts: Vec<DebugArtifact>,
    docs: Vec<DocPage>,
    docs_cache: CommonMarkCache,
}

impl DataEngineEguiApp {
    fn new(home_provider: Option<Py<PyAny>>) -> Self {
        let mut app = Self {
            current_view: SurfaceView::Home,
            home_provider,
            home_state: HomeViewState::default(),
            selected_flow_name: None,
            selected_workspace_id: None,
            last_home_poll: Instant::now() - Duration::from_secs(1),
            selected_artifact: 0,
            selected_doc: 0,
            artifacts: sample_artifacts(),
            docs: load_doc_pages(),
            docs_cache: CommonMarkCache::default(),
        };
        app.poll_home_state(true);
        app
    }

    fn selected_artifact(&self) -> &DebugArtifact {
        &self.artifacts[self.selected_artifact]
    }

    fn selected_doc(&self) -> Option<&DocPage> {
        self.docs.get(self.selected_doc)
    }

    fn selected_flow(&self) -> Option<&HomeFlowDetail> {
        let selected_name = self
            .selected_flow_name
            .as_ref()
            .or(self.home_state.default_selected_flow_name.as_ref())?;
        self.home_state.flows.get(selected_name)
    }

    fn poll_home_state(&mut self, force: bool) {
        if !force && self.last_home_poll.elapsed() < Duration::from_millis(100) {
            return;
        }
        self.last_home_poll = Instant::now();
        let Some(provider) = &self.home_provider else {
            return;
        };
        let payload = Python::attach(|py| -> PyResult<String> {
            provider
                .bind(py)
                .call_method0("snapshot_json")?
                .extract::<String>()
        });
        let Ok(text) = payload else {
            return;
        };
        let Ok(state) = serde_json::from_str::<HomeViewState>(&text) else {
            return;
        };
        if self.selected_workspace_id.as_deref() != Some(state.selected_workspace_id.as_str()) {
            self.selected_workspace_id = Some(state.selected_workspace_id.clone());
        }
        let keep_current_selection = self
            .selected_flow_name
            .as_ref()
            .map(|name| state.flows.contains_key(name))
            .unwrap_or(false);
        if !keep_current_selection {
            self.selected_flow_name = state.default_selected_flow_name.clone();
        }
        self.home_state = state;
    }

    fn select_workspace(&mut self, workspace_id: &str) {
        if self.selected_workspace_id.as_deref() == Some(workspace_id) {
            return;
        }
        self.selected_workspace_id = Some(workspace_id.to_string());
        if let Some(provider) = &self.home_provider {
            Python::attach(|py| {
                let _ = provider
                    .bind(py)
                    .call_method1("select_workspace", (workspace_id.to_string(),));
            });
        }
        self.poll_home_state(true);
    }

    fn render_nav(&mut self, ui: &mut egui::Ui) {
        egui::Panel::left("nav_rail")
            .resizable(false)
            .default_size(120.0)
            .min_size(120.0)
            .show_inside(ui, |ui| {
                ui.heading("Data Engine");
                ui.label(RichText::new("egui").small());
                ui.add_space(12.0);

                ui.selectable_value(&mut self.current_view, SurfaceView::Home, "Home");
                ui.selectable_value(&mut self.current_view, SurfaceView::Debug, "Debug");
                ui.selectable_value(&mut self.current_view, SurfaceView::Docs, "Docs");
                ui.selectable_value(&mut self.current_view, SurfaceView::Settings, "Settings");

                ui.with_layout(Layout::bottom_up(Align::LEFT), |ui| {
                    ui.separator();
                    ui.label(RichText::new("v0.3.0").small());
                });
            });
    }

    fn render_home_top_bar(&mut self, ui: &mut egui::Ui) {
        self.poll_home_state(false);
        let workspace_ids = self.home_state.workspace_ids.clone();
        egui::Panel::top("home_top_bar").show_inside(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                let _ = ui.add_enabled(false, egui::Button::new(self.home_state.engine.label.clone()));
                if self.home_state.request_control.visible {
                    let _ = ui.add_enabled(
                        false,
                        egui::Button::new(self.home_state.request_control.label.clone()),
                    );
                }
                egui::ComboBox::from_label("Workspace")
                    .selected_text(
                        self.selected_workspace_id
                            .as_deref()
                            .unwrap_or(self.home_state.selected_workspace_id.as_str()),
                    )
                    .show_ui(ui, |ui| {
                        for workspace in &workspace_ids {
                            let selected =
                                self.selected_workspace_id.as_deref() == Some(workspace.as_str());
                            if ui.selectable_label(selected, workspace).clicked() {
                                self.select_workspace(workspace);
                            }
                        }
                    });
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    let _ = ui.add_enabled(false, egui::Button::new("Theme"));
                    if ui
                        .add_enabled(self.home_state.refresh.enabled, egui::Button::new("Refresh"))
                        .clicked()
                    {
                        if let Some(provider) = &self.home_provider {
                            Python::attach(|py| {
                                let _ = provider.bind(py).call_method0("refresh");
                            });
                        }
                        self.poll_home_state(true);
                    }
                });
            });
        });
    }

    fn render_home(&mut self, ui: &mut egui::Ui) {
        self.render_home_top_bar(ui);
        egui::CentralPanel::default().show_inside(ui, |ui| {
            ui.heading("Flow Control");
            if !self.home_state.workspace_root.is_empty() {
                ui.label(RichText::new(self.home_state.workspace_root.clone()).small());
            }
            if !self.home_state.empty_message.is_empty() {
                ui.label(RichText::new(self.home_state.empty_message.clone()).small());
            }
            ui.add_space(8.0);

            ui.columns(3, |columns| {
                self.render_flows_panel(&mut columns[0]);
                self.render_steps_panel(&mut columns[1]);
                self.render_logs_panel(&mut columns[2]);
            });
        });
    }

    fn render_flows_panel(&mut self, ui: &mut egui::Ui) {
        let flow_groups = self.home_state.flow_groups.clone();
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.heading("Configured Flows");
            ui.add_space(6.0);
            ScrollArea::vertical().id_salt("flows_scroll").show(ui, |ui| {
                for (group_index, group) in flow_groups.iter().enumerate() {
                    let flow_count = group.flows.len();
                    egui::CollapsingHeader::new(format!("{} ({flow_count})", group.title))
                        .id_salt(("flow_group", group_index, group.title.as_str()))
                        .default_open(true)
                        .show(ui, |ui| {
                            for (flow_index, flow) in group.flows.iter().enumerate() {
                                ui.push_id(("flow_row", group_index, flow_index, flow.flow_name.as_str()), |ui| {
                                    let is_selected = self.selected_flow_name.as_deref() == Some(flow.flow_name.as_str());
                                    let response = ui.selectable_label(is_selected, flow.title.clone());
                                    if response.clicked() {
                                        self.selected_flow_name = Some(flow.flow_name.clone());
                                    }
                                    ui.indent(format!("flow-meta-{group_index}-{flow_index}"), |ui| {
                                        ui.label(RichText::new(flow.secondary.clone()).small());
                                        ui.label(RichText::new(flow.state.clone()).small().italics());
                                    });
                                    ui.add_space(6.0);
                                });
                            }
                        });
                    ui.add_space(6.0);
                }
            });
        });
    }

    fn render_steps_panel(&mut self, ui: &mut egui::Ui) {
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Steps");
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    let selected_actions = self.selected_flow().map(|flow| flow.actions.clone()).unwrap_or_default();
                    let _ = ui.add_enabled(false, egui::Button::new("View Config"));
                    let _ = ui.add_enabled(false, egui::Button::new(selected_actions.flow_run_label));
                });
            });
            if let Some(flow) = self.selected_flow() {
                ui.label(RichText::new(flow.title.clone()).small());
                if !flow.description.is_empty() {
                    ui.label(RichText::new(flow.description.clone()).small());
                }
            }
            ui.add_space(6.0);

            ScrollArea::vertical().id_salt("steps_scroll").show(ui, |ui| {
                let steps = self
                    .selected_flow()
                    .map(|flow| flow.steps.clone())
                    .unwrap_or_default();
                for step in &steps {
                    ui.push_id(("step_row", step.number, step.title.as_str()), |ui| {
                        egui::Frame::group(ui.style()).show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(RichText::new(format!("{:02}", step.number)).monospace());
                                ui.label(RichText::new(step.title.clone()).strong());
                                if !step.status.is_empty() {
                                    ui.label(RichText::new(step.status.clone()).small());
                                }
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    if !step.duration.is_empty() {
                                        ui.label(step.duration.clone());
                                    }
                                    if step.inspectable {
                                        let _ = ui.add_enabled(false, egui::Button::new("Inspect"));
                                    }
                                });
                            });
                        });
                        ui.add_space(4.0);
                    });
                }
            });
        });
    }

    fn render_logs_panel(&mut self, ui: &mut egui::Ui) {
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Logs");
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    let _ = ui.add_enabled(false, egui::Button::new("Reset Flow"));
                });
            });
            ui.add_space(6.0);

            ScrollArea::vertical().id_salt("logs_scroll").show(ui, |ui| {
                let logs = self
                    .selected_flow()
                    .map(|flow| flow.logs.clone())
                    .unwrap_or_default();
                for log in &logs {
                    ui.push_id(("log_row", log.timestamp.as_str(), log.label.as_str()), |ui| {
                        egui::Frame::group(ui.style()).show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(RichText::new(log.timestamp.clone()).monospace());
                                ui.label(log.label.clone());
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    ui.label(log.state.clone());
                                    if log.inspectable {
                                        let _ = ui.add_enabled(false, egui::Button::new("Inspect"));
                                    }
                                    if !log.duration.is_empty() {
                                        ui.label(log.duration.clone());
                                    }
                                });
                            });
                        });
                        ui.add_space(4.0);
                    });
                }
            });
        });
    }

    fn render_debug(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show_inside(ui, |ui| {
            ui.heading("Debug");
            ui.label("Saved artifact viewer shell.");
            ui.add_space(8.0);

            ui.columns(2, |columns| {
                egui::Frame::group(columns[0].style()).show(&mut columns[0], |ui| {
                    ui.horizontal(|ui| {
                        ui.heading("Saved Artifacts");
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            let _ = ui.button("Clear");
                        });
                    });
                    ui.add_space(6.0);
                    ScrollArea::vertical().id_salt("debug_artifacts_scroll").show(ui, |ui| {
                        for (index, artifact) in self.artifacts.iter().enumerate() {
                            if ui
                                .selectable_label(self.selected_artifact == index, artifact.name)
                                .clicked()
                            {
                                self.selected_artifact = index;
                            }
                            ui.label(RichText::new(artifact.kind).small());
                            ui.add_space(6.0);
                        }
                    });
                });

                egui::Frame::group(columns[1].style()).show(&mut columns[1], |ui| {
                    let artifact = self.selected_artifact();
                    ui.heading("Artifact Preview");
                    ui.label(RichText::new(artifact.path).small().monospace());
                    ui.label(RichText::new(artifact.source).small());
                    ui.separator();
                    ui.label("Table preview placeholder");
                    egui::Grid::new("artifact_preview_grid")
                        .striped(true)
                        .show(ui, |ui| {
                            ui.strong("column");
                            ui.strong("value");
                            ui.end_row();
                            ui.label("artifact_kind");
                            ui.label(artifact.kind);
                            ui.end_row();
                            ui.label("example_rows");
                            ui.label("4");
                            ui.end_row();
                        });
                    ui.add_space(12.0);
                    ui.heading("Metadata");
                    egui::Grid::new("artifact_metadata_grid")
                        .striped(true)
                        .show(ui, |ui| {
                            ui.strong("field");
                            ui.strong("value");
                            ui.end_row();
                            for (field, value) in &artifact.metadata {
                                ui.label(*field);
                                ui.label(*value);
                                ui.end_row();
                            }
                        });
                });
            });
        });
    }

    fn render_docs(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show_inside(ui, |ui| {
            ui.heading("Documentation");
            ui.add_space(12.0);
            let available = ui.available_size_before_wrap();
            let nav_width = (available.x * 0.33).clamp(220.0, 360.0);
            let content_width = (available.x - nav_width - 12.0).max(320.0);
            ui.horizontal(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(nav_width, available.y),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        egui::Frame::group(ui.style()).show(ui, |ui| {
                            ui.heading("Guides");
                            ui.label(RichText::new("CommonMark-backed docs view").small());
                            ui.add_space(8.0);
                            ScrollArea::vertical().id_salt("docs_selector_scroll").show(ui, |ui| {
                                for (index, doc) in self.docs.iter().enumerate() {
                                    if ui
                                        .selectable_label(self.selected_doc == index, &doc.title)
                                        .clicked()
                                    {
                                        self.selected_doc = index;
                                    }
                                    ui.add_space(6.0);
                                }
                            });
                        });
                    },
                );

                ui.add_space(12.0);

                ui.allocate_ui_with_layout(
                    egui::vec2(content_width, available.y),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        egui::Frame::group(ui.style()).show(ui, |ui| {
                            if let Some(doc) = self.selected_doc() {
                                let title = doc.title.clone();
                                let relative_path = doc.relative_path.clone();
                                let markdown = doc.markdown.clone();

                                ui.heading(&title);
                                ui.push_id(("docs_markdown", &relative_path), |ui| {
                                    CommonMarkViewer::new().show_scrollable(
                                        ("docs_markdown_scroll", &relative_path),
                                        ui,
                                        &mut self.docs_cache,
                                        &markdown,
                                    );
                                });
                            } else {
                                ui.label("No Markdown guides were found.");
                            }
                        });
                    },
                );
            });
        });
    }

    fn render_settings(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show_inside(ui, |ui| {
            ui.heading("Settings");
            ui.add_space(8.0);
            ui.columns(2, |columns| {
                egui::Frame::group(columns[0].style()).show(&mut columns[0], |ui| {
                    ui.heading("Workspace Folder");
                    ui.label("Choose the folder that contains your workspaces.");
                    ui.separator();
                    ui.label("Current root");
                    let mut workspace_root = String::from("C:\\DEV_PROJECT\\data-engine\\workspaces");
                    let _ = ui.add(
                        egui::TextEdit::singleline(&mut workspace_root)
                            .interactive(false)
                            .desired_width(f32::INFINITY),
                    );
                    let _ = ui.button("Browse...");
                    ui.add_space(8.0);
                    ui.heading("Provision Selected Workspace");
                    ui.label("Choose the workspace to provision here.");
                    let mut workspace_name = self
                        .selected_workspace_id
                        .clone()
                        .unwrap_or_else(|| self.home_state.selected_workspace_id.clone());
                    let _ = ui.add(
                        egui::TextEdit::singleline(&mut workspace_name)
                            .interactive(false)
                            .desired_width(f32::INFINITY),
                    );
                    let _ = ui.button("Provision Selected Workspace");
                });

                egui::Frame::group(columns[1].style()).show(&mut columns[1], |ui| {
                    ui.heading("Workspace Visibility");
                    ui.label("Read-only environment and daemon details.");
                    ui.separator();
                    egui::Grid::new("settings_visibility_grid")
                        .num_columns(2)
                        .spacing([12.0, 6.0])
                        .show(ui, |ui| {
                            ui.label("Environment");
                            ui.label("Virtualenv");
                            ui.end_row();
                            ui.label("Executable");
                            ui.label("C:\\DEV_PROJECT\\data-engine\\.venv\\Scripts\\python.exe");
                            ui.end_row();
                        });
                    ui.add_space(12.0);
                    ui.heading("Emergency");
                    ui.label("Use only if the selected workspace daemon stops responding.");
                    let _ = ui.button("Force Stop Daemon");
                    let _ = ui.button("Reset Workspace");
                });
            });
        });
    }

    fn render_status_bar(&mut self, ui: &mut egui::Ui) {
        egui::Panel::bottom("status_bar")
            .resizable(false)
            .show_inside(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new("groups · flows · runs").small());
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        ui.label(RichText::new("v0.3.0").small());
                    });
                });
            });
    }
}

impl eframe::App for DataEngineEguiApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.render_nav(ui);
        self.render_status_bar(ui);
        match self.current_view {
            SurfaceView::Home => self.render_home(ui),
            SurfaceView::Debug => self.render_debug(ui),
            SurfaceView::Docs => self.render_docs(ui),
            SurfaceView::Settings => self.render_settings(ui),
        }
    }
}

#[pymodule]
fn _data_engine_egui(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeEguiInfo>()?;
    module.add_function(wrap_pyfunction!(hello, module)?)?;
    module.add_function(wrap_pyfunction!(launch, module)?)?;
    module.add_function(wrap_pyfunction!(runtime_info, module)?)?;
    Ok(())
}

fn sample_artifacts() -> Vec<DebugArtifact> {
    vec![
        DebugArtifact {
            name: "example_schedule__Read Excel__2026-04-19T09-22-14",
            kind: "parquet + json",
            source: "Source: C:\\DEV_PROJECT\\data-engine\\data2\\Input\\claims_flat\\claims_flat_1.xlsx",
            path: "C:\\DEV_PROJECT\\data-engine\\debug_artifacts\\example_schedule__Read Excel__2026-04-19T09-22-14.parquet",
            metadata: vec![
                ("flow_name", "example_schedule"),
                ("step_name", "Read Excel"),
                ("row_count", "4"),
                ("saved_at_utc", "2026-04-19T13:22:14Z"),
            ],
        },
        DebugArtifact {
            name: "claims2_parallel_poll__Build Workflow Summary__2026-04-19T09-25-03",
            kind: "parquet + json",
            source: "Source: C:\\DEV_PROJECT\\data-engine\\data2\\Input\\claims_flat\\claims_flat_2.xlsx",
            path: "C:\\DEV_PROJECT\\data-engine\\debug_artifacts\\claims2_parallel_poll__Build Workflow Summary__2026-04-19T09-25-03.parquet",
            metadata: vec![
                ("flow_name", "claims2_parallel_poll"),
                ("step_name", "Build Workflow Summary"),
                ("row_count", "128"),
                ("saved_at_utc", "2026-04-19T13:25:03Z"),
            ],
        },
    ]
}

fn load_doc_pages() -> Vec<DocPage> {
    let docs_root = repo_root()
        .join("src")
        .join("data_engine")
        .join("docs")
        .join("sphinx_source")
        .join("guides");

    let mut pages = ordered_doc_specs()
        .into_iter()
        .map(|filename| {
            let path = docs_root.join(filename);
            let markdown = fs::read_to_string(&path).unwrap_or_else(|error| {
                format!(
                    "# Missing document\n\nCould not load `{}`.\n\nError: {}",
                    path.display(),
                    error
                )
            });
            DocPage {
                title: extract_doc_title(&markdown, filename),
                relative_path: format!("src/data_engine/docs/sphinx_source/guides/{filename}"),
                markdown,
            }
        })
        .collect::<Vec<_>>();

    if pages.is_empty() {
        pages.push(DocPage {
            title: "Documentation".to_string(),
            relative_path: "src/data_engine/docs/sphinx_source/guides".to_string(),
            markdown: "# Documentation\n\nNo Markdown guides were found.".to_string(),
        });
    }
    pages
}

fn ordered_doc_specs() -> Vec<&'static str> {
    vec![
        "getting-started.md",
        "core-concepts.md",
        "configuring-flows.md",
        "authoring-flow-modules.md",
        "flow-methods.md",
        "database-methods.md",
        "duckdb-helpers.md",
        "recipes.md",
        "app-runtime-and-workspaces.md",
        "flow-context.md",
        "project-map.md",
        "project-inventory.md",
    ]
}

fn extract_doc_title(markdown: &str, fallback_filename: &str) -> String {
    markdown
        .lines()
        .find_map(|line| line.strip_prefix("# ").map(str::trim))
        .filter(|title| !title.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| fallback_filename.trim_end_matches(".md").replace('-', " "))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}
