const state = {
  bootstrap: window.APP_BOOTSTRAP || {},
  currentDir: "",
  entries: [],
  appChoices: [],
  activeDownloadJobs: [],
  downloadPollTimer: null,
  downloadPollInFlight: false,
  uploadTargetPickerDir: "",
  uploadTargetSelection: null,
  uploadSourceSelection: null,
  uploadServerSourcePickerDir: "",
  uploadServerSourceSelection: null,
  downloadTargetPickerDir: "",
  downloadTargetSelection: null,
  pendingDownloadEntries: null,
};

const ACTIVE_DOWNLOAD_JOB_KEY = "pypang.activeDownloadJobs";

const els = {};

document.addEventListener("DOMContentLoaded", () => {
  if (!document.getElementById("settings-form")) {
    return;
  }

  bindElements();
  hideUploadProgress();
  hideServerDownloadProgress();
  bindEvents();
  renderDocs(state.bootstrap.official_docs || []);
  resumeServerDownloadJob();
  refreshStatus({ silent: true }).catch((error) => toast(error.message, true));
});

function bindElements() {
  els.settingsForm = document.getElementById("settings-form");
  els.appPreset = document.getElementById("app-preset");
  els.appKey = document.getElementById("app-key");
  els.secretKey = document.getElementById("secret-key");
  els.appId = document.getElementById("app-id");
  els.appName = document.getElementById("app-name");
  els.appRoot = document.getElementById("app-root");
  els.redirectUri = document.getElementById("redirect-uri");
  els.membershipTier = document.getElementById("membership-tier");
  els.uploadChunkMb = document.getElementById("upload-chunk-mb");
  els.cliDownloadWorkers = document.getElementById("cli-download-workers");
  els.webDownloadWorkers = document.getElementById("web-download-workers");
  els.singleFileParallelEnabled = document.getElementById("single-file-parallel-enabled");
  els.singleFileDownloadWorkers = document.getElementById("single-file-download-workers");
  els.customSettingsHint = document.getElementById("custom-settings-hint");
  els.readyForApi = document.getElementById("ready-for-api");
  els.authorizedStatus = document.getElementById("authorized-status");
  els.appRootLabel = document.getElementById("app-root-label");
  els.heroAppRoot = document.getElementById("hero-app-root");
  els.authButton = document.getElementById("auth-button");
  els.manualAuthForm = document.getElementById("manual-auth-form");
  els.manualAuthCode = document.getElementById("manual-auth-code");
  els.openControlCenterButton = document.getElementById("open-control-center-button");
  els.controlCenterDialog = document.getElementById("control-center-dialog");
  els.refreshTokenButton = document.getElementById("refresh-token-button");
  els.logoutButton = document.getElementById("logout-button");
  els.profileName = document.getElementById("profile-name");
  els.profileVip = document.getElementById("profile-vip");
  els.profileUk = document.getElementById("profile-uk");
  els.avatarImage = document.getElementById("avatar-image");
  els.quotaBar = document.getElementById("quota-bar");
  els.quotaUsed = document.getElementById("quota-used");
  els.quotaTotal = document.getElementById("quota-total");
  els.folderForm = document.getElementById("folder-form");
  els.folderName = document.getElementById("folder-name");
  els.uploadForm = document.getElementById("upload-form");
  els.uploadSourceLocation = document.getElementById("upload-source-location");
  els.uploadFiles = document.getElementById("upload-files");
  els.uploadFolders = document.getElementById("upload-folders");
  els.openUploadSourceButton = document.getElementById("open-upload-source-button");
  els.uploadSourceSummary = document.getElementById("upload-source-summary");
  els.uploadTargetSummary = document.getElementById("upload-target-summary");
  els.uploadSourceSelectionText = document.getElementById("upload-source-selection-text");
  els.uploadSourcePanelTitle = document.getElementById("upload-source-panel-title");
  els.uploadSourceLocalActions = document.getElementById("upload-source-local-actions");
  els.uploadLocalKind = document.getElementById("upload-local-kind");
  els.uploadLocalSelectionList = document.getElementById("upload-local-selection-list");
  els.pickLocalSourceButton = document.getElementById("pick-local-source-button");
  els.uploadSourceDialog = document.getElementById("upload-source-dialog");
  els.uploadSourceLocalPanel = document.getElementById("upload-source-local-panel");
  els.uploadSourceServerPanel = document.getElementById("upload-source-server-panel");
  els.uploadTargetPath = document.getElementById("upload-target-path");
  els.openUploadTargetButton = document.getElementById("open-upload-target-button");
  els.uploadPolicy = document.getElementById("upload-policy");
  els.uploadProgressWrap = document.getElementById("upload-progress-wrap");
  els.uploadProgressBar = document.getElementById("upload-progress-bar");
  els.uploadProgressText = document.getElementById("upload-progress-text");
  els.uploadProgressEmpty = document.getElementById("upload-progress-empty");
  els.uploadMonitorDetails = document.getElementById("upload-monitor-details");
  els.uploadSummaryText = document.getElementById("upload-summary-text");
  els.uploadSummaryPercent = document.getElementById("upload-summary-percent");
  els.uploadTargetDialog = document.getElementById("upload-target-dialog");
  els.uploadTargetBreadcrumbs = document.getElementById("upload-target-breadcrumbs");
  els.uploadTargetList = document.getElementById("upload-target-list");
  els.uploadTargetSelectionText = document.getElementById("upload-target-selection-text");
  els.uploadTargetUseCurrentButton = document.getElementById("upload-target-use-current-button");
  els.uploadTargetConfirmButton = document.getElementById("upload-target-confirm-button");
  els.uploadServerSourceBreadcrumbs = document.getElementById("upload-server-source-breadcrumbs");
  els.uploadServerSourceList = document.getElementById("upload-server-source-list");
  els.downloadSpeedCard = document.getElementById("download-speed-card");
  els.downloadSpeedDetails = document.getElementById("download-speed-details");
  els.downloadMonitorDetails = document.getElementById("download-monitor-details");
  els.transferOverviewTitle = document.getElementById("transfer-overview-title");
  els.transferOverviewPercent = document.getElementById("transfer-overview-percent");
  els.transferOverviewBar = document.getElementById("transfer-overview-bar");
  els.transferOverviewDetail = document.getElementById("transfer-overview-detail");
  els.downloadSpeedTotal = document.getElementById("download-speed-total");
  els.downloadProgressText = document.getElementById("download-progress-text");
  els.serverDownloadProgressBar = document.getElementById("server-download-progress-bar");
  els.serverDownloadProgressText = document.getElementById("server-download-progress-text");
  els.downloadActiveList = document.getElementById("download-active-list");
  els.downloadWaitingList = document.getElementById("download-waiting-list");
  els.downloadCompletedList = document.getElementById("download-completed-list");
  els.docList = document.getElementById("doc-list");
  els.createFolderButton = document.getElementById("create-folder-button");
  els.openSelectedButton = document.getElementById("open-selected-button");
  els.downloadSelectedButton = document.getElementById("download-selected-button");
  els.downloadServerButton = document.getElementById("download-server-button");
  els.downloadTargetDialog = document.getElementById("download-target-dialog");
  els.downloadTargetBreadcrumbs = document.getElementById("download-target-breadcrumbs");
  els.downloadTargetList = document.getElementById("download-target-list");
  els.downloadTargetSelectionText = document.getElementById("download-target-selection-text");
  els.downloadTargetUseCurrentButton = document.getElementById("download-target-use-current-button");
  els.downloadTargetConfirmButton = document.getElementById("download-target-confirm-button");
  els.renameSelectedButton = document.getElementById("rename-selected-button");
  els.moveSelectedButton = document.getElementById("move-selected-button");
  els.reloadButton = document.getElementById("reload-button");
  els.deleteSelectedButton = document.getElementById("delete-selected-button");
  els.noticeBar = document.getElementById("notice-bar");
  els.breadcrumbs = document.getElementById("breadcrumbs");
  els.fileTableBody = document.getElementById("file-table-body");
  els.selectAll = document.getElementById("select-all");
  els.toast = document.getElementById("toast");
  els.renameDialog = document.getElementById("rename-dialog");
  els.renameForm = document.getElementById("rename-form");
  els.renamePath = document.getElementById("rename-path");
  els.renameName = document.getElementById("rename-name");
  els.moveDialog = document.getElementById("move-dialog");
  els.moveForm = document.getElementById("move-form");
  els.movePath = document.getElementById("move-path");
  els.moveDestination = document.getElementById("move-destination");
  els.moveNewName = document.getElementById("move-new-name");
}

function bindEvents() {
  els.settingsForm.addEventListener("submit", saveSettings);
  els.openControlCenterButton.addEventListener("click", () => showDialog(els.controlCenterDialog));
  els.appPreset.addEventListener("change", applyAppPreset);
  els.membershipTier.addEventListener("change", () => {
    updateMembershipPlaceholders();
    updateCustomSettingsHint();
  });
  els.singleFileParallelEnabled.addEventListener("change", updateCustomSettingsHint);
  els.authButton.addEventListener("click", interceptAuth);
  els.manualAuthForm.addEventListener("submit", submitManualCode);
  els.refreshTokenButton.addEventListener("click", refreshToken);
  els.logoutButton.addEventListener("click", logout);
  if (els.folderForm) {
    els.folderForm.addEventListener("submit", createFolder);
  }
  els.uploadForm.addEventListener("submit", uploadFiles);
  els.openUploadSourceButton.addEventListener("click", () => {
    showDialog(els.uploadSourceDialog);
    renderUploadSourcePanels();
  });
  els.uploadSourceLocation.addEventListener("change", renderUploadSourcePanels);
  els.uploadLocalKind.addEventListener("change", syncLocalPickerKind);
  els.pickLocalSourceButton.addEventListener("click", openLocalSourcePicker);
  els.uploadFiles.addEventListener("change", handleLocalFileSelection);
  els.uploadFolders.addEventListener("change", handleLocalFolderSelection);
  els.uploadServerSourceList.addEventListener("click", onUploadServerSourceListClick);
  els.createFolderButton.addEventListener("click", createFolderFromBrowser);
  els.openUploadTargetButton.addEventListener("click", openUploadTargetDialog);
  els.uploadTargetUseCurrentButton.addEventListener("click", selectCurrentUploadTargetDir);
  els.uploadTargetConfirmButton.addEventListener("click", confirmUploadTargetSelection);
  els.uploadTargetList.addEventListener("click", onUploadTargetListClick);
  els.openSelectedButton.addEventListener("click", openSelected);
  els.downloadSelectedButton.addEventListener("click", downloadSelected);
  els.downloadServerButton.addEventListener("click", downloadToServerSelected);
  els.downloadTargetUseCurrentButton.addEventListener("click", selectCurrentDownloadTargetDir);
  els.downloadTargetConfirmButton.addEventListener("click", confirmDownloadTargetSelection);
  els.downloadTargetList.addEventListener("click", onDownloadTargetListClick);
  els.renameSelectedButton.addEventListener("click", renameSelected);
  els.moveSelectedButton.addEventListener("click", moveSelected);
  els.reloadButton.addEventListener("click", () => loadFiles(state.currentDir));
  els.deleteSelectedButton.addEventListener("click", deleteSelected);
  els.selectAll.addEventListener("change", toggleSelectAll);
  els.fileTableBody.addEventListener("click", onTableAction);
  els.renameForm.addEventListener("submit", submitRename);
  els.moveForm.addEventListener("submit", submitMove);

  document.querySelectorAll("[data-close-dialog]").forEach((button) => {
    button.addEventListener("click", () => closeDialog(button.dataset.closeDialog));
  });
}

function interceptAuth(event) {
  if (state.bootstrap.ready_for_auth) {
    return;
  }
  event.preventDefault();
  toast("请先保存 AppKey、SecretKey 和回调地址。", true);
}

async function refreshStatus({ silent = false } = {}) {
  const data = await api("/api/status");
  state.bootstrap = data;
  state.appChoices = data.app_choices || [];
  fillSettingsForm(data.config || {});
  renderAppChoices(state.appChoices, data.config || {});
  renderConnection(data);
  renderDocs(data.official_docs || []);

  if (data.authorized && data.ready_for_api) {
    await Promise.all([loadProfile(), loadQuota(), loadFiles(state.currentDir || data.app_root || "/")]);
  } else {
    resetProfile();
    resetQuota();
    renderEmptyBrowser(data.ready_for_api ? "已保存配置，继续点击“前往百度授权”即可开始。" : "请先填写 AppKey、SecretKey 和应用目录。");
  }

  if (!silent) {
    toast("状态已刷新。");
  }
}

function fillSettingsForm(config) {
  els.appKey.value = config.app_key || "";
  els.secretKey.value = config.secret_key || "";
  els.appId.value = config.app_id || "";
  els.appName.value = config.app_name || "";
  els.appRoot.value = config.app_root || "";
  els.redirectUri.value = config.redirect_uri || "";
  els.membershipTier.value = config.membership_tier || "free";
  els.uploadChunkMb.value = config.upload_chunk_mb > 0 ? config.upload_chunk_mb : "";
  els.cliDownloadWorkers.value = config.cli_download_workers > 0 ? config.cli_download_workers : "";
  els.webDownloadWorkers.value = config.web_download_workers > 0 ? config.web_download_workers : "";
  els.singleFileParallelEnabled.value = config.single_file_parallel_enabled === false ? "false" : "true";
  els.singleFileDownloadWorkers.value = config.single_file_download_workers > 0 ? config.single_file_download_workers : "";
  updateMembershipPlaceholders();
  updateCustomSettingsHint();
  ensureUploadDefaults();
  renderUploadSourcePanels();
  renderUploadTargetDisplay();
}

function renderAppChoices(choices, config) {
  els.appPreset.innerHTML = '<option value="">Manual</option>';
  const current = {
    app_key: (config.app_key || "").trim(),
    secret_key: (config.secret_key || "").trim(),
    app_name: (config.app_name || "").trim(),
    app_root: (config.app_root || "").trim(),
  };
  let selectedId = "";

  choices.forEach((choice) => {
    const option = document.createElement("option");
    option.value = choice.id;
    const pathLabel = choice.app_root || "(path not set)";
    option.textContent = `${choice.label} (${pathLabel})${choice.source === "config" ? " [config.json]" : " [bypy]"}`;
    if (
      choice.app_key === current.app_key &&
      choice.secret_key === current.secret_key &&
      choice.app_name === current.app_name &&
      choice.app_root === current.app_root
    ) {
      selectedId = choice.id;
    }
    els.appPreset.appendChild(option);
  });

  els.appPreset.value = selectedId;
}

function applyAppPreset() {
  const choice = state.appChoices.find((item) => item.id === els.appPreset.value);
  if (!choice) {
    return;
  }
  els.appKey.value = choice.app_key || "";
  els.secretKey.value = choice.secret_key || "";
  els.appName.value = choice.app_name || "";
  els.appRoot.value = choice.app_root || "";
}

function updateCustomSettingsHint() {
  const singleFileEnabled = els.singleFileParallelEnabled.value !== "false";
  const singleFileHint = singleFileEnabled
    ? "单文件下载默认会自动尝试分段并发，不支持 Range 时会自动回退。"
    : "单文件下载会保持单连接串行模式。";
  const tier = (els.membershipTier.value || "free").toLowerCase();
  if (tier === "svip") {
    els.customSettingsHint.textContent = `上传分片会按当前账号身份自动使用最高可用档位；下载并发不受会员档限制。${singleFileHint}`;
    return;
  }
  if (tier === "vip") {
    els.customSettingsHint.textContent = `上传分片会按当前账号身份自动使用最高可用档位；下载并发不受会员档限制。${singleFileHint}`;
    return;
  }
  els.customSettingsHint.textContent = `默认显示无会员档。授权后会根据当前账号身份自动切到最高上传档位；下载并发不受会员档限制。${singleFileHint}`;
}

function membershipRanges(tier) {
  const normalized = (tier || "free").toLowerCase();
  if (normalized === "free") {
    return { upload: "1-4，留空=4", workers: "1-8，留空=8", singleFileWorkers: "1-8，留空=4" };
  }
  if (normalized === "vip") {
    return { upload: "1-16，留空=16", workers: "1-8，留空=8", singleFileWorkers: "1-8，留空=4" };
  }
  return { upload: "1-32，留空=32", workers: "1-8，留空=8", singleFileWorkers: "1-8，留空=4" };
}

function updateMembershipPlaceholders() {
  const ranges = membershipRanges(els.membershipTier.value);
  els.uploadChunkMb.placeholder = ranges.upload;
  els.cliDownloadWorkers.placeholder = ranges.workers;
  els.webDownloadWorkers.placeholder = ranges.workers;
  els.singleFileDownloadWorkers.placeholder = ranges.singleFileWorkers;
}

function renderConnection(data) {
  const appRoot = data.app_root || "未配置";
  const redirectUri = (data.config?.redirect_uri || "").trim();
  const useOob = redirectUri.toLowerCase() === "oob";
  els.readyForApi.textContent = data.ready_for_api ? "是" : "否";
  els.authorizedStatus.textContent = data.authorized ? "是" : "否";
  els.appRootLabel.textContent = appRoot;
  els.heroAppRoot.textContent = appRoot;
  els.authButton.classList.toggle("disabled", !data.ready_for_auth);
  els.authButton.target = useOob ? "_blank" : "";
  els.authButton.rel = useOob ? "noreferrer" : "";
  els.manualAuthForm.hidden = !useOob;
  els.noticeBar.innerHTML = data.authorized
    ? `当前浏览范围：<code>${escapeHtml(appRoot)}</code>`
    : useOob
      ? `当前使用 <code>redirect_uri=oob</code>。点击“前往百度授权”后，会跳到百度提供的默认成功页；把页面上的 <code>code</code> 复制回来，再粘贴到下方表单里完成换 token。`
      : `保存配置后点“前往百度授权”，完成一次授权后即可浏览 <code>${escapeHtml(appRoot)}</code>。`;
}

async function saveSettings(event) {
  event.preventDefault();
  const payload = {
    app_key: els.appKey.value.trim(),
    secret_key: els.secretKey.value.trim(),
    app_id: els.appId.value.trim(),
    app_name: els.appName.value.trim(),
    app_root: els.appRoot.value.trim(),
    redirect_uri: els.redirectUri.value.trim(),
    membership_tier: els.membershipTier.value,
    upload_chunk_mb: Number(els.uploadChunkMb.value || 0),
    cli_download_workers: Number(els.cliDownloadWorkers.value || 0),
    web_download_workers: Number(els.webDownloadWorkers.value || 0),
    single_file_parallel_enabled: els.singleFileParallelEnabled.value !== "false",
    single_file_download_workers: Number(els.singleFileDownloadWorkers.value || 0),
  };
  await api("/api/settings", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  await refreshStatus({ silent: true });
  toast("本地配置已保存。");
}

async function refreshToken() {
  await api("/api/refresh-token", { method: "POST" });
  await refreshStatus({ silent: true });
  toast("Token 已刷新。");
}

async function submitManualCode(event) {
  event.preventDefault();
  const code = els.manualAuthCode.value.trim();
  if (!code) {
    toast("请先粘贴授权 code。", true);
    return;
  }
  await api("/api/exchange-code", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code }),
  });
  els.manualAuthCode.value = "";
  await refreshStatus({ silent: true });
  toast("手动授权已完成。");
}

async function logout() {
  await api("/api/logout", { method: "POST" });
  await refreshStatus({ silent: true });
  toast("本地授权已清除。");
}

async function loadProfile() {
  const data = await api("/api/profile");
  els.profileName.textContent = data.netdisk_name || data.baidu_name || "未命名用户";
  els.profileVip.textContent = vipLabel(data.vip_type);
  els.profileUk.textContent = `UK: ${data.uk || "-"}`;
  els.membershipTier.value = Number(data.vip_type) === 2 ? "svip" : Number(data.vip_type) === 1 ? "vip" : "free";
  updateMembershipPlaceholders();
  updateCustomSettingsHint();
  els.avatarImage.src = data.avatar_url || "";
}

async function loadQuota() {
  const data = await api("/api/quota");
  const used = Number(data.used || 0);
  const total = Number(data.total || 0);
  const ratio = total > 0 ? Math.min(100, Math.round((used / total) * 100)) : 0;
  els.quotaBar.style.width = `${ratio}%`;
  els.quotaUsed.textContent = `已用 ${formatBytes(used)}`;
  els.quotaTotal.textContent = `总量 ${formatBytes(total)}`;
}

async function loadFiles(dir) {
  const data = await api(`/api/files?dir=${encodeURIComponent(dir || "/")}`);
  state.currentDir = data.cwd.path;
  state.entries = data.entries || [];
  renderBreadcrumbs(data.breadcrumbs || []);
  renderTable(state.entries);
}

function renderBreadcrumbs(crumbs) {
  els.breadcrumbs.innerHTML = "";
  crumbs.forEach((crumb) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "crumb";
    button.textContent = crumb.name;
    button.addEventListener("click", () => loadFiles(crumb.path).catch((error) => toast(error.message, true)));
    els.breadcrumbs.appendChild(button);
  });
}

function renderTable(entries) {
  els.fileTableBody.innerHTML = "";

  if (!entries.length) {
    renderEmptyBrowser("列表为空");
    return;
  }

  entries.forEach((entry) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td class="fit"><input type="checkbox" class="row-check" data-path="${escapeHtml(entry.path)}" /></td>
      <td>
        <div class="file-name">
          <button type="button" class="name-button${entry.is_dir ? " dir-link" : ""}" data-action="inspect" data-path="${escapeHtml(entry.path)}">${escapeHtml(entry.name)}</button>
          <span class="path-note">${escapeHtml(entry.display_path)}</span>
        </div>
      </td>
      <td class="fit"><span class="file-type">${entry.is_dir ? "目录" : categoryLabel(entry.category)}</span></td>
      <td class="fit">${entry.is_dir ? "-" : formatBytes(entry.size)}</td>
      <td class="fit">${formatTime(entry.server_mtime)}</td>
    `;
    els.fileTableBody.appendChild(row);
  });
}

function renderEmptyBrowser(message) {
  els.fileTableBody.innerHTML = `<tr><td colspan="5" class="empty-state">${escapeHtml(message)}</td></tr>`;
}

function renderDocs(docs) {
  els.docList.innerHTML = "";
  docs.forEach((doc) => {
    const anchor = document.createElement("a");
    anchor.className = "doc-link";
    anchor.href = doc.url;
    anchor.target = "_blank";
    anchor.rel = "noreferrer";
    anchor.innerHTML = `<strong>${escapeHtml(doc.name)}</strong><span>官方更新时间 ${escapeHtml(doc.updated)}</span>`;
    els.docList.appendChild(anchor);
  });
}

async function createFolder(event) {
  event.preventDefault();
  const name = els.folderName.value.trim();
  if (!name) {
    toast("请输入文件夹名称。", true);
    return;
  }
  const path = joinRemote(state.currentDir || state.bootstrap.app_root || "/", name);
  await api("/api/folders", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path }),
  });
  els.folderName.value = "";
  await loadFiles(state.currentDir);
  toast("文件夹已创建。");
}

async function uploadFiles(event) {
  event.preventDefault();
  const source = state.uploadSourceSelection;
  if (!source?.location || !source?.path) {
    toast("请先选择文件路径。", true);
    return;
  }
  const remoteTarget = selectedUploadDirectoryPath();

  if (source.location === "server") {
    showUploadProgress(5, "正在准备服务器文件…");
    await api("/api/upload-server-source", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source_path: source.path,
        target_dir: remoteTarget,
        policy: els.uploadPolicy.value,
      }),
    });
    showUploadProgress(100, "正在提交到百度网盘…");
    hideUploadProgress();
    await loadFiles(state.currentDir);
    toast("上传完成。");
    return;
  }

  const useFolderSource = source.is_dir;
  const sourceFiles = useFolderSource ? Array.from(els.uploadFolders.files || []) : Array.from(els.uploadFiles.files || []);
  const relativePaths = sourceFiles.map((file) => (useFolderSource ? file.webkitRelativePath || file.name : ""));
  if (!sourceFiles.length) {
    toast(`请先选择${useFolderSource ? "本地文件夹" : "本地文件"}。`, true);
    return;
  }

  const formData = new FormData();
  formData.append("target_dir", remoteTarget);
  formData.append("policy", els.uploadPolicy.value);
  sourceFiles.forEach((file, index) => {
    formData.append("files", file);
    formData.append("relative_paths", relativePaths[index] || "");
  });

  showUploadProgress(0, "正在发送到本地服务…");
  await xhr("/api/upload", formData, (percent) => {
    const rounded = Math.max(1, Math.round(percent));
    showUploadProgress(rounded, rounded >= 100 ? "本地服务已收到文件，正在提交到百度网盘…" : `上传中 ${rounded}%`);
  });
  els.uploadFiles.value = "";
  els.uploadFolders.value = "";
  hideUploadProgress();
  await loadFiles(state.currentDir);
  toast("上传完成。");
}

function ensureUploadDefaults() {
  if (!state.uploadSourceSelection) {
    state.uploadSourceSelection = {
      location: "local",
      path: "",
      is_dir: false,
      label: "未选择",
    };
  }
  if (!els.uploadSourceLocation.value) {
    els.uploadSourceLocation.value = state.uploadSourceSelection.location || "local";
  }
  if (!els.uploadLocalKind.value) {
    els.uploadLocalKind.value = state.uploadSourceSelection.is_dir ? "folder" : "file";
  }
}

function renderUploadSourcePanels() {
  const mode = els.uploadSourceLocation.value || "local";
  if (!state.uploadSourceSelection) {
    ensureUploadDefaults();
  }
  if (state.uploadSourceSelection.location !== mode) {
    state.uploadSourceSelection = {
      location: mode,
      path: "",
      is_dir: false,
      label: "未选择",
    };
  }
  els.uploadSourceLocalPanel.hidden = mode !== "local";
  els.uploadSourceServerPanel.hidden = mode !== "server";
  if (els.uploadSourceLocalActions) {
    els.uploadSourceLocalActions.hidden = mode !== "local";
  }
  syncLocalPickerKind();
  if (els.uploadSourcePanelTitle) {
    els.uploadSourcePanelTitle.textContent = mode === "local" ? "本机路径" : "服务器路径";
  }
  renderUploadSourceSummary();
  renderLocalSourceSelectionList();
  if (mode === "server") {
    const baseDir = state.uploadSourceSelection.location === "server" && state.uploadSourceSelection.path
      ? state.uploadSourceSelection.is_dir
        ? state.uploadSourceSelection.path
        : parentRemoteDir(state.uploadSourceSelection.path)
      : ".";
    loadUploadServerSourceEntries(baseDir).catch((error) => toast(error.message, true));
  }
}

function syncLocalPickerKind() {
  if (!els.uploadLocalKind || !els.pickLocalSourceButton) {
    return;
  }
  const kind = els.uploadLocalKind.value || "file";
  els.pickLocalSourceButton.textContent = kind === "folder" ? "选择文件夹" : "选择文件";
}

function openLocalSourcePicker() {
  const kind = els.uploadLocalKind?.value || "file";
  if (kind === "folder") {
    els.uploadFolders.value = "";
    els.uploadFolders.click();
    return;
  }
  els.uploadFiles.value = "";
  els.uploadFiles.click();
}

function handleLocalFileSelection() {
  const files = Array.from(els.uploadFiles.files || []);
  if (!files.length) {
    return;
  }
  els.uploadFolders.value = "";
  state.uploadSourceSelection = {
    location: "local",
    path: files.length === 1 ? files[0].name : `${files.length} 个文件`,
    is_dir: false,
    label: files.length === 1 ? files[0].name : `${files.length} 个文件`,
  };
  if (els.uploadLocalKind) {
    els.uploadLocalKind.value = "file";
    syncLocalPickerKind();
  }
  els.uploadSourceLocation.value = "local";
  renderUploadSourcePanels();
}

function handleLocalFolderSelection() {
  const files = Array.from(els.uploadFolders.files || []);
  if (!files.length) {
    return;
  }
  els.uploadFiles.value = "";
  const firstPath = files[0].webkitRelativePath || files[0].name;
  const folderName = firstPath.split("/")[0] || files[0].name;
  state.uploadSourceSelection = {
    location: "local",
    path: folderName,
    is_dir: true,
    label: folderName,
  };
  if (els.uploadLocalKind) {
    els.uploadLocalKind.value = "folder";
    syncLocalPickerKind();
  }
  els.uploadSourceLocation.value = "local";
  renderUploadSourcePanels();
}

function renderUploadSourceSummary() {
  const source = state.uploadSourceSelection;
  const summary = source?.path ? `${source.is_dir ? "文件夹" : "文件"} · ${source.path}` : "未选择";
  if (els.uploadSourceSummary) {
    els.uploadSourceSummary.textContent = summary;
  }
  if (els.uploadSourceSelectionText) {
    els.uploadSourceSelectionText.textContent = summary;
  }
}

function renderLocalSourceSelectionList() {
  const source = state.uploadSourceSelection;
  if (!els.uploadLocalSelectionList) {
    return;
  }
  if (!source?.path || source.location !== "local") {
    els.uploadLocalSelectionList.innerHTML = '<tr><td colspan="3" class="empty-state">未选择</td></tr>';
    return;
  }
  els.uploadLocalSelectionList.innerHTML = `
    <tr class="picker-row-selected">
      <td>${escapeHtml(source.path)}</td>
      <td>${source.is_dir ? "文件夹" : "文件"}</td>
      <td class="fit">已选择</td>
    </tr>
  `;
}

function renderUploadTargetDisplay() {
  const targetPath = (els.uploadTargetPath.value || "").trim();
  if (els.uploadTargetSummary) {
    els.uploadTargetSummary.textContent = targetPath || "当前目录";
  }
  if (state.uploadTargetSelection?.path && ensureTrailingSlash(state.uploadTargetSelection.path) === targetPath) {
    els.uploadTargetSelectionText.textContent = describeUploadTarget(state.uploadTargetSelection);
    return;
  }
  els.uploadTargetSelectionText.textContent = targetPath || "默认当前目录";
}

async function loadUploadServerSourceEntries(dir) {
  const data = await apiWithServerPathsFallback(`/api/server-paths?dir=${encodeURIComponent(dir || ".")}&include_files=1`);
  state.uploadServerSourcePickerDir = data.cwd.path;
  renderUploadServerSourceBreadcrumbs(data.breadcrumbs || []);
  renderUploadServerSourceList(data.entries || []);
}

function renderUploadServerSourceBreadcrumbs(crumbs) {
  els.uploadServerSourceBreadcrumbs.innerHTML = "";
  crumbs.forEach((crumb) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "crumb";
    button.textContent = crumb.name;
    button.addEventListener("click", () => {
      loadUploadServerSourceEntries(crumb.path).catch((error) => toast(error.message, true));
    });
    els.uploadServerSourceBreadcrumbs.appendChild(button);
  });
}

function renderUploadServerSourceList(entries) {
  if (!entries.length) {
    els.uploadServerSourceList.innerHTML = '<tr><td colspan="3" class="empty-state">当前目录为空</td></tr>';
    return;
  }
  els.uploadServerSourceList.innerHTML = entries.map((entry) => {
    const selected = state.uploadServerSourceSelection?.path === entry.path;
    return `
      <tr class="${selected ? "picker-row-selected" : ""}${entry.is_dir ? " picker-row-openable" : ""}"${entry.is_dir ? ` data-upload-server-open="${escapeHtml(entry.path)}"` : ""}>
        <td>${escapeHtml(entry.name)}</td>
        <td>${entry.is_dir ? "目录" : "文件"}</td>
        <td class="fit">
          <div class="picker-actions">
            <button class="button ${selected ? "primary" : "accent"}" type="button" data-upload-server-select="${escapeHtml(entry.path)}" data-upload-server-kind="${entry.is_dir ? "dir" : "file"}" data-upload-server-name="${escapeHtml(entry.name)}">${selected ? "已选" : "选择"}</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");
}

function onUploadServerSourceListClick(event) {
  const target = eventTargetElement(event);
  if (!target) {
    return;
  }
  const selectButton = target.closest("[data-upload-server-select]");
  if (selectButton) {
    state.uploadServerSourceSelection = {
      location: "server",
      path: selectButton.dataset.uploadServerSelect,
      is_dir: selectButton.dataset.uploadServerKind === "dir",
      label: selectButton.dataset.uploadServerSelect,
      name: selectButton.dataset.uploadServerName || "",
    };
    state.uploadSourceSelection = { ...state.uploadServerSourceSelection };
    renderUploadSourceSummary();
    renderUploadServerSourceSelectionState();
    return;
  }

  const openRow = target.closest("[data-upload-server-open]");
  if (!openRow) {
    return;
  }
  loadUploadServerSourceEntries(openRow.dataset.uploadServerOpen).catch((error) => toast(error.message, true));
}

function renderUploadServerSourceSelectionState() {
  const current = state.uploadServerSourceSelection?.path
    || (state.uploadSourceSelection?.location === "server" ? state.uploadSourceSelection.path : "")
    || "";
  if (els.uploadSourceSelectionText && els.uploadSourceLocation.value === "server") {
    els.uploadSourceSelectionText.textContent = current || "未选择";
  }
  els.uploadServerSourceList.querySelectorAll("tr").forEach((item) => {
    const button = item.querySelector("[data-upload-server-select]");
    const isSelected = button?.dataset.uploadServerSelect === current;
    item.classList.toggle("picker-row-selected", Boolean(isSelected));
    if (button) {
      button.classList.toggle("primary", Boolean(isSelected));
      button.classList.toggle("accent", !isSelected);
      button.textContent = isSelected ? "已选" : "选择";
    }
  });
}

async function createFolderFromBrowser() {
  const name = window.prompt("请输入目录名");
  if (!name || !name.trim()) {
    return;
  }
  const path = joinRemote(state.currentDir || state.bootstrap.app_root || "/", name.trim());
  await api("/api/folders", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path }),
  });
  await loadFiles(state.currentDir);
  toast("目录已创建。");
}

function selectedUploadDirectoryPath() {
  const targetPath = (els.uploadTargetPath.value || "").trim();
  if (!targetPath) {
    return state.currentDir || state.bootstrap.app_root || "/";
  }
  return targetPath.endsWith("/") ? targetPath : `${targetPath}/`;
}

async function openUploadTargetDialog() {
  const targetPath = (els.uploadTargetPath.value || "").trim();
  const baseDir = targetPath || state.currentDir || state.bootstrap.app_root || "/";
  state.uploadTargetSelection = targetPath
    ? {
        path: targetPath,
        is_dir: true,
        name: targetPath.split("/").filter(Boolean).pop() || targetPath,
      }
    : {
        path: baseDir,
        is_dir: true,
        name: baseDir.split("/").filter(Boolean).pop() || "/",
      };
  renderUploadTargetDisplay();
  showDialog(els.uploadTargetDialog);
  await loadUploadTargetEntries(baseDir);
}

async function loadUploadTargetEntries(dir) {
  const data = await api(`/api/files?dir=${encodeURIComponent(dir || "/")}`);
  state.uploadTargetPickerDir = data.cwd.path;
  renderUploadTargetBreadcrumbs(data.breadcrumbs || []);
  renderUploadTargetList(data.entries || []);
}

function renderUploadTargetBreadcrumbs(crumbs) {
  els.uploadTargetBreadcrumbs.innerHTML = "";
  crumbs.forEach((crumb) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "crumb";
    button.textContent = crumb.name;
    button.addEventListener("click", () => {
      loadUploadTargetEntries(crumb.path).catch((error) => toast(error.message, true));
    });
    els.uploadTargetBreadcrumbs.appendChild(button);
  });
}

function renderUploadTargetList(entries) {
  const visibleEntries = entries.filter((entry) => entry.is_dir);

  if (!visibleEntries.length) {
    els.uploadTargetList.innerHTML = '<tr><td colspan="3" class="empty-state">当前目录下没有子目录</td></tr>';
    return;
  }

  els.uploadTargetList.innerHTML = visibleEntries.map((entry) => {
    const selected = state.uploadTargetSelection?.path === entry.path;
    return `
      <tr class="${selected ? "picker-row-selected" : ""} picker-row-openable" data-picker-open="${escapeHtml(entry.path)}">
        <td>${escapeHtml(entry.name)}</td>
        <td>目录</td>
        <td class="fit">
          <div class="picker-actions">
            <button class="button ${selected ? "primary" : "accent"}" type="button" data-picker-select="${escapeHtml(entry.path)}" data-picker-name="${escapeHtml(entry.name)}">${selected ? "已选" : "选择"}</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");
}

function onUploadTargetListClick(event) {
  const target = eventTargetElement(event);
  if (!target) {
    return;
  }
  const selectButton = target.closest("[data-picker-select]");
  if (selectButton) {
    state.uploadTargetSelection = {
      path: selectButton.dataset.pickerSelect,
      is_dir: true,
      name: selectButton.dataset.pickerName || "",
    };
    renderUploadTargetSelectionState();
    return;
  }

  const openRow = target.closest("[data-picker-open]");
  if (!openRow) {
    return;
  }
  loadUploadTargetEntries(openRow.dataset.pickerOpen).catch((error) => toast(error.message, true));
}

function selectCurrentUploadTargetDir() {
  const path = state.uploadTargetPickerDir || state.currentDir || state.bootstrap.app_root || "/";
  state.uploadTargetSelection = {
    path,
    is_dir: true,
    name: path.split("/").filter(Boolean).pop() || "/",
  };
  renderUploadTargetSelectionState();
}

function renderUploadTargetSelectionState() {
  els.uploadTargetSelectionText.textContent = describeUploadTarget(state.uploadTargetSelection);
  renderUploadTargetListFromDomSelection();
}

function renderUploadTargetListFromDomSelection() {
  const selectedPath = state.uploadTargetSelection?.path || "";
  els.uploadTargetList.querySelectorAll("tr").forEach((item) => {
    const button = item.querySelector("[data-picker-select]");
    const isSelected = button?.dataset.pickerSelect === selectedPath;
    item.classList.toggle("picker-row-selected", Boolean(isSelected));
    if (button) {
      button.classList.toggle("primary", Boolean(isSelected));
      button.classList.toggle("accent", !isSelected);
      button.textContent = isSelected ? "已选" : "选择";
    }
  });
}

function confirmUploadTargetSelection() {
  if (!state.uploadTargetSelection?.path) {
    els.uploadTargetPath.value = "";
    renderUploadTargetDisplay();
    closeDialog("upload-target-dialog");
    return;
  }
  els.uploadTargetPath.value = ensureTrailingSlash(state.uploadTargetSelection.path);
  renderUploadTargetDisplay();
  closeDialog("upload-target-dialog");
}

function describeUploadTarget(selection) {
  if (!selection?.path) {
    return "默认当前目录";
  }
  return selection.is_dir ? `目录 ${ensureTrailingSlash(selection.path)}` : `文件 ${selection.path}`;
}

function showUploadProgress(percent, text) {
  showServerDownloadCard();
  if (els.uploadMonitorDetails) {
    els.uploadMonitorDetails.hidden = false;
  }
  els.uploadProgressWrap.hidden = false;
  els.uploadProgressBar.style.width = `${percent}%`;
  els.uploadProgressText.textContent = text;
  if (els.uploadSummaryText) {
    els.uploadSummaryText.textContent = text;
  }
  if (els.uploadSummaryPercent) {
    els.uploadSummaryPercent.textContent = `${percent}%`;
  }
  if (els.uploadProgressEmpty) {
    els.uploadProgressEmpty.hidden = true;
  }
  updateTransferOverview();
}

function hideUploadProgress() {
  if (els.uploadMonitorDetails) {
    els.uploadMonitorDetails.hidden = true;
  }
  els.uploadProgressWrap.hidden = true;
  els.uploadProgressBar.style.width = "0%";
  els.uploadProgressText.textContent = "暂无上传";
  if (els.uploadSummaryText) {
    els.uploadSummaryText.textContent = "待机";
  }
  if (els.uploadSummaryPercent) {
    els.uploadSummaryPercent.textContent = "0%";
  }
  if (els.uploadProgressEmpty) {
    els.uploadProgressEmpty.hidden = false;
  }
  updateTransferOverview();
}

function showServerDownloadCard() {
  els.downloadSpeedCard.hidden = false;
}

function hideServerDownloadProgress() {
  if (els.downloadMonitorDetails) {
    els.downloadMonitorDetails.hidden = true;
  }
  els.downloadSpeedTotal.textContent = "0 B/s";
  els.downloadProgressText.textContent = "0%";
  els.serverDownloadProgressBar.style.width = "0%";
    els.serverDownloadProgressText.textContent = "暂无下载";
  els.downloadActiveList.innerHTML = "";
  els.downloadWaitingList.innerHTML = "";
  els.downloadCompletedList.innerHTML = "";
  updateTransferOverview();
}

function showRecoveringDownloadProgress(jobs) {
  showServerDownloadCard();
  if (els.downloadMonitorDetails) {
    els.downloadMonitorDetails.hidden = false;
  }
  const count = Array.isArray(jobs) ? jobs.length : 0;
  els.downloadSpeedTotal.textContent = "恢复中";
  els.downloadProgressText.textContent = "--";
  els.serverDownloadProgressBar.style.width = "0%";
  els.serverDownloadProgressText.textContent = `正在恢复 ${count} 个下载任务…`;
  els.downloadActiveList.innerHTML = '<div class="download-file-empty">正在重新连接后端任务</div>';
  els.downloadWaitingList.innerHTML = '<div class="download-file-empty">请稍候…</div>';
  updateTransferOverview();
}

function renderServerDownloadProgress(job, fallbackTotalBytes = 0) {
  const safeTotal = Math.max(fallbackTotalBytes || 0, job.total_bytes || 0, 1);
  const transferred = Math.max(0, job.transferred_bytes || 0);
  const percent = Math.min(100, Math.round((transferred / safeTotal) * 100));
  const activeFiles = Array.isArray(job.active_files) ? job.active_files : [];
  const waitingFiles = Array.isArray(job.waiting_files) ? job.waiting_files : [];
  const completedFiles = Array.isArray(job.completed_files) ? job.completed_files : [];
  const verifyingCount = activeFiles.filter((file) => file.status === "verifying").length;
  const sizeText = job.total_bytes
    ? `${formatSize(transferred)} / ${formatSize(job.total_bytes)}`
    : `${job.total_files || 0} 个文件`;

  showServerDownloadCard();
  if (els.downloadMonitorDetails) {
    els.downloadMonitorDetails.hidden = false;
  }
  els.downloadSpeedTotal.textContent = formatSpeed(job.speed_bps || 0);
  els.downloadProgressText.textContent = `${percent}%`;
  els.serverDownloadProgressBar.style.width = `${percent}%`;
  els.serverDownloadProgressText.textContent = verifyingCount
    ? `${sizeText}，校验中 ${verifyingCount}，等待 ${waitingFiles.length}，完成 ${completedFiles.length}`
    : `${sizeText}，活跃 ${activeFiles.length}，等待 ${waitingFiles.length}，完成 ${completedFiles.length}`;
  els.downloadActiveList.innerHTML = renderDownloadFileList(activeFiles, "暂无下载");
  els.downloadWaitingList.innerHTML = renderDownloadWaitingList(waitingFiles);
  els.downloadCompletedList.innerHTML = renderCompletedDownloadList(completedFiles);
  updateTransferOverview();
}

function updateTransferOverview() {
  const uploadVisible = els.uploadMonitorDetails ? !els.uploadMonitorDetails.hidden : false;
  const downloadVisible = els.downloadMonitorDetails ? !els.downloadMonitorDetails.hidden : false;
  const uploadPercent = parseInt((els.uploadSummaryPercent?.textContent || "0").replace("%", ""), 10) || 0;
  const downloadPercent = parseInt((els.downloadProgressText?.textContent || "0").replace("%", ""), 10) || 0;

  const activePercents = [];
  const parts = [];
  if (uploadVisible) {
    activePercents.push(uploadPercent);
    parts.push(`上传 ${uploadPercent}%`);
  }
  if (downloadVisible) {
    activePercents.push(downloadPercent);
    parts.push(`下载 ${downloadPercent}%`);
  }

  const totalPercent = activePercents.length
    ? Math.round(activePercents.reduce((sum, value) => sum + value, 0) / activePercents.length)
    : 0;

  if (els.transferOverviewTitle) {
    els.transferOverviewTitle.textContent = activePercents.length ? "总进度" : "空闲";
  }
  if (els.transferOverviewPercent) {
    els.transferOverviewPercent.textContent = `${totalPercent}%`;
  }
  if (els.transferOverviewBar) {
    els.transferOverviewBar.style.width = `${totalPercent}%`;
  }
  if (els.transferOverviewDetail) {
    els.transferOverviewDetail.textContent = parts.length ? parts.join(" · ") : "暂无任务";
  }
}

function renderDownloadFileList(files, emptyText) {
  if (!files.length) {
    return `<div class="download-file-empty">${emptyText}</div>`;
  }
  return files.map((file) => `
    <article class="download-file-card">
      <strong title="${escapeHtml(file.path || file.label || file.name)}">${escapeHtml(file.name || file.label || "未命名文件")}</strong>
      <span>${downloadFileProgressText(file)}</span>
      <span>${downloadFileStatusText(file)}</span>
    </article>
  `).join("");
}

function renderDownloadWaitingList(files) {
  if (!files.length) {
    return '<div class="download-file-empty">队列为空</div>';
  }
  return files.map((file) => `
    <article class="download-file-card waiting">
      <strong title="${escapeHtml(file.path || file.label || file.name)}">${escapeHtml(file.name || file.label || "未命名文件")}</strong>
      <span>${formatSize(file.total_bytes || 0)}</span>
      <span>等待中</span>
    </article>
  `).join("");
}

function renderCompletedDownloadList(files) {
  if (!files.length) {
    return '<div class="download-file-empty">暂无完成文件</div>';
  }
  return files.map((file) => `
    <article class="download-file-card completed">
      <strong title="${escapeHtml(file.path || file.label || file.name)}">${escapeHtml(file.name || file.label || "未命名文件")}</strong>
      <span>${formatSize(file.total_bytes || file.transferred_bytes || 0)}</span>
      <span>已完成</span>
    </article>
  `).join("");
}

function downloadFileProgressText(file) {
  if (file.status === "verifying") {
    return `${formatSize(file.verify_bytes || 0)} / ${formatSize(file.verify_total_bytes || file.total_bytes || 0)}`;
  }
  return `${formatSize(file.transferred_bytes || 0)} / ${formatSize(file.total_bytes || 0)}`;
}

function downloadFileStatusText(file) {
  if (file.status === "verifying") {
    const total = Math.max(Number(file.verify_total_bytes || file.total_bytes || 0), 1);
    const done = Math.max(0, Number(file.verify_bytes || 0));
    const percent = Math.min(100, Math.round((done / total) * 100));
    return `校验中 ${percent}%`;
  }
  if (file.status === "completed") {
    return "已完成";
  }
  return formatSpeed(file.speed_bps || 0);
}

function persistActiveDownloadJobs() {
  if (!state.activeDownloadJobs.length) {
    window.sessionStorage.removeItem(ACTIVE_DOWNLOAD_JOB_KEY);
    return;
  }
  window.sessionStorage.setItem(ACTIVE_DOWNLOAD_JOB_KEY, JSON.stringify(state.activeDownloadJobs));
}

function upsertActiveDownloadJob(job) {
  state.activeDownloadJobs = [
    ...state.activeDownloadJobs.filter((item) => item.jobId !== job.jobId),
    job,
  ];
  persistActiveDownloadJobs();
  ensureDownloadPolling();
}

function removeActiveDownloadJob(jobId) {
  state.activeDownloadJobs = state.activeDownloadJobs.filter((item) => item.jobId !== jobId);
  persistActiveDownloadJobs();
  if (!state.activeDownloadJobs.length) {
    stopDownloadPolling();
    hideServerDownloadProgress();
  }
}

function stopDownloadPolling() {
  if (state.downloadPollTimer) {
    window.clearTimeout(state.downloadPollTimer);
    state.downloadPollTimer = null;
  }
}

function ensureDownloadPolling() {
  if (state.downloadPollTimer || !state.activeDownloadJobs.length) {
    return;
  }
  const tick = async () => {
    state.downloadPollTimer = null;
    await pollActiveDownloadJobs();
    if (state.activeDownloadJobs.length) {
      state.downloadPollTimer = window.setTimeout(tick, 150);
    }
  };
  state.downloadPollTimer = window.setTimeout(tick, 0);
}

function resumeServerDownloadJob() {
  const raw = window.sessionStorage.getItem(ACTIVE_DOWNLOAD_JOB_KEY);
  if (!raw) {
    return;
  }
  try {
    const payload = JSON.parse(raw);
    const jobs = Array.isArray(payload) ? payload : payload?.jobId ? [payload] : [];
    if (!jobs.length) {
      removeActiveDownloadJob("__invalid__");
      return;
    }
    state.activeDownloadJobs = jobs.map((job) => ({
      ...job,
      silent: true,
      pollFailures: Number(job.pollFailures || 0),
    }));
    persistActiveDownloadJobs();
    showRecoveringDownloadProgress(state.activeDownloadJobs);
    ensureDownloadPolling();
  } catch (error) {
    state.activeDownloadJobs = [];
    persistActiveDownloadJobs();
  }
}

function onTableAction(event) {
  const target = eventTargetElement(event);
  if (!target) {
    return;
  }
  const button = target.closest("[data-action]");
  if (!button) {
    return;
  }

  const action = button.dataset.action;
  const path = button.dataset.path;
  if (action === "inspect") {
    const entry = findEntry(path);
    if (entry?.is_dir) {
      loadFiles(path).catch((error) => toast(error.message, true));
    }
    return;
  }
  if (action === "open") {
    loadFiles(path).catch((error) => toast(error.message, true));
    return;
  }
  if (action === "download") {
    window.location.assign(`/api/download?fs_id=${encodeURIComponent(button.dataset.fsId)}`);
    return;
  }
  if (action === "rename") {
    openRenameDialog(path, button.dataset.name || "");
    return;
  }
  if (action === "move") {
    openMoveDialog(path);
    return;
  }
  if (action === "delete") {
    deletePaths([path]).catch((error) => toast(error.message, true));
  }
}

function toggleSelectAll() {
  document.querySelectorAll(".row-check").forEach((checkbox) => {
    checkbox.checked = els.selectAll.checked;
  });
}

function selectedPaths() {
  return Array.from(document.querySelectorAll(".row-check:checked")).map((node) => node.dataset.path);
}

function selectedEntries() {
  const paths = new Set(selectedPaths());
  return state.entries.filter((entry) => paths.has(entry.path));
}

function findEntry(path) {
  return state.entries.find((entry) => entry.path === path) || null;
}

function requireSingleSelection(entries, actionName) {
  if (!entries.length) {
    toast(`请先勾选要${actionName}的文件或目录`, true);
    return null;
  }
  if (entries.length !== 1) {
    toast(`${actionName}一次只能处理一个项目`, true);
    return null;
  }
  return entries[0];
}

async function openSelected() {
  const entry = requireSingleSelection(selectedEntries(), "打开");
  if (!entry) {
    return;
  }
  if (!entry.is_dir) {
    toast("只有目录可以直接打开", true);
    return;
  }
  await loadFiles(entry.path);
}

function downloadSelected() {
  const entries = selectedEntries();
  if (!entries.length) {
    toast("?????????????", true);
    return;
  }

  if (entries.length === 1 && !entries[0].is_dir) {
    window.location.assign(`/api/download?path=${encodeURIComponent(entries[0].path)}`);
    return;
  }

  downloadEntriesRecursive(entries).catch((error) => toast(error.message, true));
}

async function downloadEntriesRecursive(entries) {
  if (typeof window.showDirectoryPicker !== "function") {
    toast("?????????????????? Chromium ?????? CLI?", true);
    return;
  }

  const picker = await window.showDirectoryPicker({ mode: "readwrite" });
  const files = [];

  for (const entry of entries) {
    if (entry.is_dir) {
      await collectDirectoryFiles(entry.path, entry.name, files);
      continue;
    }
    files.push({
      path: entry.path,
      name: entry.name,
      relativePath: entry.name,
    });
  }

  if (!files.length) {
    toast("????????", true);
    return;
  }

  let completed = 0;
  await runWithConcurrency(files, 4, async (file) => {
    await saveRemoteFileToDirectory(file, picker);
    completed += 1;
    toast(`???? ${completed}/${files.length}: ${file.relativePath}`);
  });
  toast(`?????? ${files.length} ????`);
}

async function collectDirectoryFiles(remotePath, relativePrefix, output) {
  const data = await api(`/api/files?dir=${encodeURIComponent(remotePath)}`);
  for (const entry of data.entries || []) {
    const relativePath = relativePrefix ? `${relativePrefix}/${entry.name}` : entry.name;
    if (entry.is_dir) {
      await collectDirectoryFiles(entry.path, relativePath, output);
      continue;
    }
    output.push({
      path: entry.path,
      name: entry.name,
      relativePath,
    });
  }
}

async function saveRemoteFileToDirectory(file, rootHandle) {
  const parts = file.relativePath.split("/");
  const fileName = parts.pop();
  let current = rootHandle;
  for (const part of parts) {
    current = await current.getDirectoryHandle(part, { create: true });
  }

  const response = await fetch(`/api/download?path=${encodeURIComponent(file.path)}`);
  if (!response.ok) {
    let message = "????";
    try {
      const payload = await response.json();
      message = payload.error || message;
    } catch (error) {
      message = response.statusText || message;
    }
    throw new Error(message);
  }

  const fileHandle = await current.getFileHandle(fileName, { create: true });
  const writable = await fileHandle.createWritable();
  try {
    await writable.write(await response.blob());
  } finally {
    await writable.close();
  }
}

async function runWithConcurrency(items, limit, worker) {
  const queue = items.slice();
  const runners = Array.from({ length: Math.min(limit, queue.length) }, async () => {
    while (queue.length) {
      const item = queue.shift();
      if (!item) {
        return;
      }
      await worker(item);
    }
  });
  await Promise.all(runners);
}

async function downloadToServerSelected() {
  const entries = selectedEntries();
  if (!entries.length) {
    toast("\u8bf7\u5148\u52fe\u9009\u8981\u4e0b\u8f7d\u5230\u670d\u52a1\u5668\u7684\u6587\u4ef6\u6216\u76ee\u5f55", true);
    return;
  }

  state.pendingDownloadEntries = entries;
  state.downloadTargetSelection = {
    path: state.downloadTargetSelection?.path || "",
    is_dir: true,
    name: "",
  };
  const initialDir = state.downloadTargetSelection.path || "./downloads";
  showDialog(els.downloadTargetDialog);
  await loadDownloadTargetEntries(initialDir);
}

async function loadDownloadTargetEntries(dir) {
  const data = await apiWithServerPathsFallback(`/api/server-paths?dir=${encodeURIComponent(dir || "./downloads")}`);
  state.downloadTargetPickerDir = data.cwd.path;
  renderDownloadTargetBreadcrumbs(data.breadcrumbs || []);
  renderDownloadTargetList(data.entries || []);
  if (!state.downloadTargetSelection?.path) {
    state.downloadTargetSelection = {
      path: data.cwd.path,
      is_dir: true,
      name: data.cwd.name || "",
    };
  }
  renderDownloadTargetSelectionState();
}

function renderDownloadTargetBreadcrumbs(crumbs) {
  els.downloadTargetBreadcrumbs.innerHTML = "";
  crumbs.forEach((crumb) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "crumb";
    button.textContent = crumb.name;
    button.addEventListener("click", () => {
      loadDownloadTargetEntries(crumb.path).catch((error) => toast(error.message, true));
    });
    els.downloadTargetBreadcrumbs.appendChild(button);
  });
}

function renderDownloadTargetList(entries) {
  const directories = entries.filter((entry) => entry.is_dir);
  if (!directories.length) {
    els.downloadTargetList.innerHTML = '<tr><td colspan="3" class="empty-state">当前目录下没有子目录</td></tr>';
    return;
  }

  els.downloadTargetList.innerHTML = directories.map((entry) => {
    const selected = state.downloadTargetSelection?.path === entry.path;
    return `
      <tr class="${selected ? "picker-row-selected" : ""} picker-row-openable" data-download-open="${escapeHtml(entry.path)}">
        <td>${escapeHtml(entry.name)}</td>
        <td>目录</td>
        <td class="fit">
          <div class="picker-actions">
            <button class="button ${selected ? "primary" : "accent"}" type="button" data-download-select="${escapeHtml(entry.path)}">${selected ? "已选" : "选择"}</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");
}

function onDownloadTargetListClick(event) {
  const target = eventTargetElement(event);
  if (!target) {
    return;
  }
  const selectButton = target.closest("[data-download-select]");
  if (selectButton) {
    state.downloadTargetSelection = {
      path: selectButton.dataset.downloadSelect,
      is_dir: true,
      name: selectButton.dataset.downloadSelect.split("/").filter(Boolean).pop() || "/",
    };
    renderDownloadTargetSelectionState();
    return;
  }

  const openRow = target.closest("[data-download-open]");
  if (!openRow) {
    return;
  }
  loadDownloadTargetEntries(openRow.dataset.downloadOpen).catch((error) => toast(error.message, true));
}

function selectCurrentDownloadTargetDir() {
  const path = state.downloadTargetPickerDir || state.currentDir || state.bootstrap.app_root || "/";
  state.downloadTargetSelection = {
    path,
    is_dir: true,
    name: path.split("/").filter(Boolean).pop() || "/",
  };
  renderDownloadTargetSelectionState();
}

function renderDownloadTargetSelectionState() {
  els.downloadTargetSelectionText.textContent = state.downloadTargetSelection?.path || "未选择";
  els.downloadTargetList.querySelectorAll("tr").forEach((item) => {
    const button = item.querySelector("[data-download-select]");
    const isSelected = button?.dataset.downloadSelect === state.downloadTargetSelection?.path;
    item.classList.toggle("picker-row-selected", Boolean(isSelected));
    if (button) {
      button.classList.toggle("primary", Boolean(isSelected));
      button.classList.toggle("accent", !isSelected);
      button.textContent = isSelected ? "已选" : "选择";
    }
  });
}

async function confirmDownloadTargetSelection() {
  const entries = state.pendingDownloadEntries || [];
  const destination = state.downloadTargetSelection?.path;
  if (!entries.length || !destination) {
    state.pendingDownloadEntries = null;
    closeDialog("download-target-dialog");
    return;
  }
  closeDialog("download-target-dialog");

  const payload = {
    paths: entries.map((entry) => entry.path),
    destination,
  };
  const result = await api("/api/download-to-server", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  upsertActiveDownloadJob({
    jobId: result.job_id,
    destination: result.destination,
    totalBytes: result.total_bytes,
    count: result.count,
    silent: false,
  });
  showServerDownloadCard();
  state.pendingDownloadEntries = null;
  toast(`已加入下载队列: ${result.destination}`);
}

async function pollActiveDownloadJobs() {
  if (state.downloadPollInFlight || !state.activeDownloadJobs.length) {
    return;
  }
  state.downloadPollInFlight = true;
  try {
    const jobs = [...state.activeDownloadJobs];
    const results = await Promise.all(jobs.map(async (meta) => {
      try {
        const job = await api(`/api/download-to-server/${encodeURIComponent(meta.jobId)}`);
        return { meta, job };
      } catch (error) {
        return { meta, error };
      }
    }));

    const running = [];
    const nextJobs = [];
    results.forEach(({ meta, job, error }) => {
      if (error) {
        const failureCount = Number(meta.pollFailures || 0) + 1;
        if (/Download job not found/i.test(error.message || "") || failureCount >= 6) {
          toast(error.message || "下载任务查询失败", true);
          return;
        }
        nextJobs.push({ ...meta, pollFailures: failureCount, silent: true });
        return;
      }
      if (job.status === "completed") {
        if (!meta.silent) {
          toast(`已保存 ${job.count} 个选中项到服务器: ${meta.destination}`);
        }
        return;
      }
      if (job.status === "failed") {
        toast(job.error || "服务器端下载失败", true);
        return;
      }
      nextJobs.push({ ...meta, pollFailures: 0 });
      running.push({ meta, job });
    });

    state.activeDownloadJobs = nextJobs;
    persistActiveDownloadJobs();

    if (!state.activeDownloadJobs.length) {
      hideServerDownloadProgress();
      return;
    }

    if (!running.length) {
      showRecoveringDownloadProgress(state.activeDownloadJobs);
      return;
    }

    renderAggregatedDownloadProgress(running);
  } finally {
    state.downloadPollInFlight = false;
  }
}

function renderAggregatedDownloadProgress(runningJobs) {
  const aggregate = {
    total_bytes: 0,
    transferred_bytes: 0,
    speed_bps: 0,
    active_files: [],
    waiting_files: [],
    completed_files: [],
    total_files: 0,
  };

  runningJobs.forEach(({ meta, job }) => {
    aggregate.total_bytes += Math.max(job.total_bytes || 0, meta.totalBytes || 0);
    aggregate.transferred_bytes += Math.max(0, job.transferred_bytes || 0);
    aggregate.speed_bps += Number(job.speed_bps || 0);
    aggregate.total_files += Number(job.total_files || 0);
    aggregate.active_files.push(...(Array.isArray(job.active_files) ? job.active_files : []));
    aggregate.waiting_files.push(...(Array.isArray(job.waiting_files) ? job.waiting_files : []));
    aggregate.completed_files.push(...(Array.isArray(job.completed_files) ? job.completed_files : []));
  });

  renderServerDownloadProgress(aggregate, aggregate.total_bytes || 0);
  els.serverDownloadProgressText.textContent = `${runningJobs.length} 个任务 · ${els.serverDownloadProgressText.textContent}`;
}

function renameSelected() {
  const entry = requireSingleSelection(selectedEntries(), "重命名");
  if (!entry) {
    return;
  }
  openRenameDialog(entry.path, entry.name);
}

function moveSelected() {
  const entry = requireSingleSelection(selectedEntries(), "移动");
  if (!entry) {
    return;
  }
  openMoveDialog(entry.path);
}

async function deleteSelected() {
  const paths = selectedPaths();
  if (!paths.length) {
    toast("请先选中要删除的文件。", true);
    return;
  }
  await deletePaths(paths);
}

async function deletePaths(paths) {
  if (!window.confirm(`确认删除 ${paths.length} 项吗？`)) {
    return;
  }
  await api("/api/delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ paths }),
  });
  await loadFiles(state.currentDir);
  toast("删除完成。");
}

function openRenameDialog(path, name) {
  els.renamePath.value = path;
  els.renameName.value = name;
  showDialog(els.renameDialog);
  els.renameName.focus();
}

function openMoveDialog(path) {
  els.movePath.value = path;
  els.moveDestination.value = state.currentDir || state.bootstrap.app_root || "/";
  els.moveNewName.value = "";
  showDialog(els.moveDialog);
  els.moveDestination.focus();
}

async function submitRename(event) {
  event.preventDefault();
  await api("/api/rename", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      path: els.renamePath.value,
      new_name: els.renameName.value.trim(),
    }),
  });
  closeDialog("rename-dialog");
  await loadFiles(state.currentDir);
  toast("重命名完成。");
}

async function submitMove(event) {
  event.preventDefault();
  await api("/api/move", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      path: els.movePath.value,
      destination_dir: els.moveDestination.value.trim(),
      new_name: els.moveNewName.value.trim() || null,
    }),
  });
  closeDialog("move-dialog");
  await loadFiles(state.currentDir);
  toast("移动完成。");
}

function showDialog(dialog) {
  if (typeof dialog.showModal === "function") {
    dialog.showModal();
  } else {
    dialog.setAttribute("open", "open");
  }
}

function closeDialog(id) {
  const dialog = document.getElementById(id);
  if (!dialog) {
    return;
  }
  if (typeof dialog.close === "function") {
    dialog.close();
  } else {
    dialog.removeAttribute("open");
  }
}

function resetProfile() {
  els.profileName.textContent = "等待授权";
  els.profileVip.textContent = "未连接";
  els.profileUk.textContent = "UK: -";
  els.avatarImage.removeAttribute("src");
}

function resetQuota() {
  els.quotaBar.style.width = "0%";
  els.quotaUsed.textContent = "已用 -";
  els.quotaTotal.textContent = "总量 -";
}

function vipLabel(vipType) {
  if (Number(vipType) === 2) return "超级会员 SVIP";
  if (Number(vipType) === 1) return "会员 VIP";
  return "普通用户";
}

function categoryLabel(category) {
  const labels = {
    1: "视频",
    2: "音频",
    3: "图片",
    4: "文档",
    5: "应用",
    6: "其他",
    7: "种子",
  };
  return labels[Number(category)] || "文件";
}

function formatBytes(bytes) {
  const value = Number(bytes || 0);
  if (!value) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let index = 0;
  let current = value;
  while (current >= 1024 && index < units.length - 1) {
    current /= 1024;
    index += 1;
  }
  return `${current.toFixed(current >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
}

function formatTime(timestamp) {
  const value = Number(timestamp || 0);
  if (!value) return "-";
  return new Date(value * 1000).toLocaleString("zh-CN", { hour12: false });
}

function joinRemote(base, leaf) {
  const cleanedLeaf = String(leaf || "").replace(/\\/g, "/").replace(/^\/+/, "");
  const cleanedBase = String(base || "/").replace(/\\/g, "/").replace(/\/+$/, "");
  return cleanedBase ? `${cleanedBase}/${cleanedLeaf}` : `/${cleanedLeaf}`;
}

function ensureTrailingSlash(path) {
  const value = String(path || "").trim();
  if (!value) {
    return "";
  }
  return value.endsWith("/") ? value : `${value}/`;
}

function parentRemoteDir(path) {
  const normalized = String(path || "").replace(/\\/g, "/").replace(/\/+$/, "");
  if (!normalized || normalized === "/") {
    return "/";
  }
  const index = normalized.lastIndexOf("/");
  if (index <= 0) {
    return "/";
  }
  return normalized.slice(0, index) || "/";
}

function eventTargetElement(event) {
  const target = event?.target;
  return target instanceof Element ? target : null;
}

async function api(url, options = {}) {
  const response = await fetch(url, options);
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    const message = typeof payload === "string" ? payload : payload.error || "请求失败";
    throw new Error(message);
  }
  return payload;
}

async function apiWithServerPathsFallback(url) {
  try {
    return await api(url);
  } catch (error) {
    if (String(error.message || "").includes("404")) {
      throw new Error("当前服务未加载服务器路径接口，请重启 Web 服务后再试。");
    }
    throw error;
  }
}

function xhr(url, formData, onProgress) {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("POST", url, true);
    request.responseType = "json";
    request.upload.addEventListener("progress", (event) => {
      if (!event.lengthComputable) {
        return;
      }
      onProgress((event.loaded / event.total) * 100);
    });
    request.onload = () => {
      if (request.status >= 200 && request.status < 300) {
        resolve(request.response);
        return;
      }
      const message = request.response?.error || request.statusText || "上传失败";
      reject(new Error(message));
    };
    request.onerror = () => reject(new Error("上传失败"));
    request.send(formData);
  });
}

function toast(message, isError = false) {
  els.toast.hidden = false;
  els.toast.textContent = message;
  els.toast.style.background = isError ? "rgba(175, 64, 48, 0.96)" : "rgba(22, 50, 47, 0.94)";
  window.clearTimeout(els.toastTimer);
  els.toastTimer = window.setTimeout(() => {
    els.toast.hidden = true;
  }, 2800);
}

function formatSize(bytes) {
  const value = Number(bytes || 0);
  if (value < 1024) {
    return `${value} B`;
  }
  const units = ["KB", "MB", "GB", "TB"];
  let size = value / 1024;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatSpeed(bytesPerSecond) {
  return `${formatSize(bytesPerSecond)}/s`;
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
