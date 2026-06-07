const RESIZABLE_TABLE_STORAGE_PREFIX = "redisStreamCoordinator.console.tableWidths.";

document.addEventListener("DOMContentLoaded", () => {
    initResizableTables();
});

function initResizableTables(root = document) {
    root.querySelectorAll("table.resizable-message-table").forEach((table) => {
        if (table.dataset.resizableInitialized === "true") return;
        table.dataset.resizableInitialized = "true";
        ensureResizableColgroup(table);
        restoreResizableTableWidths(table);
        bindResizableTableHandles(table);
    });
}

function ensureResizableColgroup(table) {
    const headerCount = table.querySelectorAll("thead th").length;
    let colgroup = table.querySelector("colgroup");
    if (!colgroup) {
        colgroup = document.createElement("colgroup");
        table.insertBefore(colgroup, table.firstChild);
    }
    while (colgroup.children.length < headerCount) {
        colgroup.appendChild(document.createElement("col"));
    }
}

function bindResizableTableHandles(table) {
    const headers = Array.from(table.querySelectorAll("thead th"));
    headers.forEach((header, index) => {
        const handle = header.querySelector(".column-resize-handle");
        if (!handle) return;
        handle.addEventListener("pointerdown", (event) => startColumnResize(event, table, index));
    });
}

function startColumnResize(event, table, index) {
    event.preventDefault();
    const col = table.querySelectorAll("colgroup col")[index];
    if (!col) return;

    const startX = event.clientX;
    const startWidth = table.querySelectorAll("thead th")[index].getBoundingClientRect().width;
    const minWidth = Number(table.dataset.minColumnWidth || 72);
    table.classList.add("is-resizing");
    document.body.classList.add("column-resize-active");

    const move = (moveEvent) => {
        const nextWidth = Math.max(minWidth, Math.round(startWidth + moveEvent.clientX - startX));
        col.style.width = `${nextWidth}px`;
    };
    const stop = () => {
        document.removeEventListener("pointermove", move);
        document.removeEventListener("pointerup", stop);
        table.classList.remove("is-resizing");
        document.body.classList.remove("column-resize-active");
        persistResizableTableWidths(table);
    };

    document.addEventListener("pointermove", move);
    document.addEventListener("pointerup", stop);
}

function persistResizableTableWidths(table) {
    const key = resizableTableStorageKey(table);
    if (!key) return;
    const widths = Array.from(table.querySelectorAll("colgroup col")).map((col) => col.style.width || "");
    localStorage.setItem(key, JSON.stringify(widths));
}

function restoreResizableTableWidths(table) {
    const key = resizableTableStorageKey(table);
    if (!key) return;
    const raw = localStorage.getItem(key);
    if (!raw) return;
    try {
        const widths = JSON.parse(raw);
        const cols = table.querySelectorAll("colgroup col");
        widths.forEach((width, index) => {
            if (cols[index] && typeof width === "string" && width.endsWith("px")) {
                cols[index].style.width = width;
            }
        });
    } catch (_error) {
        localStorage.removeItem(key);
    }
}

function resizableTableStorageKey(table) {
    const name = table.dataset.resizableTable;
    return name ? `${RESIZABLE_TABLE_STORAGE_PREFIX}${name}` : "";
}
