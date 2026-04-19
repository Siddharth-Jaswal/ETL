from __future__ import annotations

import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from etl_project.config.config import settings


def _format_numeric_cell(value: object, decimals: int = 4) -> str:
    if isinstance(value, float):
        return f"{value:.{decimals}f}"
    return str(value)


def render_results_table(
    csv_path: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    csv_path = (csv_path or settings.results_csv_path).resolve()
    output_path = (output_path or (settings.results_dir / "results_table.png")).resolve()
    dataframe = pd.read_csv(csv_path)
    case3_columns = _get_case3_chunk_columns(dataframe)

    display_columns = [
        "SrNo",
        "#Records",
        "DB_SIZE_MB",
        "CASE1_SEC",
        "CASE2_SEC",
        *case3_columns,
    ]
    table_df = dataframe[display_columns].copy()
    table_df.columns = [
        "SrNo",
        "#Records",
        "DB Size (MB)",
        "CASE1",
        "CASE2",
        *[column.replace("_SEC", "").replace("_", " ") for column in case3_columns],
    ]

    formatted_df = table_df.map(_format_numeric_cell)
    row_count = len(formatted_df)
    fig_height = max(3.8, 1.15 + row_count * 0.72)

    fig, ax = plt.subplots(figsize=(14, fig_height))
    fig.patch.set_facecolor("#f4f7fb")
    ax.set_facecolor("#f4f7fb")
    ax.axis("off")

    title = "ETL Benchmark Results"
    subtitle = "Execution time in seconds across dataset sizes and shared Case 3 chunk sizes"
    ax.text(
        0.5,
        1.08,
        title,
        ha="center",
        va="bottom",
        fontsize=20,
        fontweight="bold",
        color="#0f172a",
        transform=ax.transAxes,
    )
    ax.text(
        0.5,
        1.02,
        subtitle,
        ha="center",
        va="bottom",
        fontsize=10.5,
        color="#475569",
        transform=ax.transAxes,
    )

    table = ax.table(
        cellText=formatted_df.values,
        colLabels=formatted_df.columns,
        cellLoc="center",
        loc="center",
        colColours=["#dbeafe"] * len(formatted_df.columns),
    )
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.8)

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#cbd5e1")
        if row == 0:
            cell.set_facecolor("#bfdbfe")
            cell.set_text_props(weight="bold", color="#0f172a")
            cell.set_height(0.12)
        else:
            cell.set_facecolor("#ffffff" if row % 2 else "#f8fafc")
            if col in {3, 4, 5, 6, 7, 8}:
                cell.set_text_props(color="#0f172a", weight="semibold")
            else:
                cell.set_text_props(color="#334155")

    footer = "Case 3 columns use shared chunk sizes across all source databases."
    ax.text(
        0.5,
        -0.08,
        footer,
        ha="center",
        va="top",
        fontsize=9.5,
        color="#64748b",
        transform=ax.transAxes,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return output_path


def render_results_plot(
    csv_path: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    csv_path = (csv_path or settings.results_csv_path).resolve()
    output_path = (output_path or settings.plot_path).resolve()
    dataframe = pd.read_csv(csv_path)

    fig, ax = plt.subplots(figsize=(11, 6.5))
    fig.patch.set_facecolor("#f8fafc")
    ax.set_facecolor("#ffffff")
    y_values = dataframe["RECORD_COUNT"]
    y_labels = dataframe["#Records"]

    ax.plot(
        dataframe["CASE1_SEC"],
        y_values,
        marker="o",
        linewidth=2.4,
        color="#2563eb",
        label="Case 1",
    )
    ax.plot(
        dataframe["CASE2_SEC"],
        y_values,
        marker="s",
        linewidth=2.4,
        color="#f97316",
        label="Case 2",
    )
    ax.plot(
        dataframe["CASE3_OPTIMAL_SEC"],
        y_values,
        marker="^",
        linewidth=2.4,
        color="#059669",
        label="Case 3 (Optimal)",
    )

    ax.set_title("ETL Benchmark Comparison", fontsize=18, fontweight="bold", color="#0f172a")
    ax.set_xlabel("Time (seconds)", fontsize=12, color="#1e293b")
    ax.set_ylabel("#Records", fontsize=12, color="#1e293b")
    ax.set_yticks(y_values)
    ax.set_yticklabels(y_labels)
    ax.grid(True, linestyle="--", alpha=0.3)
    ax.legend(frameon=False, fontsize=11)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return output_path


def _get_case3_chunk_columns(dataframe: pd.DataFrame) -> list[str]:
    chunk_columns = [
        column
        for column in dataframe.columns
        if re.fullmatch(r"\d+_MB_SEC", column)
    ]
    return sorted(chunk_columns, key=lambda column: int(column.split("_", maxsplit=1)[0]))
