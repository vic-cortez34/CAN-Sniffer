import sys
import random
import time
from datetime import datetime, timezone
import csv
import re
import fnmatch
from pathlib import Path
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Deque, Dict, List, Tuple

from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtGui import QColor, QBrush
from PyQt5.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QSpinBox,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QGroupBox,
    QLineEdit,
    QGridLayout,
    QSpinBox as QtSpinBox,
    QMessageBox,
)

import pyqtgraph as pg


@dataclass
class CANFrame:
    ts: float  # seconds since start
    can_id: int
    data: bytes  # length 0..8
    orig_abs_str: str | None = None  # original CSV absolute timestamp string (9th column), if available


class CANSimulator:
    """Generate CAN frames in pseudo real-time with controllable frequencies per ID.

    Implementation: every tick of size dt, for each configured ID with rate r (Hz)
    it emits with probability p = r * dt (Bernoulli approximation of Poisson).
    """

    def __init__(self, dt_ms: int = 50):
        self.dt = dt_ms / 1000.0
        # Define some IDs and their base rates (Hz) to simulate high/low frequency traffic
        self.id_rates: Dict[int, float] = {
            0x07E0: 20.0,  # high frequency
            0x07E8: 10.0,
            0x100: 5.0,
            0x101: 2.0,
            0x200: 1.0,
            0x300: 0.5,
            0x400: 0.2,
        }
        # Keep last data per ID to sometimes make small changes instead of random
        self.last_data: Dict[int, bytearray] = {}
        self.start_time = time.perf_counter()

    def tick(self) -> List[CANFrame]:
        frames: List[CANFrame] = []
        now = time.perf_counter() - self.start_time
        for can_id, rate in self.id_rates.items():
            p = rate * self.dt
            if random.random() < p:
                # Generate data: start from previous and tweak 0-2 random bytes
                prev = self.last_data.get(can_id)
                if prev is None:
                    buf = bytearray(random.getrandbits(8) for _ in range(8))
                else:
                    buf = bytearray(prev)
                    for _ in range(random.randint(0, 2)):
                        idx = random.randrange(8)
                        # +/- small change
                        delta = random.choice([-3, -2, -1, 1, 2, 3])
                        buf[idx] = (buf[idx] + delta) & 0xFF
                self.last_data[can_id] = buf
                frames.append(CANFrame(ts=now, can_id=can_id, data=bytes(buf)))
        return frames


class CSVPlayer:
    """Load and replay CAN frames from a CSV with columns similar to SavvyCAN.

    Expected columns (robust to naming):
      - start_time: float seconds (used for timing). If missing, timestamps will be sequential.
      - id: hex string (e.g., 0x...07E0) or plain hex like 7E0. If missing, try parsing from 'datastring'.
      - data: hex bytes as a single hex string (e.g., 0x02010C0000000000) or plain hex without 0x.
      - datastring: e.g., "7E0#02.01.0C.00.00.00.00.00" or similar; we parse ID and bytes.

    The loader normalizes timestamps to start at t=0.
    """

    BYTE_SPLIT = re.compile(r"[\s\.:,_-]+")

    def __init__(self):
        self.frames: List[CANFrame] = []
        self.duration: float = 0.0
        self.base_epoch: float | None = None  # absolute epoch seconds from CSV if provided
        self.is_epoch: bool = False  # True if CSV timestamps appear to be absolute epoch seconds
        self.base_rel_start: float | None = None  # first CSV timestamp when not epoch

    @staticmethod
    def _parse_hex_int(text: str) -> int:
        t = text.strip()
        if t.lower().startswith("0x"):
            return int(t, 16)
        return int(t, 16)

    @staticmethod
    def _parse_bytes_from_hex(text: str) -> bytes:
        t = text.strip()
        if t.lower().startswith("0x"):
            t = t[2:]
        # pad if odd
        if len(t) % 2 == 1:
            t = "0" + t
        raw = bytes.fromhex(t)
        # Ensure length 8
        if len(raw) < 8:
            raw = raw + bytes(8 - len(raw))
        return raw[:8]

    @classmethod
    def _parse_from_datastring(cls, s: str) -> Tuple[int | None, bytes | None]:
        if not s:
            return None, None
        part = s.strip()
        # Common formats: "7E0#02.01.0C.00.00.00.00.00" or "7E0 02 01 0C ..."
        # Split on separators
        pieces = cls.BYTE_SPLIT.split(part)
        pieces = [p for p in pieces if p]
        if not pieces:
            return None, None
        # First piece could be ID (hex). Validate length <= 8 and hex
        try:
            can_id = int(pieces[0], 16)
            data_hex = pieces[1:1+8]
        except Exception:
            return None, None
        # Convert bytes
        data: List[int] = []
        for p in data_hex:
            try:
                data.append(int(p, 16) & 0xFF)
            except Exception:
                break
        while len(data) < 8:
            data.append(0)
        return can_id, bytes(data[:8])

    def load(self, file_path: str) -> None:
        self.frames.clear()
        times: List[float] = []
        got_any_ts = False
        first_ts_val: float | None = None
        with open(file_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            # Normalize field names to lowercase for resilience
            field_map = {k.lower(): k for k in reader.fieldnames or []}
            # Determine 9th column header if available (authoritative timestamp per user)
            ninth_header = None
            if reader.fieldnames and len(reader.fieldnames) >= 9:
                ninth_header = reader.fieldnames[8]
            for row in reader:
                # Scheduling timestamp from start_time (seconds since sampling start)
                ts = None
                orig_abs_str: str | None = None
                # Capture absolute 9th-column text if present
                if ninth_header and ninth_header in row:
                    v9 = (row.get(ninth_header) or "").strip()
                    if v9:
                        orig_abs_str = v9
                        # Also parse to seconds for epoch heuristic
                        try:
                            dt = datetime.strptime(v9, "%Y-%m-%d %H:%M:%S")
                            abs_sec = dt.timestamp()
                            if first_ts_val is None:
                                first_ts_val = abs_sec
                                got_any_ts = True
                        except Exception:
                            try:
                                s = re.sub(r"\s+", " ", v9)
                                s = re.sub(r"(?i)\b(a\.?\s*m\.?)\b", "AM", s)
                                s = re.sub(r"(?i)\b(p\.?\s*m\.?)\b", "PM", s)
                                dt = datetime.strptime(s, "%d/%m/%Y %I:%M:%S %p")
                                abs_sec = dt.timestamp()
                                if first_ts_val is None:
                                    first_ts_val = abs_sec
                                    got_any_ts = True
                            except Exception:
                                pass
                # Use start_time for playback scheduling
                st_key = None
                for k in ("start_time", "start time"):
                    if k in field_map:
                        st_key = field_map[k]
                        break
                if st_key:
                    v = (row.get(st_key) or "").strip()
                    if v:
                        try:
                            ts = float(v)
                        except Exception:
                            ts = None
                # Fallbacks if start_time missing
                if ts is None:
                    # Try numeric seconds-like fields
                    for key in ("time", "timestamp_s", "time_s"):
                        if key in field_map:
                            v = row[field_map[key]].strip()
                            if v:
                                try:
                                    ts = float(v)
                                except Exception:
                                    ts = None
                            if ts is not None:
                                break
                # ID
                can_id = None
                if "id" in field_map:
                    v = row[field_map["id"]].strip()
                    if v:
                        try:
                            # Could be 0x... or decimal or hex
                            if v.lower().startswith("0x"):
                                can_id = int(v, 16)
                            else:
                                # Some exports include long 0-padded 64-bit hex, try int(v,16)
                                can_id = int(v, 16)
                        except Exception:
                            can_id = None

                data_bytes: bytes | None = None
                # Prefer explicit data column if present
                if "data" in field_map:
                    dv = row[field_map["data"]].strip()
                    if dv:
                        try:
                            data_bytes = self._parse_bytes_from_hex(dv)
                        except Exception:
                            data_bytes = None

                # Parse datastring as fallback or to get ID if missing
                if (can_id is None or data_bytes is None) and "datastring" in field_map:
                    ds = row[field_map["datastring"]]
                    ds_id, ds_data = self._parse_from_datastring(ds)
                    if can_id is None:
                        can_id = ds_id
                    if data_bytes is None:
                        data_bytes = ds_data

                if ts is None:
                    # If missing, append sequential increments of 0.01s
                    ts = times[-1] + 0.01 if times else 0.0

                if can_id is None or data_bytes is None:
                    continue  # skip malformed row

                times.append(ts)
                # Keep full CAN ID (supports 29-bit identifiers)
                self.frames.append(CANFrame(ts=ts, can_id=can_id, data=data_bytes, orig_abs_str=orig_abs_str))

        # Normalize timestamps to start at 0 (relative seconds based on start_time)
        if times:
            t0 = times[0]
            for i, fr in enumerate(self.frames):
                self.frames[i] = CANFrame(ts=fr.ts - t0, can_id=fr.can_id, data=fr.data, orig_abs_str=fr.orig_abs_str)
            self.duration = times[-1] - t0
            # Determine if CSV provided absolute epoch seconds (rough heuristic)
            # Anything greater than ~1973-03-03 (1e8 seconds since epoch) treated as epoch
            self.is_epoch = bool(got_any_ts and first_ts_val is not None and first_ts_val > 1e8)
            # Preserve CSV absolute base epoch if detected as epoch
            self.base_epoch = first_ts_val if self.is_epoch else None
            # For non-epoch CSV, keep the original first timestamp to display exact values
            self.base_rel_start = None if self.is_epoch else (first_ts_val or 0.0)
        else:
            self.duration = 0.0
            self.base_epoch = None
            self.is_epoch = False
            self.base_rel_start = None


class SnifferWindow(QWidget):
    COL_TIMESTAMP = 0
    COL_FREQ = 1
    COL_COUNT = 2
    COL_ID = 3
    COL_DATA_START = 4  # up to 11

    def __init__(self):
        super().__init__()
        self.setWindowTitle("CAN Sniffer Simulator (SavvyCAN-like)")
        self.resize(1100, 600)

        # State
        self.sim = CANSimulator(dt_ms=50)
        self.csv_player = CSVPlayer()
        self.timer = QTimer(self)
        self.timer.setInterval(50)
        self.timer.timeout.connect(self.on_tick)
        self.running = False
        self.mode = "Continuous"  # or "Latched"
        self.source = "Simulation"  # or "CSV"
        self.play_start_monotonic = None  # type: float | None
        # Base epoch (seconds since Unix epoch) to map relative frame times to wall-clock
        self.display_epoch_base = time.time()
        # Runtime verification: log first 3 CSV timestamps vs table/clock
        self._csv_verify_count = 0

        # Frequency tracking (sliding window)
        self.window_sec = 2.0
        self.id_times: Dict[int, Deque[float]] = defaultdict(deque)

        # Latched tracking
        self.id_row: Dict[int, int] = {}
        self.id_last_data: Dict[int, bytes] = {}
        self.id_prev_data: Dict[int, bytes] = {}
        self.id_counts: Dict[int, int] = {}

        # Graphing state
        self.graph_enabled = False
        self.graph_cfg = None  # type: GraphConfig | None
        self.graph_x = deque(maxlen=5000)
        self.graph_y = deque(maxlen=5000)

        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Controls (split into two rows for cleaner look)
        ctl1 = QHBoxLayout()
        ctl2 = QHBoxLayout()
        self.btn_start = QPushButton("Start")
        self.btn_stop = QPushButton("Stop")
        self.btn_reset = QPushButton("Reset")
        self.btn_clear = QPushButton("Clear Table")
        self.cmb_mode = QComboBox()
        self.cmb_mode.addItems(["Continuous", "Latched"])
        self.cmb_mode.currentTextChanged.connect(self.on_mode_changed)

        self.cmb_source = QComboBox()
        self.cmb_source.addItems(["Simulation", "CSV"])
        self.cmb_source.currentTextChanged.connect(self.on_source_changed)
        self.btn_load_csv = QPushButton("Load CSV…")
        self.btn_load_csv.clicked.connect(self.load_csv)
        self.chk_loop = QCheckBox("Loop")

        self.chk_filter = QCheckBox("Ignore IDs above freq")
        self.spn_freq = QSpinBox()
        self.spn_freq.setRange(1, 1000)
        self.spn_freq.setValue(15)  # default threshold
        self.lbl_hz = QLabel("Hz")

        # ID Filter controls
        self.chk_id_filter = QCheckBox("ID Filter")
        self.txt_id_filter = QLineEdit()
        self.txt_id_filter.setPlaceholderText("IDs: 7E0,7E8,0xCFE6C00 or patterns: CFE*, 0xCF??400")
        self.txt_id_filter.setToolTip(
            "Enter comma-separated hex IDs or patterns. Examples:\n"
            "- Exact: 7E0, 0xCFE6C00\n"
            "- Prefix: CFE* (matches CFExxxx)\n"
            "- Wildcards: 0xCF??400 (?: any hex, *: any length)\n"
            "Case-insensitive; 0x prefix optional."
        )
        self.chk_id_filter.setToolTip("Enable hex ID filter. Supports exact IDs and wildcards.")

        # Running clock
        self.lbl_clock_title = QLabel("Clock:")
        self.lbl_clock = QLabel("")

        # Timestamp format selector
        self.lbl_tsfmt = QLabel("Timestamp:")
        self.cmb_ts_format = QComboBox()
        self.cmb_ts_format.addItems([
            "Auto",              # CSV epoch -> Absolute; CSV relative -> Rel seconds
            "Absolute",          # Always absolute date-time using display_epoch_base
            "Rel seconds",       # seconds.mmm (uses CSV base if available)
            "Rel HH:MM:SS.mmm",  # formatted relative time
        ])
        self.cmb_ts_format.currentTextChanged.connect(self.on_ts_format_changed)

        # View toggles
        self.chk_show_bits = QCheckBox("Show Bit View")
        self.chk_show_bits.setChecked(True)
        self.chk_show_graph = QCheckBox("Show Graph")
        self.chk_show_graph.setChecked(True)

        # Row 1: Mode/Source/CSV/Loop + Timestamp + Clock + main actions
        ctl1.addWidget(QLabel("Mode:"))
        ctl1.addWidget(self.cmb_mode)
        ctl1.addSpacing(12)
        ctl1.addWidget(QLabel("Source:"))
        ctl1.addWidget(self.cmb_source)
        ctl1.addWidget(self.btn_load_csv)
        ctl1.addWidget(self.chk_loop)
        ctl1.addSpacing(12)
        ctl1.addWidget(self.lbl_tsfmt)
        ctl1.addWidget(self.cmb_ts_format)
        ctl1.addSpacing(8)
        ctl1.addWidget(self.lbl_clock_title)
        ctl1.addWidget(self.lbl_clock)
        ctl1.addStretch(1)
        ctl1.addWidget(self.btn_clear)
        ctl1.addWidget(self.btn_reset)
        ctl1.addWidget(self.btn_start)
        ctl1.addWidget(self.btn_stop)

        # Row 2: Filters and view toggles
        ctl2.addWidget(self.chk_filter)
        ctl2.addWidget(self.spn_freq)
        ctl2.addWidget(self.lbl_hz)
        ctl2.addSpacing(12)
        ctl2.addWidget(self.chk_id_filter)
        ctl2.addWidget(self.txt_id_filter)
        ctl2.addStretch(1)
        ctl2.addWidget(self.chk_show_bits)
        ctl2.addWidget(self.chk_show_graph)
        # Help button
        self.btn_help = QPushButton("Help")
        self.btn_help.setToolTip("Show usage and feature help")
        ctl2.addWidget(self.btn_help)

        self.btn_start.clicked.connect(self.start)
        self.btn_stop.clicked.connect(self.stop)
        self.btn_clear.clicked.connect(self.clear_table)
        self.btn_reset.clicked.connect(self.reset_playback)
        self.btn_help.clicked.connect(self.show_help)

        # ID filter parsing
        self._id_filter_set: set[int] = set()
        self._id_filter_patterns: list[str] = []  # wildcard/prefix patterns over hex ID
        self.txt_id_filter.textChanged.connect(self._parse_id_filter_text)
        
        layout.addLayout(ctl1)
        layout.addLayout(ctl2)

        # Connections for view toggles
        self.chk_show_bits.toggled.connect(self.on_toggle_bits)
        self.chk_show_graph.toggled.connect(self.on_toggle_graph)

        # ... (rest of the code remains the same)
        # Frame Table
        self.table = QTableWidget(0, 12)
        headers = [
            "timestamp",
            "frequency",
            "count",
            "id",
            "b0",
            "b1",
            "b2",
            "b3",
            "b4",
            "b5",
            "b6",
            "b7",
        ]
        self.table.setHorizontalHeaderLabels(headers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.itemSelectionChanged.connect(self.on_selection_changed)
        layout.addWidget(self.table)

        # Bit View Group
        self.bit_grp = QGroupBox("Bit View (selected row)")
        bit_layout = QVBoxLayout(self.bit_grp)
        self.bit_table = QTableWidget(8, 8)
        self.bit_table.setHorizontalHeaderLabels(["b7","b6","b5","b4","b3","b2","b1","b0"])
        self.bit_table.setVerticalHeaderLabels([f"byte{i}" for i in range(8)])
        self.bit_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.bit_table.verticalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.bit_table.setEditTriggers(QTableWidget.NoEditTriggers)
        bit_layout.addWidget(self.bit_table)
        layout.addWidget(self.bit_grp)

        # Graph Settings and Plot
        graph_container = QHBoxLayout()

        settings_grp = QGroupBox("Graph Settings")
        s_layout = QVBoxLayout(settings_grp)

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Name:"))
        self.txt_sig_name = QLineEdit()
        self.txt_sig_name.setPlaceholderText("Signal name")
        row1.addWidget(self.txt_sig_name)
        s_layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.addWidget(QLabel("ID:"))
        self.txt_sig_id = QLineEdit("0x7E0")
        row2.addWidget(self.txt_sig_id)
        s_layout.addLayout(row2)

        s_layout.addWidget(QLabel("Start Bit:"))
        self.grid_bits = QTableWidget(8, 8)
        # Label like image: numbers 63..0 with columns b7..b0 across top
        self.grid_bits.setHorizontalHeaderLabels(["7","6","5","4","3","2","1","0"])
        self.grid_bits.setVerticalHeaderLabels([str(r) for r in range(8)])
        self.grid_bits.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.grid_bits.verticalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.grid_bits.setEditTriggers(QTableWidget.NoEditTriggers)
        # Fill numbers with canonical mapping:
        # row = byte index (0..7), column = bit label (b7..b0),
        # and the numeric label equals LE bit index = row*8 + (7 - column)
        for r in range(8):
            for c in range(8):
                le_index = r * 8 + (7 - c)
                item = QTableWidgetItem(str(le_index))
                item.setTextAlignment(Qt.AlignCenter)
                self.grid_bits.setItem(r, c, item)
        self.grid_bits.cellClicked.connect(self.on_grid_bit_clicked)
        s_layout.addWidget(self.grid_bits)

        row3 = QHBoxLayout()
        row3.addWidget(QLabel("Data Len (bits):"))
        self.spn_len_bits = QtSpinBox()
        self.spn_len_bits.setRange(1, 64)
        self.spn_len_bits.setValue(8)
        row3.addWidget(self.spn_len_bits)
        s_layout.addLayout(row3)

        row4 = QHBoxLayout()
        self.chk_little = QCheckBox("Little Endian (LSB first)")
        row4.addWidget(self.chk_little)
        self.chk_signed = QCheckBox("Signed")
        row4.addWidget(self.chk_signed)
        s_layout.addLayout(row4)

        row5 = QHBoxLayout()
        row5.addWidget(QLabel("Scale:"))
        self.txt_scale = QLineEdit("1.0")
        row5.addWidget(self.txt_scale)
        row5.addWidget(QLabel("Bias:"))
        self.txt_bias = QLineEdit("0.0")
        row5.addWidget(self.txt_bias)
        s_layout.addLayout(row5)

        btn_row = QHBoxLayout()
        self.btn_apply_graph = QPushButton("Apply Graph Config")
        self.btn_clear_graph = QPushButton("Clear Graph")
        btn_row.addWidget(self.btn_apply_graph)
        btn_row.addWidget(self.btn_clear_graph)
        s_layout.addLayout(btn_row)

        self.btn_apply_graph.clicked.connect(self.apply_graph_config)
        self.btn_clear_graph.clicked.connect(self.clear_graph)

        graph_container.addWidget(settings_grp)

        plot_grp = QGroupBox("Real-time Signal Plot")
        p_layout = QVBoxLayout(plot_grp)
        self.plot = pg.PlotWidget()
        self.plot.setBackground('w')
        self.plot.showGrid(x=True, y=True)
        self.plot.setLabel('bottom', 'Time', units='s')
        self.plot.setLabel('left', 'Value')
        self.plot_curve = self.plot.plot(pen=pg.mkPen(color=(0, 100, 200), width=2))
        p_layout.addWidget(self.plot)
        graph_container.addWidget(plot_grp, stretch=1)

        # Wrap graph container for show/hide
        self.graph_panel = QWidget()
        self.graph_panel.setLayout(graph_container)
        layout.addWidget(self.graph_panel)

        self.setLayout(layout)

    def on_grid_bit_clicked(self, r: int, c: int):
        # Map r,c to bit index label
        item = self.grid_bits.item(r, c)
        if item:
            # Just visually select cell; the value is read during apply
            pass

    def on_toggle_bits(self, checked: bool):
        self.bit_grp.setVisible(checked)

    def on_toggle_graph(self, checked: bool):
        self.graph_panel.setVisible(checked)

    def show_help(self):
        msg = (
            "CAN Sniffer Simulator Help\n\n"
            "Playback & Sources:\n"
            "- Source: choose Simulation or CSV; Load CSV to import frames.\n"
            "- Start/Stop/Reset control playback; Loop repeats CSV when done.\n\n"
            "Timestamps:\n"
            "- Timestamp selector controls clock display (Auto/Absolute/Relative).\n"
            "- Table shows relative seconds based on CSV start_time.\n\n"
            "Filtering:\n"
            "- Ignore IDs above freq: hides IDs with measured frequency above threshold.\n"
            "- ID Filter: comma-separated hex IDs or patterns. Examples:\n"
            "  * Exact: 7E0, 0xCFE6C00\n"
            "  * Prefix: CFE*  (matches CFExxxx)\n"
            "  * Wildcards: 0xCF??400 (?: single hex, *: any length)\n"
            "  * Case-insensitive; 0x optional.\n\n"
            "Views:\n"
            "- Show Bit View: displays bits of the selected row's bytes and highlights changes.\n"
            "- Show Graph: configure a signal (ID, start bit, length, scaling) and plot over time.\n\n"
            "Notes:\n"
            "- Supports full 29-bit CAN IDs; IDs display as 0xHEX without leading zeros.\n"
            "- The clock shows the absolute timestamp from CSV (9th column) if present.\n"
        )
        try:
            QMessageBox.information(self, "Help", msg)
        except Exception:
            # Fallback: print to console if QMessageBox fails
            print(msg)

    def apply_graph_config(self):
        try:
            can_id_text = self.txt_sig_id.text().strip()
            can_id = int(can_id_text, 16) if can_id_text.lower().startswith('0x') else int(can_id_text, 16)
            # Selected start bit is the currently selected cell number
            idx = self.grid_bits.currentItem()
            if not idx:
                return
            r = idx.row()
            c = idx.column()
            start_bit = r * 8 + (7 - c)
            length = int(self.spn_len_bits.value())
            little = self.chk_little.isChecked()
            signed = self.chk_signed.isChecked()
            scale = float(self.txt_scale.text() or '1')
            bias = float(self.txt_bias.text() or '0')
        except Exception:
            return
        self.graph_cfg = GraphConfig(
            name=self.txt_sig_name.text().strip() or 'Signal',
            can_id=can_id,
            start_bit=start_bit,
            length=length,
            little_endian=little,
            signed=signed,
            scale=scale,
            bias=bias,
        )
        self.graph_enabled = True
        # Reset view for new config
        self.graph_x.clear()
        self.graph_y.clear()
        self.plot_curve.setData([], [])
        try:
            self.plot.enableAutoRange('xy', True)
        except Exception:
            pass

    def clear_graph(self):
        self.graph_enabled = False
        self.graph_cfg = None
        self.graph_x.clear()
        self.graph_y.clear()
        self.plot_curve.setData([], [])
        # Reset axes to initial position
        try:
            self.plot.enableAutoRange('xy', True)
            # Provide a small default window at origin so it's visually reset
            self.plot.setXRange(0, 1, padding=0)
            self.plot.setYRange(0, 1, padding=0)
        except Exception:
            pass

    def on_mode_changed(self, text: str):
        self.mode = text
        # Reset latched structures when switching into latched
        if self.mode == "Latched":
            self.id_row.clear()
            self.id_last_data.clear()
            self.clear_table()

    def on_source_changed(self, text: str):
        self.source = text
        # Reset playback clock
        self.play_start_monotonic = None

    def load_csv(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open CSV", str(Path.home()), "CSV Files (*.csv)")
        if not path:
            return
        self.csv_player.load(path)
        # Switch source to CSV automatically
        self.cmb_source.setCurrentText("CSV")
        self.play_start_monotonic = None
        # Reset playback index and frequency/history
        self._csv_play_index = 0  # type: ignore[attr-defined]
        self.id_times.clear()
        self.id_row.clear()
        self.id_last_data.clear()
        self.clear_table()

    def start(self):
        if not self.running:
            self.running = True
            self.play_start_monotonic = time.perf_counter()
            # Choose display epoch base: CSV absolute base if available and epoch-like; else real-time now
            if self.source == "CSV" and self.csv_player.is_epoch and self.csv_player.base_epoch is not None:
                self.display_epoch_base = self.csv_player.base_epoch
            else:
                self.display_epoch_base = time.time()
            self.timer.start()

    def stop(self):
        if self.running:
            self.running = False
            self.timer.stop()

    def clear_table(self):
        self.table.setRowCount(0)
        # Also clear bit view
        self._clear_bit_view()
        self.id_prev_data.clear()
        self.id_counts.clear()

    def reset_playback(self):
        """Reset to the start of the CSV file (if CSV source), and clear table/state."""
        # Clear table/state first
        self.clear_table()
        self.id_times.clear()
        self.id_row.clear()
        self.id_last_data.clear()
        self.id_prev_data.clear()
        self.id_counts.clear()
        # Reset playback index/clock and display-base
        self._csv_play_index = 0  # type: ignore[attr-defined]
        self.play_start_monotonic = time.perf_counter()
        if self.source == "CSV" and self.csv_player.is_epoch and self.csv_player.base_epoch is not None:
            self.display_epoch_base = self.csv_player.base_epoch
        else:
            self.display_epoch_base = time.time()

    def on_tick(self):
        # Determine source frames for this tick
        if self.source == "Simulation":
            frames = self.sim.tick()
        else:
            frames = self._csv_frames_for_now()
        for fr in frames:
            # Preserve previous data for bit view
            prev_for_id = self.id_last_data.get(fr.can_id)
            if prev_for_id is not None:
                self.id_prev_data[fr.can_id] = prev_for_id
            self._register_time(fr.can_id, fr.ts)
            if self._should_filter(fr.can_id) or not self._passes_id_filter(fr.can_id):
                continue
            # increment count per ID
            self.id_counts[fr.can_id] = self.id_counts.get(fr.can_id, 0) + 1
            if self.mode == "Continuous":
                self._append_row(fr)
            else:
                self._latched_update(fr)
            # Track last data for bit comparison regardless of mode
            self.id_last_data[fr.can_id] = fr.data

            # Graphing: if enabled and ID matches, extract value and plot
            if self.graph_enabled and self.graph_cfg and fr.can_id == self.graph_cfg.can_id:
                val = self._extract_signal_value(fr.data, self.graph_cfg)
                if val is not None:
                    self.graph_x.append(fr.ts)
                    self.graph_y.append(val)

        # Update running clock: for CSV, show absolute timestamp of latest displayed frame
        try:
            if self.source == "CSV" and frames:
                last = frames[-1]
                if last.orig_abs_str:
                    self.lbl_clock.setText(last.orig_abs_str)
                else:
                    # Fallback to base epoch + relative seconds
                    epoch = (self.csv_player.base_epoch or self.display_epoch_base) + last.ts
                    dt = datetime.fromtimestamp(epoch)
                    self.lbl_clock.setText(dt.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                # Simulation or no new CSV frame: show elapsed-based clock
                if self.play_start_monotonic is None:
                    elapsed = 0.0
                else:
                    elapsed = time.perf_counter() - self.play_start_monotonic
                self.lbl_clock.setText(self._format_wall_time(elapsed))
        except Exception:
            pass

        # Runtime verification logging: first 3 CSV frames
        try:
            if self.source == "CSV" and frames and self._csv_verify_count < 3:
                last = frames[-1]
                table_ts = self._format_table_time(last)
                clock_ts = self.lbl_clock.text()
                print(f"[VERIFY] CSV9='{last.orig_abs_str}' | table='{table_ts}' | clock='{clock_ts}'")
                self._csv_verify_count += 1
        except Exception:
            pass

        # After processing this batch, update plot
        if self.graph_enabled and self.graph_cfg:
            self.plot_curve.setData(list(self.graph_x), list(self.graph_y))

        # (Sorting of latched table disabled per user request)

        # Optional: cap rows to keep UI responsive
        max_rows = 5000 if self.mode == "Continuous" else 2000
        if self.table.rowCount() > max_rows:
            self.table.removeRow(0)

    # --- Frequency tracking and filtering ---
    def _register_time(self, can_id: int, ts: float):
        dq = self.id_times[can_id]
        dq.append(ts)
        cutoff = ts - self.window_sec
        while dq and dq[0] < cutoff:
            dq.popleft()

    def _freq(self, can_id: int) -> float:
        dq = self.id_times.get(can_id)
        if not dq:
            return 0.0
        return len(dq) / self.window_sec

    def _should_filter(self, can_id: int) -> bool:
        if not self.chk_filter.isChecked():
            return False
        thr = float(self.spn_freq.value())
        return self._freq(can_id) > thr

    # --- Table helpers ---
    def _append_row(self, fr: CANFrame):
        row = self.table.rowCount()
        self.table.insertRow(row)
        self._populate_row(row, fr, prev_data=None)

    def _latched_update(self, fr: CANFrame):
        # if ID not in table, add
        row = self.id_row.get(fr.can_id)
        prev = self.id_last_data.get(fr.can_id)
        if row is None:
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.id_row[fr.can_id] = row
            self._populate_row(row, fr, prev_data=None)
            self.id_last_data[fr.can_id] = fr.data
            return

        # If data identical, only refresh timestamp and frequency
        if prev is not None and prev == fr.data:
            self._update_timestamp_and_freq(row, fr)
            return

        # Overwrite row and color cells based on increase/decrease
        # Save previous for bit-view
        if prev is not None:
            self.id_prev_data[fr.can_id] = prev
        self._populate_row(row, fr, prev_data=prev)
        self.id_last_data[fr.can_id] = fr.data

    def _update_timestamp_and_freq(self, row: int, fr: CANFrame):
        ts_item = QTableWidgetItem(self._format_table_time(fr))
        ts_item.setData(Qt.UserRole, fr.ts)
        try:
            ts_item.setData(Qt.UserRole + 1, fr.orig_abs_str)
        except Exception:
            pass
        ts_item.setTextAlignment(Qt.AlignCenter)
        self.table.setItem(row, self.COL_TIMESTAMP, ts_item)

        freq_item = QTableWidgetItem(f"{self._freq(fr.can_id):0.1f}")
        freq_item.setTextAlignment(Qt.AlignCenter)
        self.table.setItem(row, self.COL_FREQ, freq_item)

        count_val = self.id_counts.get(fr.can_id, 0)
        count_item = QTableWidgetItem(str(count_val))
        count_item.setTextAlignment(Qt.AlignCenter)
        self.table.setItem(row, self.COL_COUNT, count_item)

    def _populate_row(self, row: int, fr: CANFrame, prev_data: bytes | None):
        # timestamp (full datetime)
        ts_item = QTableWidgetItem(self._format_table_time(fr))
        ts_item.setData(Qt.UserRole, fr.ts)
        try:
            ts_item.setData(Qt.UserRole + 1, fr.orig_abs_str)
        except Exception:
            pass
        ts_item.setTextAlignment(Qt.AlignCenter)
        self.table.setItem(row, self.COL_TIMESTAMP, ts_item)

        # frequency
        freq_item = QTableWidgetItem(f"{self._freq(fr.can_id):0.1f}")
        freq_item.setTextAlignment(Qt.AlignCenter)
        self.table.setItem(row, self.COL_FREQ, freq_item)

        # count
        count_val = self.id_counts.get(fr.can_id, 0)
        count_item = QTableWidgetItem(str(count_val))
        count_item.setTextAlignment(Qt.AlignCenter)
        self.table.setItem(row, self.COL_COUNT, count_item)

        # id (show full hex without leading zero padding)
        id_item = QTableWidgetItem(f"0x{fr.can_id:X}")
        id_item.setTextAlignment(Qt.AlignCenter)
        self.table.setItem(row, self.COL_ID, id_item)

        # data bytes
        for i in range(8):
            b = fr.data[i] if i < len(fr.data) else 0
            item = QTableWidgetItem(f"{b:02X}")
            item.setTextAlignment(Qt.AlignCenter)
            # Color if in latched with prev_data
            if prev_data is not None and i < len(prev_data):
                pb = prev_data[i]
                if b > pb:
                    item.setBackground(QBrush(QColor(0, 200, 0, 80)))  # greenish
                elif b < pb:
                    item.setBackground(QBrush(QColor(220, 0, 0, 80)))  # reddish
            self.table.setItem(row, self.COL_DATA_START + i, item)

        # If this row is selected, refresh bit view to reflect latest values
        if self.table.currentRow() == row:
            self._refresh_bit_view_from_row(row)

    def on_ts_format_changed(self, _: str):
        # Refresh timestamp column for all rows and update clock immediately
        self._refresh_timestamp_cells()
        try:
            if self.play_start_monotonic is None:
                elapsed = 0.0
            else:
                elapsed = time.perf_counter() - self.play_start_monotonic
            self.lbl_clock.setText(self._format_wall_time(elapsed))
        except Exception:
            pass

    def _refresh_timestamp_cells(self):
        rows = self.table.rowCount()
        for r in range(rows):
            it = self.table.item(r, self.COL_TIMESTAMP)
            if not it:
                continue
            rel_ts = it.data(Qt.UserRole)
            if rel_ts is None:
                continue
            try:
                # Try to use stored original CSV text when applicable
                orig_abs = it.data(Qt.UserRole + 1)
                # Build a temporary CANFrame shim to reuse _format_table_time logic
                fr = CANFrame(ts=float(rel_ts), can_id=0, data=b"\x00"*8, orig_abs_str=orig_abs)
                txt = self._format_table_time(fr)
                it.setText(txt)
            except Exception:
                continue

    def _resort_latched_by_freq(self):
        # Sort by frequency column descending
        self.table.sortItems(self.COL_FREQ, Qt.DescendingOrder)
        # Rebuild id_row mapping according to new row order
        self.id_row.clear()
        for r in range(self.table.rowCount()):
            id_item = self.table.item(r, self.COL_ID)
            if not id_item:
                continue
            try:
                can_id = int(id_item.text().replace("0x", ""), 16)
            except Exception:
                continue
            self.id_row[can_id] = r

    # --- Bit view helpers ---
    def _clear_bit_view(self):
        for r in range(8):
            for c in range(8):
                self.bit_table.setItem(r, c, QTableWidgetItem(""))

    def on_selection_changed(self):
        row = self.table.currentRow()
        if row < 0:
            self._clear_bit_view()
            return
        self._refresh_bit_view_from_row(row)

    def _refresh_bit_view_from_row(self, row: int):
        # Extract ID and data bytes from the row
        id_item = self.table.item(row, self.COL_ID)
        if not id_item:
            self._clear_bit_view()
            return
        try:
            can_id = int(id_item.text().replace("0x", ""), 16)
        except Exception:
            self._clear_bit_view()
            return
        data = []
        for i in range(8):
            it = self.table.item(row, self.COL_DATA_START + i)
            val = int(it.text(), 16) if it and it.text() else 0
            data.append(val)
        cur = bytes(data)
        prev = self.id_prev_data.get(can_id)
        self._update_bit_table(cur, prev)

    def _update_bit_table(self, cur: bytes, prev: bytes | None):
        for r in range(8):
            cb = cur[r] if r < len(cur) else 0
            pb = prev[r] if (prev is not None and r < len(prev)) else None
            for c in range(8):
                # b7 at column 0
                bit = (cb >> (7 - c)) & 1
                item = QTableWidgetItem(str(bit))
                item.setTextAlignment(Qt.AlignCenter)
                if pb is not None:
                    pbit = (pb >> (7 - c)) & 1
                    if bit > pbit:
                        item.setBackground(QBrush(QColor(0, 200, 0, 80)))
                    elif bit < pbit:
                        item.setBackground(QBrush(QColor(220, 0, 0, 80)))
                self.bit_table.setItem(r, c, item)

    # --- CSV playback helpers ---
    def _csv_frames_for_now(self) -> List[CANFrame]:
        if not self.csv_player.frames:
            return []
        if self.play_start_monotonic is None:
            self.play_start_monotonic = time.perf_counter()
            self._csv_play_index = 0  # type: ignore[attr-defined]

        elapsed = time.perf_counter() - self.play_start_monotonic
        out: List[CANFrame] = []
        # Emit frames whose ts <= elapsed
        i = getattr(self, "_csv_play_index", 0)
        frs = self.csv_player.frames
        n = len(frs)
        while i < n and frs[i].ts <= elapsed:
            out.append(frs[i])
            i += 1
        self._csv_play_index = i

        # If finished
        if i >= n and self.chk_loop.isChecked():
            # Loop: reset clock and index
            self.play_start_monotonic = time.perf_counter()
            self._csv_play_index = 0
        return out

    # --- Timestamp formatting ---
    def _format_wall_time(self, rel_ts: float) -> str:
        # Determine desired formatting mode
        mode = self.cmb_ts_format.currentText() if hasattr(self, 'cmb_ts_format') else 'Auto'
        csv_is_epoch = getattr(self.csv_player, 'is_epoch', False)
        base_rel = getattr(self.csv_player, 'base_rel_start', None) or 0.0

        def fmt_hms(sec: float) -> str:
            s = max(0.0, sec)
            hours = int(s // 3600)
            s -= hours * 3600
            minutes = int(s // 60)
            s -= minutes * 60
            return f"{hours:02d}:{minutes:02d}:{s:06.3f}"

        if mode == 'Rel seconds':
            return f"{base_rel + rel_ts:.3f}s"
        if mode == 'Rel HH:MM:SS.mmm':
            return fmt_hms(base_rel + rel_ts)
        if mode == 'Absolute':
            # If CSV has absolute timestamps, use its original base epoch
            if self.source == 'CSV' and csv_is_epoch and getattr(self.csv_player, 'base_epoch', None):
                epoch = self.csv_player.base_epoch + rel_ts
                dt = datetime.fromtimestamp(epoch)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                epoch = self.display_epoch_base + rel_ts
                dt = datetime.fromtimestamp(epoch)
                return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _format_table_time(self, fr: CANFrame) -> str:
        """Formatting for the table timestamp column: relative seconds from start_time."""
        return f"{fr.ts:.6f}s"

    # --- ID Filter ---
    def _parse_id_filter_text(self, text: str):
        s: set[int] = set()
        patterns: list[str] = []
        for part in text.split(','):
            p = part.strip()
            if not p:
                continue
            # Normalize remove 0x and upper-case for pattern matching
            p_norm = p.upper()
            if p_norm.startswith('0X'):
                p_norm = p_norm[2:]
            # Wildcard pattern (supports * and ?)
            if '*' in p_norm or '?' in p_norm:
                patterns.append(p_norm)
                continue
            # Try hex integer parse
            try:
                v = int(p, 16)
                s.add(v)
                # If user provided a short hex (prefix), also treat as prefix pattern
                hex_text = p_norm
                if hex_text.startswith('0X'):
                    hex_text = hex_text[2:]
                if 1 <= len(hex_text) < 8:
                    patterns.append(hex_text + '*')
            except Exception:
                # If not valid hex, skip
                continue
        self._id_filter_set = s
        self._id_filter_patterns = patterns

    def _passes_id_filter(self, can_id: int) -> bool:
        if not self.chk_id_filter.isChecked():
            return True
        # If no explicit filters or patterns, pass everything
        if not self._id_filter_set and not self._id_filter_patterns:
            return True
        # Exact numeric match
        if can_id in self._id_filter_set:
            return True
        # Pattern match on uppercase hex without 0x
        hex_id = f"{can_id:X}"
        for pat in self._id_filter_patterns:
            if fnmatch.fnmatchcase(hex_id, pat):
                return True
        return False

    # --- Signal extraction ---
    def _extract_signal_value(self, data: bytes, cfg: 'GraphConfig') -> float | None:
        if not data or cfg.length <= 0 or cfg.length > 64:
            return None
        if cfg.start_bit < 0 or cfg.start_bit > 63:
            return None
        # Make sure extraction does not overflow available 64-bit window
        end_bit = cfg.start_bit + cfg.length - 1
        if end_bit > 63:
            return None
        if cfg.little_endian:
            raw = _bits_to_value_le(data, cfg.start_bit, cfg.length)
        else:
            raw = _bits_to_value_be(data, cfg.start_bit, cfg.length)
        if cfg.signed:
            raw = _apply_signed(raw, cfg.length)
        val = float(raw) * cfg.scale + cfg.bias
        return val

# --- Graphing structures and helpers ---

@dataclass
class GraphConfig:
    name: str
    can_id: int  # 11-bit
    start_bit: int  # 0..63 (0 = LSB of byte0 when little-endian)
    length: int  # number of bits
    little_endian: bool
    signed: bool
    scale: float
    bias: float


def _bits_to_value_le(data: bytes, start_bit: int, length: int) -> int:
    # Little-endian bit numbering: bit 0 = LSB of byte0
    v = int.from_bytes(data.ljust(8, b"\x00"), byteorder="little", signed=False)
    mask = (1 << length) - 1
    return (v >> start_bit) & mask


def _bits_to_value_be(data: bytes, start_bit: int, length: int) -> int:
    # Big-endian bit numbering over 64-bit value: bit 63 = MSB of byte0
    v = int.from_bytes(data.ljust(8, b"\x00"), byteorder="big", signed=False)
    # Convert start_bit in 0..63 to offset from MSB
    bit_from_msb = 63 - start_bit - (length - 1)
    if bit_from_msb < 0:
        bit_from_msb = 0
    mask = (1 << length) - 1
    return (v >> bit_from_msb) & mask


def _apply_signed(val: int, length: int) -> int:
    sign_bit = 1 << (length - 1)
    if val & sign_bit:
        val = val - (1 << length)
    return val



def main():
    app = QApplication(sys.argv)
    w = SnifferWindow()
    w.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
