import os
import re
import time
import asyncio
import subprocess
from pathlib import Path
from typing import Dict, Any
from utils.job_manager import update_job
from utils.ingestion_pipeline import discover_nesstar_converter_exe, log_terminal


def _scan_export_dir(watch_dir: str) -> tuple[int, int]:
    """
    Returns (file_count, total_size_bytes) for all .sav/.csv/.dta files
    found recursively in watch_dir. Tracks both count AND size so we can
    detect files that are still being written (size still growing).
    """
    count = 0
    total_size = 0
    try:
        for root, _dirs, files in os.walk(watch_dir):
            for f in files:
                if f.lower().endswith(('.sav', '.csv', '.dta')):
                    count += 1
                    try:
                        total_size += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
    except Exception:
        pass
    return count, total_size


async def run_nesstar_conversion_pipeline(
    job_id: str, input_path: str, output_dir: str, schema: str
) -> Dict[str, Any]:
    """
    1. Launch Nesstar Explorer with the .Nesstar file.
    2. Fire AutoIt dataset-export (background thread) + start file watcher
       SIMULTANEOUSLY so we observe files as they land.
    3. Watcher polls every 10 s; tracks both file COUNT and total SIZE.
       Breaks once both are stable for IDLE_STABLE_CYCLES consecutive polls
       (at least 1 file exists, and at least IDLE_WAIT_SECS have elapsed
        since the first file appeared).
    4. Trigger DDI export AutoIt script.
    5. Cleanup Nesstar Explorer process.
    """
    if not schema or not schema.strip():
        raise ValueError("Schema must be provided for Nesstar conversion")

    # ── Resolve Nesstar Explorer EXE ──────────────────────────────────────────
    exe = (os.getenv("NESSTAR_CONVERTER_EXE") or "").strip()
    if exe and not os.path.exists(exe):
        exe = ""
    if not exe:
        exe = discover_nesstar_converter_exe()
        if exe:
            os.environ.setdefault("NESSTAR_CONVERTER_EXE", exe)
    if not exe or not os.path.exists(exe):
        raise ValueError(f"NESSTAR_CONVERTER_EXE not configured or does not exist: {exe}")

    # ── Resolve AutoIt EXE ────────────────────────────────────────────────────
    autoit_exe_candidates = [
        (os.getenv("NESSTAR_AUTOIT_EXE") or "").strip(),
        r"C:\Program Files (x86)\AutoIt3\AutoIt3.exe",
        r"C:\Program Files\AutoIt3\AutoIt3.exe",
    ]
    autoit_exe = next((p for p in autoit_exe_candidates if p and os.path.exists(p)), None)
    if not autoit_exe:
        raise ValueError("AutoIt executable not found")

    # ── Resolve script paths ──────────────────────────────────────────────────
    project_root = Path(__file__).resolve().parent.parent
    script_data = str((project_root / "utils" / "nesstar_export_data.au3").resolve())
    script_ddi  = str((project_root / "utils" / "nesstar_export_ddi.au3").resolve())
    if not os.path.exists(script_data) or not os.path.exists(script_ddi):
        raise ValueError("AutoIt scripts missing")

    # Ensure the watch dir exists
    os.makedirs(output_dir, exist_ok=True)

    update_job(job_id, status="processing", progress=5, message="Launching Nesstar Explorer...")
    log_terminal(f"Launching Nesstar Explorer with {input_path}")
    log_terminal(f"Watching export folder: {output_dir}")

    nesstar_proc = None
    loop = asyncio.get_running_loop()

    try:
        # ── 1. Launch Nesstar Explorer ────────────────────────────────────────
        nesstar_proc = subprocess.Popen([exe, input_path], cwd=output_dir)
        await asyncio.sleep(3)  # Let the UI fully load

        # ── 2. Fire AutoIt data-export using Popen (NOT run, so we can kill it later) ──
        update_job(job_id, progress=10, message="AutoIt launched: exporting datasets...")
        cmd_data = [autoit_exe, "/ErrorStdOut", script_data, input_path, output_dir]

        # Use Popen so we hold the process handle and can kill it when datasets are done
        data_export_proc = subprocess.Popen(
            cmd_data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        log_terminal(f"AutoIt data-export launched (PID {data_export_proc.pid}).")

        # ── 3. PARALLEL watcher: track file count + size ──────────────────────
        POLL_INTERVAL     = 10   # seconds between each folder scan
        IDLE_STABLE_CYCLES = 2   # how many consecutive stable polls = done
        MIN_IDLE_WAIT     = 20   # minimum seconds after first file before we can break
        MAX_TOTAL_WAIT    = 600  # 10-minute hard cap

        idle_counter   = 0
        last_count     = 0
        last_size      = 0
        total_elapsed  = 0
        first_file_at  = None   # timestamp when first file detected

        update_job(job_id, status="exporting_datasets", progress=15,
                   message=f"Watching {output_dir} for dataset files...")
        log_terminal("Watcher started (parallel with AutoIt export).")

        while total_elapsed < MAX_TOTAL_WAIT:
            await asyncio.sleep(POLL_INTERVAL)
            total_elapsed += POLL_INTERVAL

            cur_count, cur_size = _scan_export_dir(output_dir)

            if cur_count > 0 and first_file_at is None:
                first_file_at = time.monotonic()
                log_terminal(f"First dataset file detected ({cur_count} file(s), {cur_size} bytes).")

            changed = (cur_count != last_count) or (cur_size != last_size)

            if changed:
                idle_counter = 0
                last_count   = cur_count
                last_size    = cur_size
                log_terminal(
                    f"Export progress: {cur_count} file(s), {cur_size} bytes total – idle counter reset."
                )
                update_job(job_id, message=f"Exporting datasets ({cur_count} file(s), {cur_size} bytes)...")
            elif cur_count > 0:
                idle_counter += 1
                log_terminal(
                    f"No change detected. Idle: {idle_counter}/{IDLE_STABLE_CYCLES} | "
                    f"Files: {cur_count} | Size: {cur_size} bytes"
                )
            else:
                # No files yet – just wait
                log_terminal(f"No dataset files found yet. Elapsed: {total_elapsed}s")

            # Only break if: we have files, they've been stable, AND enough time has passed
            elapsed_since_first = (
                time.monotonic() - first_file_at if first_file_at else 0
            )
            if (
                idle_counter >= IDLE_STABLE_CYCLES
                and cur_count > 0
                and elapsed_since_first >= MIN_IDLE_WAIT
            ):
                log_terminal(
                    f"Export stable: {cur_count} file(s), {cur_size} bytes – "
                    f"idle for {idle_counter} cycles, {elapsed_since_first:.0f}s since first file.",
                    "success"
                )
                break

        if last_count == 0:
            log_terminal(
                "Watcher timeout: no .sav/.csv/.dta files found in export folder.", "warning"
            )

        # ── Kill the data-export AutoIt BEFORE opening DDI dialog ────────────
        # Critical: if data-export AutoIt is still running it sends keystrokes
        # to Nesstar which closes the DDI dialog the moment it opens.
        if data_export_proc.poll() is None:  # still running
            log_terminal("Terminating data-export AutoIt process before DDI step...")
            try:
                data_export_proc.terminate()
                data_export_proc.wait(timeout=5)
            except Exception:
                try:
                    data_export_proc.kill()
                except Exception:
                    pass
            log_terminal("Data-export AutoIt process stopped.")
        else:
            log_terminal(f"Data-export AutoIt already finished (exit {data_export_proc.returncode}).")

        await asyncio.sleep(2)  # brief pause to let Nesstar settle before DDI shortcut

        # ── 4. Trigger DDI export dialog + watch for XML ──────────────────────
        update_job(job_id, status="exporting_ddi", progress=80,
                   message="Opening DDI export dialog – please save the XML file when prompted...")

        xml_name = os.path.basename(input_path)
        xml_name = re.sub(
            r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}_',
            '', xml_name,
        )
        if xml_name.lower().endswith('.nesstar'):
            xml_name = xml_name[:-len('.nesstar')] + ".xml"
        elif xml_name.lower().endswith('.zip'):
            xml_name = xml_name[:-len('.zip')] + ".xml"
        else:
            xml_name += ".xml"

        # AutoIt ONLY opens the dialog — no save automation at all
        cmd_ddi = [autoit_exe, "/ErrorStdOut", script_ddi, output_dir, xml_name]

        def _run_ddi_open():
            return subprocess.run(cmd_ddi, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        log_terminal(f"Firing DDI dialog (Ctrl+Alt+E). User must manually save as '{xml_name}' in the export folder.")
        # Run AutoIt (fire-and-forget: it opens dialog then exits in ~2s)
        await loop.run_in_executor(None, _run_ddi_open)

        # ── Watch for the .xml file to appear (user manually saves) ──────────
        XML_POLL_INTERVAL = 5    # seconds between polls
        XML_MAX_WAIT      = 1000  
        xml_elapsed       = 0
        xml_found_path    = None
        last_xml_size     = -1

        log_terminal(f"Watching for XML file in {output_dir} — user has up to 5 minutes to save...")

        while xml_elapsed < XML_MAX_WAIT:
            await asyncio.sleep(XML_POLL_INTERVAL)
            xml_elapsed += XML_POLL_INTERVAL

            # Look for any .xml file that appeared in the export folder
            found_xmls = [
                os.path.join(root, f)
                for root, _dirs, files in os.walk(output_dir)
                for f in files if f.lower().endswith('.xml')
            ]

            if found_xmls:
                candidate = found_xmls[0]
                try:
                    current_size = os.path.getsize(candidate)
                except OSError:
                    current_size = 0
                
                if current_size > 0 and current_size == last_xml_size:
                    # File size is stable — user has finished saving
                    xml_found_path = candidate
                    log_terminal(f"DDI XML detected and stable: {os.path.basename(candidate)}", "success")
                    break
                else:
                    last_xml_size = current_size
                    log_terminal(f"XML detected ({os.path.basename(candidate)}, {current_size} bytes) — waiting for stability...")
            else:
                if xml_elapsed % 30 == 0:
                    log_terminal(f"Still waiting for user to save DDI XML... ({xml_elapsed}s elapsed)")

        if not xml_found_path:
            log_terminal("DDI XML not saved within timeout — continuing to ingestion without metadata.", "warning")
        else:
            log_terminal(f"DDI XML ready: {xml_found_path}", "success")


        update_job(job_id, progress=90, message="Nesstar extraction finished.")
        return {"returncode": 0}

    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        log_terminal(f"Nesstar pipeline failed:\n{err_msg}", "error")
        return {"returncode": 1, "error": str(e) or "Unknown Error (see terminal logs)"}

    finally:
        # ── 5. Cleanup Nesstar Explorer process ───────────────────────────────
        if nesstar_proc:
            try:
                nesstar_proc.terminate()
            except Exception:
                pass
